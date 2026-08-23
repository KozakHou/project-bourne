from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.backends import BackendError
from bourneprov.constraint_providers import DeclarativeConstraintProvider
from bourneprov.cli import main as cli_main
from bourneprov.execution_request import execution_request_from_cli
from bourneprov.execution_service import ExecutionService
from bourneprov.execution_service import PlanningError
from bourneprov.ids import new_ulid
from bourneprov.inventory_models import Capability, InventorySnapshot
from bourneprov.inventory_storage import InventoryStore
from bourneprov.planning_models import ResourceShape
from bourneprov.remote_backend import AmbiguousSubmissionError
from bourneprov.remote_transport import (
    MAX_REMOTE_RESPONSE_BYTES,
    OpenSSHTransport,
    RemoteProtocolError,
    RemoteResponse,
    RemoteTransportError,
    RemoteWorkerClient,
)
from bourneprov import remote_worker
from bourneprov.site_models import Site, SitePolicyClaim
from bourneprov.site_service import SitePlanningService, SiteService
from bourneprov.site_storage import SiteStore
from bourneprov.variants import materialize_json_variant
from bourneprov.workload import utc_now
from bourneprov.workload_models import ExecutionConstraints
from bourneprov.workload_storage import ExecutionStore
from bourneprov.worker_result import MAX_RESULT_BUNDLE_BYTES
from bourneprov.worker_bundle import build_remote_worker_zipapp
from tests.test_v07_planning import provider_document
from tests.v04_fixtures import inventory_snapshot


def result(argv: list[str], stdout: str = "", stderr: str = "", returncode: int = 0):
    return BoundedCommandResult(tuple(argv), returncode, stdout, stderr)


class InProcessTransport:
    """Deterministic SSH boundary: typed calls only, shared fake-HPC filesystem."""

    def __init__(self, *, lose_submit_response: bool = False, lose_reconcile: bool = False):
        self.operations: list[str] = []
        self.uploads: list[tuple[str, str]] = []
        self.lose_submit_response = lose_submit_response
        self.lose_reconcile = lose_reconcile
        self.submit_calls = 0

    def ensure_worker(self, site: Site) -> str:
        del site
        return "/fake/bourne-remote-worker.pyz"

    def invoke(
        self, site: Site, worker_path: str, operation: str, payload: dict[str, object]
    ) -> RemoteResponse:
        del site, worker_path
        self.operations.append(operation)
        status, data = remote_worker._dispatch(operation, payload)  # type: ignore[arg-type]
        if operation == "submit":
            self.submit_calls += 1
            if self.lose_submit_response:
                self.lose_submit_response = False
                raise RemoteTransportError("simulated SSH loss after scheduler acceptance")
        if operation == "reconcile" and self.lose_reconcile:
            raise RemoteTransportError("simulated SSH reconnect failure")
        return RemoteResponse(operation, status, data)

    def upload(self, site: Site, local_path: Path, remote_path: str) -> None:
        del site
        destination = Path(remote_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, destination)
        self.uploads.append((str(local_path), remote_path))


class RemoteTransportContractTests(unittest.TestCase):
    def test_remote_collection_preserves_the_existing_worker_result_bound(self) -> None:
        self.assertGreater(MAX_REMOTE_RESPONSE_BYTES, MAX_RESULT_BUNDLE_BYTES)

    def test_ssh_is_exact_argv_shell_false_and_does_not_weaken_host_trust(self) -> None:
        calls: list[tuple[list[str], dict[str, object]]] = []

        def runner(argv, **kwargs):
            calls.append((list(argv), kwargs))
            envelope = {
                "protocol": "bourne.remote-worker", "protocol_version": 1,
                "operation": "discover", "status": "ok", "data": {},
            }
            return result(list(argv), stdout=json.dumps(envelope))

        site = Site(
            new_ulid(), "imperial", "remote_ssh", utc_now(),
            ssh_host="login.example.edu", ssh_username="scientist", ssh_port=2222,
        )
        transport = OpenSSHTransport(runner=runner)
        with patch("bourneprov.remote_transport.shutil.which", return_value="/usr/bin/ssh"):
            response = transport.invoke(site, "/opt/bourne/worker.pyz", "discover", {})
        self.assertEqual(response.status, "ok")
        argv, options = calls[0]
        self.assertEqual(
            argv,
            [
                "/usr/bin/ssh", "-p", "2222", "--",
                "scientist@login.example.edu",
                "/opt/bourne/worker.pyz", "_remote", "discover",
            ],
        )
        self.assertIs(options["shell"], False)
        self.assertNotIn("StrictHostKeyChecking", " ".join(argv))
        self.assertNotIn("UserKnownHostsFile", " ".join(argv))
        request = json.loads(options["input_bytes"])
        self.assertEqual(request["operation"], "discover")

    def test_no_generic_remote_shell_operation_or_credential_field_exists(self) -> None:
        site = Site(new_ulid(), "cluster", "remote_ssh", utc_now(), ssh_host="cluster")
        with self.assertRaisesRegex(RemoteProtocolError, "unsupported typed"):
            OpenSSHTransport().invoke(site, "/worker.pyz", "ssh_exec", {"command": "id"})
        with self.assertRaisesRegex(ValueError, "unsupported site fields"):
            Site.from_dict({**site.to_dict(), "password": "secret"})
        self.assertFalse(any("shell" in operation for operation in remote_worker._OPERATIONS))
        self.assertFalse(any("exec" in operation for operation in remote_worker._OPERATIONS))

    def test_worker_version_or_digest_mismatch_is_explicit(self) -> None:
        class MismatchTransport(InProcessTransport):
            def ensure_worker(self, site: Site) -> str:
                raise RemoteTransportError("remote worker version/digest verification failed")

        site = Site(new_ulid(), "cluster", "remote_ssh", utc_now(), ssh_host="cluster")
        with self.assertRaisesRegex(RemoteTransportError, "version/digest"):
            _ = RemoteWorkerClient(site, MismatchTransport()).worker_path

    def test_preexisting_console_worker_path_and_hidden_typed_dispatch(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **kwargs):
            del kwargs
            calls.append(list(argv))
            envelope = {
                "protocol": "bourne.remote-worker", "protocol_version": 1,
                "operation": "hello", "status": "ok",
                "data": {"bourne_version": "0.7.0.dev0", "compatible": True},
            }
            return result(list(argv), stdout=json.dumps(envelope))

        site = Site(
            new_ulid(), "cluster", "remote_ssh", utc_now(),
            ssh_host="cluster", remote_worker_path="~/.local/bin/bourne",
        )
        transport = OpenSSHTransport(runner=runner)
        with patch("bourneprov.remote_transport.shutil.which", return_value="/usr/bin/ssh"):
            self.assertEqual(transport.ensure_worker(site), "~/.local/bin/bourne")
        self.assertIn("~/.local/bin/bourne", calls[0])

        with patch("bourneprov.remote_worker.main", return_value=17) as worker:
            self.assertEqual(cli_main(["_remote", "hello"]), 17)
        worker.assert_called_once_with(["_remote", "hello"])

    def test_bootstrap_program_uses_stdin_and_only_safe_remote_command_tokens(self) -> None:
        calls: list[tuple[list[str], dict[str, object]]] = []

        def runner(argv, **kwargs):
            calls.append((list(argv), kwargs))
            return result(list(argv))

        site = Site(
            new_ulid(), "cluster", "remote_ssh", utc_now(), ssh_host="cluster"
        )
        transport = OpenSSHTransport(runner=runner)
        with patch("bourneprov.remote_transport.shutil.which", return_value="/usr/bin/ssh"):
            transport._bootstrap_directory(site, "/work/user/.bourne/workers")

        argv, options = calls[0]
        self.assertEqual(
            argv,
            [
                "/usr/bin/ssh", "--", "cluster", "python3", "-",
                "/work/user/.bourne/workers",
            ],
        )
        self.assertNotIn("-c", argv)
        self.assertIn(b"pathlib.Path(sys.argv[1])", options["input_bytes"])
        self.assertIs(options["shell"], False)

    def test_remote_state_and_pbs_identity_reject_option_or_path_substitution(self) -> None:
        execution_id = new_ulid()
        with self.assertRaisesRegex(remote_worker.RemoteWorkerError, "Bourne execution path"):
            remote_worker._state(
                {
                    "staging_directory": f"/tmp/not-bourne/{execution_id}",
                },
                execution_id,
            )
        self.assertIsNone(remote_worker._job_id("pbs", "-W force"))

    def test_remote_worker_is_one_shot_non_ai_non_mcp_non_daemon(self) -> None:
        with patch.object(sys, "argv", [sys.executable]):
            status, data = remote_worker._hello({})
        self.assertEqual(status, "ok")
        self.assertEqual(data["role"], "remote_worker")
        self.assertFalse(data["ai"])
        self.assertFalse(data["mcp"])
        self.assertFalse(data["daemon"])

    def test_bootstrapped_worker_is_directly_executable_and_digest_verified(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            worker = build_remote_worker_zipapp(Path(directory) / "remote-worker.pyz")
            digest = hashlib.sha256(worker.read_bytes()).hexdigest()
            request = {
                "protocol": "bourne.remote-worker",
                "protocol_version": 1,
                "operation": "hello",
                "payload": {
                    "expected_version": "0.7.0.dev0",
                    "expected_sha256": digest,
                },
            }
            completed = subprocess.run(
                [str(worker), "_remote", "hello"],
                input=json.dumps(request), text=True,
                capture_output=True, check=False, timeout=10,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        response = json.loads(completed.stdout)
        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["data"]["worker_sha256"], digest)


class RemoteDiscoveryTests(unittest.TestCase):
    def test_remote_facts_retain_login_context_and_are_not_compute_facts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            site_service = SiteService(database)
            site = site_service.add_site(
                "fake-hpc", ssh_host="fake",
                remote_project_root=str(root.resolve()),
            )
            site_service.remote_clients[site.id] = RemoteWorkerClient(
                site, InProcessTransport()
            )
            snapshot = site_service.discover(site.id)
            reopened = InventoryStore(database).get(snapshot.id)
            linked_site_id = SiteStore(database).site_for_inventory(snapshot.id).id  # type: ignore[union-attr]
        self.assertEqual(reopened.site_label, "fake-hpc")
        self.assertEqual(
            reopened.metadata["observation_context"], "login_access_node"
        )
        self.assertEqual(
            reopened.metadata["compute_allocation_facts"], "not_observed"
        )
        self.assertEqual(linked_site_id, site.id)

    def test_scheduler_free_remote_can_be_discovered_but_not_selected_for_durable_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            service = SiteService(database)
            site = service.add_site(
                "remote-workstation", ssh_host="fake", scheduler_hint="none",
                local_project_root=str(root.resolve()),
                remote_project_root=str(root.resolve()),
            )
            snapshot = inventory_snapshot(root, executable=Path(sys.executable).name)
            snapshot = replace(snapshot, site_label=site.name)
            InventoryStore(database).save(snapshot)
            service.sites.link_inventory(site.id, snapshot.id)
            request = execution_request_from_cli([sys.executable, "-c", "print(1)"], cwd=root)
            session = SitePlanningService(database).explore_request(
                request, site.id, snapshot,
                resource_shapes=[
                    ResourceShape(
                        nodes=1, total_cpus=1,
                        evidence=[{"authorization": "user-declared-authorized"}],
                    )
                ],
            )
            candidate = session.exploration.candidates[0]
            self.assertEqual(candidate.state, "viable")
            with self.assertRaisesRegex(ValueError, "selection source"):
                SitePlanningService(database).select(
                    session, candidate.id, selection_source=""
                )
            with self.assertRaisesRegex(ValueError, "rationale"):
                SitePlanningService(database).select(
                    session, candidate.id, selection_source="human",
                    selection_rationale="x" * 4097,
                )
            with self.assertRaisesRegex(ValueError, "scheduler"):
                SitePlanningService(database).select(
                    session, candidate.id, selection_source="human"
                )

    def test_launcher_requirements_are_resolved_not_silently_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            service = SiteService(database)
            site = service.add_site(
                "fake", ssh_host="fake",
                remote_project_root=str(root.resolve()),
            )
            snapshot = inventory_snapshot(
                root, scheduler_families=("slurm",),
                executable=Path(sys.executable).name,
            )
            target = replace(
                snapshot.execution_targets[0],
                authorization="observed-authorized",
                metadata={
                    **snapshot.execution_targets[0].metadata,
                    "resource_shapes": [{"nodes": 1, "total_cpus": 1}],
                },
            )
            snapshot = replace(
                snapshot, site_label=site.name,
                targets=[snapshot.current_target, target],  # type: ignore[list-item]
            )
            InventoryStore(database).save(snapshot)
            service.sites.link_inventory(site.id, snapshot.id)
            document = provider_document()
            document["parameters"] = []
            document["constraints"] = []
            document["environment_requirements"] = []
            request = execution_request_from_cli([sys.executable, "-c", "pass"], cwd=root)

            session = SitePlanningService(database).explore_request(
                request, site.id, snapshot,
                provider=DeclarativeConstraintProvider.from_dict(document),
            )

        self.assertEqual(session.exploration.candidates[0].state, "unresolved")
        self.assertIn("mpirun", session.exploration.candidates[0].unresolved[0])


class FakeHPCEndToEndTests(unittest.TestCase):
    def _prepared(self, root: Path, transport: InProcessTransport):
        local_root = root / "local"
        remote_root = root / "remote"
        local_root.mkdir()
        remote_root.mkdir()
        database = root / "bourne.sqlite3"
        site_service = SiteService(database)
        site = site_service.add_site(
            "fake-hpc", ssh_host="fake", scheduler_hint="slurm",
            local_project_root=str(local_root.resolve()),
            remote_project_root=str(remote_root.resolve()),
        )
        site_service.remote_clients[site.id] = RemoteWorkerClient(site, transport)
        executable_name = Path(sys.executable).name
        snapshot = inventory_snapshot(
            remote_root, scheduler_families=("slurm",), executable=executable_name
        )
        target = snapshot.execution_targets[0]
        target = replace(
            target,
            authorization="observed-authorized",
            metadata={
                **target.metadata,
                "resource_shapes": [
                    {
                        "nodes": 1, "cpus_per_node": 4, "total_cpus": 4,
                        "mpi_ranks": 4, "ranks_per_node": 4,
                        "scheduler_class": target.name,
                    },
                    {
                        "nodes": 2, "cpus_per_node": 4, "total_cpus": 8,
                        "mpi_ranks": 8, "ranks_per_node": 4,
                        "scheduler_class": target.name,
                    },
                ],
            },
        )
        contexts = [
            replace(
                snapshot.execution_contexts[0],
                metadata={"activation": {"kind": "none"}},
            )
        ]
        launcher = Capability(
            id=new_ulid(), snapshot_id=snapshot.id,
            context_id=snapshot.execution_contexts[0].id,
            kind="executable", name="mpirun", locator="/bin/mpirun",
            observation_state="observed", provider="fixture",
            classifications=["launcher"],
        )
        snapshot = replace(
            snapshot, site_label=site.name,
            metadata={
                **snapshot.metadata,
                "observation_scope": "remote_ssh_login_access_node",
                "observation_context": "login_access_node",
                "compute_allocation_facts": "not_observed",
            },
            targets=[snapshot.current_target, target],  # type: ignore[list-item]
            execution_contexts=contexts,
            capabilities=[*snapshot.capabilities, launcher],
        )
        InventoryStore(database).save(snapshot)
        site_service.sites.link_inventory(site.id, snapshot.id)
        site_service.add_policy_claim(
            SitePolicyClaim(
                new_ulid(), site.id, "user-account", "max_nodes", 1,
                "site_declared", "hard_constraint", "official-fake-policy",
                utc_now(), source_identifier="fake-policy-v1",
            )
        )
        case = local_root / "case.json"
        original = b'{"decomposition":{"x":1,"y":1},"science":42}\n'
        case.write_bytes(original)
        request = execution_request_from_cli(
            [
                sys.executable, "-c",
                "import json,sys; print(json.load(open(sys.argv[1]))['decomposition'])",
                "case.json",
            ],
            cwd=local_root, inputs=["case.json"],
            execution=ExecutionConstraints(backend="slurm"),
        )
        document = provider_document()
        document["environment_requirements"][0]["name"] = executable_name  # type: ignore[index]
        provider = DeclarativeConstraintProvider.from_dict(document)
        planner = SitePlanningService(database)
        session = planner.explore_request(request, site.id, snapshot, provider=provider)
        selected = next(
            item for item in session.exploration.candidates
            if item.state == "viable"
            and item.parameters == {"px": 2, "py": 2}
        )
        variant = materialize_json_variant(
            session.workload_id, case, root / "variant-staging",
            selected.parameters, provider, proposer="agent",
        )
        plan = planner.select(
            session, selected.id, selection_source="human",
            selection_rationale="use the proven one-node shape",
            variant=variant,
        )
        return (
            database, site, snapshot, request, session, variant, plan,
            original, local_root, remote_root,
        )

    @staticmethod
    def _scheduler_runner(argv, **_kwargs):
        name = Path(argv[0]).name
        if name == "sbatch":
            return result(list(argv), stdout="9001;fake\n")
        if name == "squeue":
            return result(list(argv), stdout="COMPLETED\n")
        raise AssertionError(f"unexpected scheduler command: {argv}")

    def test_full_local_ssh_scheduler_allocation_reconcile_path(self) -> None:
        transport = InProcessTransport()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (
                database, site, snapshot, _request, session, variant, plan,
                original, local_root, remote_root,
            ) = self._prepared(root, transport)
            self.assertEqual((local_root / "case.json").read_bytes(), original)
            self.assertEqual(session.exploration.hard_invalid_count > 0, True)
            self.assertEqual(plan.resource_shape.nodes, 1)  # type: ignore[union-attr]
            self.assertEqual(plan.environment.activation.kind, "none")  # type: ignore[union-attr]
            self.assertNotEqual("case.json", plan.argv[-1])
            self.assertIn(f"/.bourne/variants/{variant.id}/", plan.argv[-1])

            client = RemoteWorkerClient(site, transport)
            service = ExecutionService(
                ExecutionStore(database), InventoryStore(database),
                staging_root=root / "control-staging",
                remote_clients={site.id: client},
            )
            with patch("bourneprov.remote_worker.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), patch(
                "bourneprov.remote_worker.run_bounded_command", side_effect=self._scheduler_runner
            ):
                submission = service.execute_plan(plan.id, snapshot)
            self.assertEqual(submission.job_id, "9001")  # type: ignore[union-attr]
            self.assertEqual(transport.operations[-1], "submit")
            self.assertNotIn("wait", transport.operations)

            # The local control plane is now gone. The fake scheduler runs the
            # durable compute bundle independently from a fresh process.
            remote_stage = remote_root / ".bourne" / "executions" / submission.execution_id  # type: ignore[union-attr]
            completed = subprocess.run(
                [
                    sys.executable, str(remote_stage / "worker.pyz"),
                    str(remote_stage / "plan.json"),
                    str(remote_stage / "result.json"),
                    submission.execution_id,  # type: ignore[union-attr]
                ],
                env={
                    **os_environ_without_secrets(),
                    "SLURM_JOB_ID": "9001", "SLURM_JOB_NUM_NODES": "1",
                    "SLURM_CPUS_ON_NODE": "4", "SLURM_NTASKS": "4",
                },
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

            reconnected = ExecutionService(
                ExecutionStore(database), InventoryStore(database),
                remote_clients={site.id: RemoteWorkerClient(site, transport)},
            )
            with patch("bourneprov.remote_worker.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), patch(
                "bourneprov.remote_worker.run_bounded_command", side_effect=self._scheduler_runner
            ):
                worker_result = reconnected.collect_execution(submission.execution_id)  # type: ignore[union-attr]
            view = ExecutionStore(database).view(submission.execution_id)  # type: ignore[union-attr]
            self.assertEqual(worker_result.state, "completed")
            self.assertEqual(view.execution.state, "completed")
            self.assertEqual(view.scheduler_job.job_id, "9001")  # type: ignore[union-attr]
            self.assertEqual(view.allocations[0].resources["cpus"], 4)
            self.assertIn("'x': 2", worker_result.experiment.stdout)  # type: ignore[union-attr]
            self.assertEqual(SiteStore(database).get_variant(variant.id).original_sha256, variant.original_sha256)
            self.assertEqual(transport.submit_calls, 1)

    def test_lost_submit_response_reconciles_without_duplicate_submission(self) -> None:
        transport = InProcessTransport(lose_submit_response=True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, site, snapshot, *_rest, plan, _original, _local, _remote = self._prepared(root, transport)
            service = ExecutionService(
                ExecutionStore(database), InventoryStore(database),
                staging_root=root / "control-stage",
                remote_clients={site.id: RemoteWorkerClient(site, transport)},
            )
            with patch("bourneprov.remote_worker.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), patch(
                "bourneprov.remote_worker.run_bounded_command", side_effect=self._scheduler_runner
            ):
                submission = service.execute_plan(plan.id, snapshot)
            self.assertEqual(submission.job_id, "9001")  # type: ignore[union-attr]
            self.assertEqual(transport.submit_calls, 1)
            self.assertEqual(transport.operations.count("submit"), 1)
            self.assertIn("reconcile", transport.operations)

    def test_unknown_submit_truth_is_ambiguous_and_never_blindly_retried(self) -> None:
        transport = InProcessTransport(lose_submit_response=True, lose_reconcile=True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, site, snapshot, *_rest, plan, _original, _local, _remote = self._prepared(root, transport)
            service = ExecutionService(
                ExecutionStore(database), InventoryStore(database),
                staging_root=root / "control-stage",
                remote_clients={site.id: RemoteWorkerClient(site, transport)},
            )
            with patch("bourneprov.remote_worker.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), patch(
                "bourneprov.remote_worker.run_bounded_command", side_effect=self._scheduler_runner
            ):
                with self.assertRaises(AmbiguousSubmissionError):
                    service.execute_plan(plan.id, snapshot)
            execution = ExecutionStore(database).list_executions(1)[0]
            remote = SiteStore(database).remote_state(execution.id)
            self.assertEqual(execution.state, "submission_ambiguous")
            self.assertEqual(remote["state"], "ambiguous")  # type: ignore[index]
            self.assertFalse(remote["evidence"]["blind_retry"])  # type: ignore[index]
            self.assertEqual(transport.submit_calls, 1)
            with self.assertRaisesRegex(PlanningError, "reconcile"):
                service.execute_plan(plan.id, snapshot)
            self.assertEqual(transport.submit_calls, 1)
            transport.lose_reconcile = False
            with patch("bourneprov.remote_worker.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"), patch(
                "bourneprov.remote_worker.run_bounded_command", side_effect=self._scheduler_runner
            ):
                with self.assertRaisesRegex(BackendError, "result"):
                    service.collect_execution(execution.id)
            recovered = ExecutionStore(database).get_execution(execution.id)
            self.assertEqual(recovered.state, "scheduler_terminal")
            self.assertNotEqual(recovered.state, "completed")
            self.assertEqual(
                ExecutionStore(database).get_scheduler_job(execution.id).job_id, "9001"  # type: ignore[union-attr]
            )
            self.assertEqual(transport.submit_calls, 1)


def os_environ_without_secrets() -> dict[str, str]:
    # The fake compute process receives only the ordinary environment required
    # to locate Python; this helper intentionally strips common credential names.
    import os

    blocked = ("TOKEN", "SECRET", "PASSWORD", "API_KEY")
    return {
        key: value for key, value in os.environ.items()
        if not any(part in key.upper() for part in blocked)
    }


if __name__ == "__main__":
    unittest.main()
