from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.discovery_providers import (
    BoundedCommandResult,
    CurrentTargetProvider,
    PBSProvider,
    SlurmProvider,
    CurrentEnvironmentProvider,
    IdentityProvider,
    StorageProvider,
    SystemCapabilityProvider,
)
from bourneprov.discovery import discover_site
from bourneprov.inventory_storage import InventoryStore
from tests.fixtures import system_provenance
from tests.inventory_fixtures import request, state


def command_result(
    stdout: str = "", stderr: str = "", returncode: int = 0,
    *, timed_out: bool = False, truncated: bool = False,
) -> BoundedCommandResult:
    return BoundedCommandResult(
        argv=("fixture",), returncode=returncode, stdout=stdout, stderr=stderr,
        timed_out=timed_out, truncated=truncated,
    )


class CurrentTargetTests(unittest.TestCase):
    def test_ssh_presence_is_boolean_without_source_address(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {"PATH": "", "SSH_CONNECTION": "10.0.0.1 1 10.0.0.2 2"}
            with patch(
                "bourneprov.discovery_providers.collect_system",
                return_value=system_provenance(),
            ):
                output = CurrentTargetProvider().discover(request(root, environment), state())

        metadata = output.targets[0].metadata
        self.assertTrue(metadata["ssh_session"])
        self.assertNotIn("10.0.0.1", str(metadata))
        self.assertEqual(metadata["node_role"], "unknown")

    def test_direct_allocation_evidence_changes_role_without_hostname_guessing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = {
                "PATH": "", "SLURM_JOB_ID": "123", "SLURM_JOB_PARTITION": "gpu",
                "SLURM_JOB_NUM_NODES": "1", "SECRET": "must-not-persist",
            }
            with patch(
                "bourneprov.discovery_providers.collect_system",
                return_value=system_provenance(),
            ):
                output = CurrentTargetProvider().discover(request(root, environment), state())

        target = output.targets[0]
        self.assertEqual(target.state, "allocated_compute_environment")
        self.assertEqual(target.metadata["node_role"], "allocated_compute_environment")
        self.assertEqual(target.metadata["allocation"]["partition"], "gpu")
        self.assertNotIn("must-not-persist", str(target.metadata))


class SlurmProviderTests(unittest.TestCase):
    def test_unavailable_and_allocation_without_client_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value=None):
                unavailable = SlurmProvider().discover(request(root), state())
                allocation = SlurmProvider().discover(
                    request(root, {"PATH": "", "SLURM_JOB_ID": "7", "SLURM_JOB_PARTITION": "gpu"}),
                    state(),
                )

        self.assertEqual(unavailable.status, "unavailable")
        self.assertEqual(allocation.status, "partial")
        self.assertEqual(allocation.schedulers[0].current_allocation["job_id"], "7")

    def test_partition_topology_is_aggregate_visible_and_authorization_unknown(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(list(argv))
            return command_result(
                "cpu*|up|2-00:00:00|20|64|256000|(null)\n"
                "gpu|up|1-00:00:00|4|128|512000|gpu:a100:8\n"
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value="/usr/bin/sinfo"):
                output = SlurmProvider().discover(
                    request(root, runner=runner), state()
                )

        self.assertEqual(output.status, "complete")
        self.assertEqual([item.name for item in output.targets], ["cpu", "gpu"])
        self.assertTrue(all(item.visible for item in output.targets))
        self.assertTrue(all(item.authorization == "unknown" for item in output.targets))
        self.assertEqual(output.targets[1].metadata["generic_resources"], "gpu:a100:8")
        self.assertTrue(all(item.parent_target_id == state().targets[0].id for item in output.targets))
        self.assertEqual(output.schedulers[0].execution_target_ids, [item.id for item in output.targets])
        self.assertEqual(calls, [["/usr/bin/sinfo", "--noheader", "--format=%P|%a|%l|%D|%c|%m|%G"]])
        self.assertFalse(output.metadata["node_detail_query"])
        self.assertEqual(output.metadata["job_query"], "none")
        self.assertFalse(output.metadata["submission_commands"])
        self.assertFalse(output.metadata["cancellation_commands"])

    def test_malformed_timeout_error_and_partial_truncation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value="sinfo"):
                malformed = SlurmProvider().discover(
                    request(root, runner=lambda *a, **k: command_result("bad row\n")), state()
                )
                timeout = SlurmProvider().discover(
                    request(root, runner=lambda *a, **k: command_result(timed_out=True)), state()
                )
                error = SlurmProvider().discover(
                    request(root, runner=lambda *a, **k: command_result(stderr="controller down", returncode=1)),
                    state(),
                )
                partial = SlurmProvider().discover(
                    request(
                        root,
                        runner=lambda *a, **k: command_result(
                            "cpu|up|1:00|1|4|8000|(null)\n", truncated=True
                        ),
                    ),
                    state(),
                )

        self.assertEqual(malformed.status, "error")
        self.assertEqual(timeout.status, "timeout")
        self.assertEqual(error.status, "error")
        self.assertEqual(partial.status, "partial")


class PBSProviderTests(unittest.TestCase):
    def test_read_only_queue_summary_uses_no_job_or_mutation_command(self) -> None:
        payload = (
            "Queue: normal\n"
            "    queue_type = Execution\n"
            "    enabled = True\n"
            "    started = True\n"
            "Queue: gpu\n"
            "    queue_type = Execution\n"
            "    resources_max.ncpus = 128\n"
            "    secret_field = must-not-persist\n"
        )
        calls: list[list[str]] = []

        def runner(argv, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(list(argv))
            return command_result(payload)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value="/usr/bin/qstat"):
                output = PBSProvider().discover(request(root, runner=runner), state())

        self.assertEqual(output.status, "complete")
        self.assertEqual([item.name for item in output.targets], ["normal", "gpu"])
        self.assertEqual(calls, [["/usr/bin/qstat", "-Q", "-f"]])
        self.assertNotIn("must-not-persist", str(output.targets))
        self.assertEqual(output.metadata["job_query"], "none")
        self.assertFalse(output.metadata["submission_commands"])
        self.assertFalse(output.metadata["cancellation_commands"])

    def test_unavailable_timeout_error_and_allocation_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value=None):
                unavailable = PBSProvider().discover(request(root), state())
                allocation = PBSProvider().discover(
                    request(root, {"PATH": "", "PBS_JOBID": "12", "PBS_QUEUE": "gpu"}), state()
                )
            with patch("bourneprov.discovery_providers.shutil.which", return_value="qstat"):
                timeout = PBSProvider().discover(
                    request(root, runner=lambda *a, **k: command_result(timed_out=True)), state()
                )
                error = PBSProvider().discover(
                    request(root, runner=lambda *a, **k: command_result(stderr="denied", returncode=1)),
                    state(),
                )

        self.assertEqual(unavailable.status, "unavailable")
        self.assertEqual(allocation.status, "partial")
        self.assertEqual(timeout.status, "timeout")
        self.assertEqual(error.status, "error")


class HpcTopologyIntegrationTests(unittest.TestCase):
    def test_generic_hpc_topology_is_persisted_without_node_or_user_scanning(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "home" / "current-user"
            project = root / "project" / "current-user"
            scratch = root / "scratch" / "current-user"
            for path in (home, project, scratch):
                path.mkdir(parents=True)
            environment = {
                "PATH": "/scheduler/bin", "HOME": str(home),
                "PROJECT": str(project), "SCRATCH": str(scratch),
                "SSH_CONNECTION": "redacted by provider",
            }
            runner = lambda *a, **k: command_result(
                "cpu|up|2-00:00:00|16|64|256000|(null)\n"
                "gpu|up|1-00:00:00|4|128|512000|gpu:h100:8\n"
            )
            providers = [
                IdentityProvider(), CurrentTargetProvider(), CurrentEnvironmentProvider(),
                StorageProvider(), SlurmProvider(), SystemCapabilityProvider(),
            ]
            with (
                patch(
                    "bourneprov.discovery_providers.collect_system",
                    return_value=system_provenance(),
                ),
                patch("bourneprov.discovery_providers.shutil.which", return_value="/usr/bin/sinfo"),
                patch("bourneprov.discovery.run_bounded_command", side_effect=runner),
            ):
                snapshot = discover_site(
                    InventoryStore(root / "bourne.sqlite3"), cwd=project,
                    environment=environment, providers=providers,
                )

        self.assertEqual(snapshot.current_target.role, "access_target")  # type: ignore[union-attr]
        self.assertEqual({item.name for item in snapshot.execution_targets}, {"cpu", "gpu"})
        self.assertTrue(all(item.authorization == "unknown" for item in snapshot.execution_targets))
        self.assertEqual(
            {hint for item in snapshot.storage for hint in item.role_hints},
            {"home", "project", "cwd", "scratch"},
        )
        self.assertEqual(snapshot.schedulers[0].execution_target_ids,
                         [item.id for item in snapshot.execution_targets])
        self.assertGreaterEqual(len(snapshot.capabilities), 2)
        self.assertTrue(all(item.parent_target_id == snapshot.current_target.id  # type: ignore[union-attr]
                            for item in snapshot.execution_targets))


if __name__ == "__main__":
    unittest.main()
