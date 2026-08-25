from __future__ import annotations

import hashlib
import io
import json
import os
import signal
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from bourneprov.backends import (
    AmbiguousSubmission,
    BackendError,
    LSFBackend,
    render_batch_script,
)
from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.compute_worker import _load_plan, _observe_allocation, execute_plan
from bourneprov.compute_worker import _runtime_evidence
from bourneprov.constraint_providers import DeclarativeConstraintProvider
from bourneprov.discovery_providers import LSFProvider
from bourneprov.execution_request import execution_request_from_cli
from bourneprov.execution import execute_command
from bourneprov.execution_service import request_to_workload
from bourneprov.execution_service import ExecutionService
from bourneprov.ids import new_ulid
from bourneprov.inventory_storage import InventoryStore
from bourneprov.planning_models import ContainerExecution, ContainerMount, ResourceShape
from bourneprov.models import SystemProvenance
from bourneprov.resolver import resolve_execution
from bourneprov.site_planning import generate_resource_shapes
from bourneprov.storage import ExperimentStore
from bourneprov import remote_worker
from bourneprov.worker_result import encode_worker_result, parse_worker_result
from bourneprov.worker_bundle import write_staged_plan
from bourneprov.workload import inspect_workload, utc_now
from bourneprov.workload_models import (
    AllocationObservation,
    ExecutionAttempt,
    ExecutionConstraints,
    ResourceRequirements,
    SchedulerJob,
)
from bourneprov.workload_storage import ExecutionStore
from bourneprov.workload_presentation import format_execution
from tests.inventory_fixtures import request as discovery_request, state as discovery_state
from tests.v04_fixtures import inventory_snapshot


def result(
    argv: list[str], stdout: str = "", stderr: str = "", returncode: int = 0,
    *, timed_out: bool = False,
) -> BoundedCommandResult:
    return BoundedCommandResult(
        tuple(argv), returncode, stdout, stderr, timed_out=timed_out
    )


class LSFDiscoveryTests(unittest.TestCase):
    def test_queue_discovery_is_bounded_and_authorization_remains_unknown(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            calls.append(list(argv))
            return result(
                list(argv),
                "normal Open 200 20 14 2 12\naccelerated Open - - 3 1 2\n",
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "bourneprov.discovery_providers.shutil.which",
                return_value="/opt/lsf/bin/bqueues",
            ):
                output = LSFProvider().discover(
                    discovery_request(root, runner=runner), discovery_state()
                )

        self.assertEqual(output.status, "complete")
        self.assertEqual(output.schedulers[0].family, "lsf")
        self.assertEqual([item.name for item in output.targets], ["normal", "accelerated"])
        self.assertTrue(all(item.authorization == "unknown" for item in output.targets))
        self.assertEqual(output.metadata["job_query"], "none")
        self.assertEqual(calls[0][1:], [
            "-noheader", "-o", "queue_name stat max jl_u njobs pend run"
        ])

    def test_missing_client_and_allocation_environment_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value=None):
                missing = LSFProvider().discover(discovery_request(root), discovery_state())
                allocated = LSFProvider().discover(
                    discovery_request(
                        root,
                        environment={
                            "PATH": "", "LSB_JOBID": "77", "LSB_QUEUE": "normal",
                        },
                    ),
                    discovery_state(),
                )
        self.assertEqual(missing.status, "unavailable")
        self.assertEqual(allocated.status, "partial")
        self.assertEqual(allocated.schedulers[0].current_allocation["job_id"], "77")


class LSFBackendTests(unittest.TestCase):
    def _execution(self, root: Path):
        database = root / "bourne.sqlite3"
        snapshot = inventory_snapshot(root, scheduler_families=("lsf",))
        InventoryStore(database).save(snapshot)
        workload = inspect_workload(
            ["solver", "literal;not-shell"], cwd=root,
            resources=ResourceRequirements(
                cpus=8, nodes=2, mpi_ranks=8, walltime_seconds=120
            ),
            constraints=ExecutionConstraints(backend="lsf"),
        )
        plan = resolve_execution(workload, snapshot).selected
        self.assertIsNotNone(plan)
        plan = replace(
            plan,
            resource_shape=ResourceShape(
                nodes=2, cpus_per_node=4, total_cpus=8,
                mpi_ranks=8, ranks_per_node=4, walltime_seconds=120,
                scheduler_class=snapshot.execution_targets[0].name,
            ),
        )
        store = ExecutionStore(database)
        store.save_workload(workload)
        store.save_plan(plan)
        now = utc_now()
        execution = ExecutionAttempt(
            id=new_ulid(), plan_id=plan.id, backend="lsf", state="planned",
            created_at=now, updated_at=now,
            submitting_identity=None,
        )
        store.create_execution(execution)
        return store, snapshot, workload, plan, execution

    def test_resource_shape_maps_only_owned_unambiguous_lsf_concepts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _store, snapshot, _workload, plan, execution = self._execution(root)
            script = render_batch_script(
                "lsf", plan, Path("worker.pyz"), Path("plan.json"),
                Path("result.json"), execution.id,
                target_name=snapshot.execution_targets[0].name,
            )
        self.assertNotIn("#BSUB -nnodes", script)
        self.assertIn("#BSUB -n 8", script)
        self.assertIn('#BSUB -R "span[ptile=4]"', script)
        self.assertIn("#BSUB -W 2", script)
        self.assertNotIn("literal;not-shell", script)

    def test_submit_active_recent_finished_cancel_and_exact_identity(self) -> None:
        calls: list[tuple[list[str], bytes | None]] = []
        active = True

        def runner(argv, **kwargs):
            nonlocal active
            values = list(argv)
            calls.append((values, kwargs.get("input_bytes")))
            command = Path(values[0]).name
            if command == "bsub":
                return result(values, "Job <4182> is submitted to queue <normal>.\n")
            if command == "bkill":
                return result(values)
            if "-a" in values:
                return result(values, "4182 DONE\n")
            if active:
                return result(values, "4182 RUN\n")
            return result(values, stderr="Job <4182> is not found", returncode=255)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root)
            backend = LSFBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/opt/lsf/bin/{name}",
            ):
                submission = backend.execute(execution, plan, workload, snapshot)
                running = backend.status(store.get_execution(execution.id))
                active = False
                completed = backend.status(store.get_execution(execution.id))
                backend.cancel(store.get_execution(execution.id))

        self.assertEqual(submission.job_id, "4182")
        self.assertEqual(running, "running")
        self.assertEqual(completed, "completed")
        self.assertTrue(calls[0][1].startswith(b"#!/bin/sh"))  # type: ignore[union-attr]
        self.assertTrue(any("-a" in argv for argv, _data in calls))
        self.assertEqual(calls[-1][0][-1], "4182")

    def test_ambiguous_submission_is_not_blindly_retried(self) -> None:
        submissions = 0

        def runner(argv, **_kwargs):
            nonlocal submissions
            submissions += 1
            return result(list(argv), "submission accepted\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root)
            backend = LSFBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/opt/lsf/bin/bsub"):
                with self.assertRaises(AmbiguousSubmission):
                    backend.execute(execution, plan, workload, snapshot)
            current = store.get_execution(execution.id)
            event_details = store.events(execution.id)[-1].details

        self.assertEqual(submissions, 1)
        self.assertEqual(current.state, "submission_ambiguous")
        self.assertFalse(event_details["retry_safe"])

    def test_missing_submit_command_and_active_query_error_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root)
            backend = LSFBackend(store, root / "stage")
            with patch("bourneprov.backends.shutil.which", return_value=None):
                with self.assertRaisesRegex(BackendError, "bsub executable"):
                    backend.execute(execution, plan, workload, snapshot)
            self.assertEqual(store.get_execution(execution.id).state, "failed")

        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "bsub":
                return result(values, "Job <4182> is submitted to queue <normal>.\n")
            return result(values, stderr="permission denied", returncode=1)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root)
            backend = LSFBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/opt/lsf/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)
                with self.assertRaisesRegex(BackendError, "active status failed"):
                    backend.status(store.get_execution(execution.id))
        self.assertFalse(any("-a" in argv for argv in calls))

        calls.clear()

        def malformed_runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "bsub":
                return result(values, "Job <4182> is submitted to queue <normal>.\n")
            return result(values, "unexpected localized fields\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root)
            backend = LSFBackend(store, root / "stage", runner=malformed_runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/opt/lsf/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)
                with self.assertRaisesRegex(BackendError, "unique exact job record"):
                    backend.status(store.get_execution(execution.id))
        self.assertFalse(any("-a" in argv for argv in calls))

    def test_lsf_allocation_observation_is_allowlisted_and_structured(self) -> None:
        environment = {
            "LSB_JOBID": "99", "LSB_QUEUE": "normal",
            "LSB_MCPU_HOSTS": "n01 4 n02 4", "LSB_DJOB_NUMPROC": "8",
            "CUDA_VISIBLE_DEVICES": "0,1", "SECRET_TOKEN": "never-store",
        }
        with patch.dict(os.environ, environment, clear=True):
            allocation = _observe_allocation(new_ulid())
        self.assertEqual(allocation.hosts, ["n01", "n02"])
        self.assertEqual(allocation.resources["nodes"], 2)
        self.assertEqual(allocation.resources["cpus"], 8)
        self.assertEqual(allocation.resources["mpi_ranks"], 8)
        self.assertEqual(allocation.resources["gpus"], 2)
        self.assertNotIn("SECRET_TOKEN", str(allocation.evidence))

    def _observe_job(
        self,
        responses: dict[str, tuple[str, str, int]],
        *,
        bhist_available: bool = True,
    ):
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "bhist":
                stage = "historical"
            elif "-a" in values:
                stage = "recent"
            else:
                stage = "active"
            stdout, stderr, returncode = responses[stage]
            return result(values, stdout, stderr, returncode)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            ExperimentStore(database).initialize()
            backend = LSFBackend(ExecutionStore(database), root / "stage", runner=runner)
            job = SchedulerJob(
                execution_id=new_ulid(), family="lsf", job_id="4182",
                submitting_identity="scientist", submitted_at=utc_now(),
                state="submitted", last_observed_at=utc_now(),
            )
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: (
                    "/opt/lsf/bin/bhist"
                    if name == "bhist" and bhist_available
                    else None
                ),
            ):
                observation = backend._observe_job(job, "/opt/lsf/bin/bjobs")
        return observation, calls

    def test_lsf_reconciliation_separates_active_recent_and_durable_history(self) -> None:
        missing = ("", "Job <4182> is not found", 255)
        active, active_calls = self._observe_job(
            {"active": ("4182 RUN\n", "", 0), "recent": missing, "historical": missing}
        )
        recent, recent_calls = self._observe_job(
            {"active": missing, "recent": ("4182 DONE\n", "", 0), "historical": missing}
        )
        historical, historical_calls = self._observe_job(
            {
                "active": missing,
                "recent": missing,
                "historical": (
                    "Job <4182>, User <scientist>, Status <DONE>, Queue <normal>\n",
                    "", 0,
                ),
            }
        )

        self.assertEqual((active.state, active.source, active.raw_state), ("running", "active", "RUN"))
        self.assertEqual(len(active_calls), 1)
        self.assertEqual((recent.state, recent.source, recent.raw_state), ("completed", "recent_finished", "DONE"))
        self.assertEqual(len(recent_calls), 2)
        self.assertEqual(
            (historical.state, historical.source, historical.raw_state),
            ("completed", "historical_accounting", "DONE"),
        )
        bhist = historical_calls[-1]
        self.assertEqual(Path(bhist[0]).name, "bhist")
        self.assertIn("-n", bhist)
        self.assertIn("0", bhist)
        self.assertEqual(bhist[-1], "4182")
        self.assertTrue(historical.terminal)

    def test_lsf_missing_or_malformed_durable_history_remains_unobservable(self) -> None:
        missing = ("", "Job <4182> is not found", 255)
        unavailable, _calls = self._observe_job(
            {"active": missing, "recent": missing, "historical": missing},
            bhist_available=False,
        )
        malformed, calls = self._observe_job(
            {
                "active": missing,
                "recent": missing,
                "historical": (
                    "Job <9999>, User <other>, Status <DONE>\n", "", 0
                ),
            }
        )

        self.assertEqual(unavailable.state, "unobservable")
        self.assertEqual(unavailable.source, "historical_accounting_unavailable")
        self.assertFalse(unavailable.observable)
        self.assertFalse(unavailable.terminal)
        self.assertEqual(malformed.state, "unobservable")
        self.assertEqual(malformed.source, "historical_accounting")
        self.assertFalse(malformed.observable)
        self.assertIn("unique exact job", malformed.diagnostic or "")
        self.assertEqual(Path(calls[-1][0]).name, "bhist")

    def test_lsf_uncertain_and_post_processing_states_are_not_conflated(self) -> None:
        expected = {
            "UNKWN": ("scheduler_uncertain", False),
            "ZOMBI": ("scheduler_uncertain", False),
            "POST_DONE": ("post_processing_completed", True),
            "POST_ERR": ("post_processing_failed", True),
        }
        for raw_state, (normalized, terminal) in expected.items():
            with self.subTest(raw_state=raw_state):
                observation, _calls = self._observe_job(
                    {
                        "active": (f"4182 {raw_state}\n", "", 0),
                        "recent": ("", "", 0),
                        "historical": ("", "", 0),
                    }
                )
                self.assertEqual(observation.state, normalized)
                self.assertEqual(observation.raw_state, raw_state)
                self.assertEqual(observation.terminal, terminal)

    def test_lsf_wait_preserves_uncertainty_without_collection_failure(self) -> None:
        def runner(argv, **_kwargs):
            values = list(argv)
            if Path(values[0]).name == "bsub":
                return result(values, "Job <4182> is submitted.\n")
            return result(values, "4182 UNKWN\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root)
            backend = LSFBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/opt/lsf/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)
                with self.assertRaises(TimeoutError):
                    backend.wait(
                        store.get_execution(execution.id),
                        poll_seconds=0.05, timeout_seconds=0,
                    )
            current = store.get_execution(execution.id)
            details = store.events(execution.id)[-1].details

        self.assertEqual(current.state, "scheduler_unobservable")
        self.assertEqual(details["raw_scheduler_state"], "UNKWN")
        self.assertNotEqual(current.state, "collection_failed")


class RemoteLSFReconciliationTests(unittest.TestCase):
    def _state(self) -> dict[str, str]:
        return {
            "scheduler_family": "lsf", "job_id": "4182",
            "submitting_identity": "scientist",
        }

    def test_remote_reconciliation_uses_recent_then_exact_durable_history(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "bhist":
                return result(
                    values,
                    "Job <4182>, User <scientist>, Status <DONE>, Queue <normal>\n",
                )
            return result(values, stderr="Job <4182> is not found", returncode=255)

        with patch(
            "bourneprov.remote_worker.shutil.which",
            side_effect=lambda name: f"/opt/lsf/bin/{name}",
        ), patch(
            "bourneprov.remote_worker.run_bounded_command", side_effect=runner,
        ):
            observation = remote_worker._scheduler_observation(self._state())

        self.assertEqual(observation["state"], "completed")
        self.assertEqual(observation["source"], "historical_accounting")
        self.assertEqual(observation["raw_scheduler_state"], "DONE")
        self.assertEqual([Path(call[0]).name for call in calls], ["bjobs", "bjobs", "bhist"])
        self.assertEqual(calls[-1][-1], "4182")

    def test_remote_missing_and_malformed_history_remain_unknown(self) -> None:
        missing = result([], stderr="Job <4182> is not found", returncode=255)

        with patch(
            "bourneprov.remote_worker.shutil.which",
            side_effect=lambda name: None if name == "bhist" else f"/opt/lsf/bin/{name}",
        ), patch(
            "bourneprov.remote_worker.run_bounded_command", return_value=missing,
        ):
            unavailable = remote_worker._scheduler_observation(self._state())

        def malformed_runner(argv, **_kwargs):
            values = list(argv)
            if Path(values[0]).name == "bhist":
                return result(values, "Job <9999>, Status <DONE>\n")
            return result(values, stderr="Job <4182> is not found", returncode=255)

        with patch(
            "bourneprov.remote_worker.shutil.which",
            side_effect=lambda name: f"/opt/lsf/bin/{name}",
        ), patch(
            "bourneprov.remote_worker.run_bounded_command", side_effect=malformed_runner,
        ):
            malformed = remote_worker._scheduler_observation(self._state())

        self.assertEqual(unavailable["source"], "historical_accounting_unavailable")
        self.assertFalse(unavailable["observable"])
        self.assertEqual(malformed["source"], "historical_accounting")
        self.assertFalse(malformed["observable"])
        self.assertFalse(malformed["terminal"])

    def test_remote_reconcile_without_result_preserves_uncertain_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            execution_id = new_ulid()
            staging = root / ".bourne" / "executions" / execution_id
            staging.mkdir(parents=True)
            remote_worker._write_state(
                staging / "submission-state.json",
                {
                    "execution_id": execution_id, "scheduler_family": "lsf",
                    "job_id": "4182", "submitting_identity": "scientist",
                    "staging_directory": str(staging), "state": "submitted",
                    "created_at": utc_now(), "updated_at": utc_now(),
                },
            )
            with patch(
                "bourneprov.remote_worker._scheduler_observation",
                return_value={
                    "state": "scheduler_uncertain", "observable": True,
                    "terminal": False, "source": "active",
                    "raw_scheduler_state": "ZOMBI",
                },
            ):
                status, data = remote_worker._reconcile(
                    {"execution_id": execution_id, "staging_directory": str(staging)}
                )

        self.assertEqual(status, "unknown")
        self.assertEqual(data["result_state"], "absent")
        self.assertEqual(data["scheduler"]["raw_scheduler_state"], "ZOMBI")


class LSFTopologyPlanningTests(unittest.TestCase):
    def _case(self, root: Path, *, nodes: int | None = None):
        snapshot = inventory_snapshot(root, scheduler_families=("lsf",))
        workload = inspect_workload(
            ["solver", "case.in"], cwd=root,
            resources=ResourceRequirements(cpus=512, nodes=nodes, mpi_ranks=512),
            constraints=ExecutionConstraints(backend="lsf"),
        )
        return snapshot, workload

    def test_unknown_lsf_topology_stays_unknown_and_requests_only_total_slots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot, workload = self._case(root)
            first = generate_resource_shapes(snapshot, workload)
            second = generate_resource_shapes(snapshot, workload)
            plan = resolve_execution(workload, snapshot).selected
            self.assertIsNotNone(plan)
            plan = replace(plan, resource_shape=first[0])
            script = render_batch_script(
                "lsf", plan, Path("worker.pyz"), Path("plan.json"),
                Path("result.json"), new_ulid(),
                target_name=snapshot.execution_targets[0].name,
            )

        self.assertEqual(len(first), 1)
        self.assertIsNone(first[0].nodes)
        self.assertIsNone(first[0].cpus_per_node)
        self.assertIsNone(first[0].ranks_per_node)
        self.assertEqual(first[0].total_cpus, 512)
        self.assertEqual(first[0].mpi_ranks, 512)
        self.assertIn("#BSUB -n 512", script)
        self.assertNotIn("span[", script)
        self.assertEqual(
            [item.identity for item in first], [item.identity for item in second]
        )
        self.assertNotIn("preference", str(first[0].evidence).casefold())

    def test_explicit_lsf_nodes_authorize_exact_ptile_layout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot, workload = self._case(root, nodes=8)
            shapes = generate_resource_shapes(snapshot, workload)
            plan = resolve_execution(workload, snapshot).selected
            self.assertIsNotNone(plan)
            plan = replace(plan, resource_shape=shapes[0])
            script = render_batch_script(
                "lsf", plan, Path("worker.pyz"), Path("plan.json"),
                Path("result.json"), new_ulid(),
                target_name=snapshot.execution_targets[0].name,
            )

        self.assertEqual(len(shapes), 1)
        self.assertEqual(shapes[0].nodes, 8)
        self.assertEqual(shapes[0].ranks_per_node, 64)
        self.assertIn('#BSUB -R "span[ptile=64]"', script)

    def test_provider_per_host_contract_can_derive_bounded_lsf_layout(self) -> None:
        provider = DeclarativeConstraintProvider.from_dict(
            {
                "kind": "bourne.constraint-provider", "schema_version": 1,
                "name": "fixed-layout", "provider_version": "1",
                "parameters": [],
                "constraints": [
                    {
                        "id": "ranks", "operator": "equal", "hard": True,
                        "left": {"resource": "mpi_ranks"},
                        "right": {"constant": 512},
                    },
                    {
                        "id": "rank-layout", "operator": "equal", "hard": True,
                        "left": {"resource": "ranks_per_node"},
                        "right": {"constant": 64},
                    },
                ],
                "environment_requirements": [], "launcher_requirements": [],
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot, workload = self._case(root)
            shapes = generate_resource_shapes(snapshot, workload, provider=provider)

        self.assertEqual({item.nodes for item in shapes}, {8})
        self.assertEqual(shapes[0].ranks_per_node, 64)
        self.assertTrue(
            any("provider_layout_hints" in item for item in shapes[0].evidence)
        )


class SlurmAllocationTruthTests(unittest.TestCase):
    def test_heterogeneous_whole_job_cpu_layout_is_summed_exactly(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SLURM_JOB_ID": "77", "SLURM_JOB_NUM_NODES": "3",
                "SLURM_JOB_CPUS_PER_NODE": "72(x2),36",
                "SLURM_CPUS_ON_NODE": "72",
                "SLURM_JOB_NODELIST": "n[01-03]",
            },
            clear=True,
        ):
            allocation = _observe_allocation(new_ulid())

        self.assertEqual(allocation.resources["cpus_per_node_allocation"], [72, 72, 36])
        self.assertEqual(allocation.resources["cpus"], 180)
        self.assertNotEqual(allocation.resources["cpus"], 216)
        self.assertEqual(allocation.resources["local_cpus_on_node"], 72)
        self.assertNotIn("cpus_per_node", allocation.resources)
        self.assertEqual(allocation.hosts, [])
        self.assertEqual(allocation.evidence["host_scope"], "allocation")
        self.assertEqual(
            allocation.evidence["environment"]["slurm_job_nodelist"], "n[01-03]"
        )

    def test_local_node_cpu_fact_does_not_become_a_job_total(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SLURM_JOB_ID": "77", "SLURM_JOB_NUM_NODES": "3",
                "SLURM_CPUS_ON_NODE": "72",
            },
            clear=True,
        ):
            allocation = _observe_allocation(new_ulid())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = inventory_snapshot(root)
            workload = inspect_workload([sys.executable, "-c", "pass"], cwd=root)
            plan = resolve_execution(workload, snapshot).selected
            self.assertIsNotNone(plan)
            plan = replace(
                plan, backend="slurm", resource_shape=None,
                requested_resources=ResourceRequirements(nodes=3, cpus=216),
            )
            evidence = _runtime_evidence(
                new_ulid(), new_ulid(), {}, allocation,
                {"environment_activation": None, "container": None}, plan,
            )

        self.assertEqual(allocation.resources["local_cpus_on_node"], 72)
        self.assertNotIn("cpus", allocation.resources)
        self.assertEqual(allocation.evidence["host_scope"], "local_only")
        self.assertEqual(evidence.allocation.coverage, "partially_observed")
        self.assertFalse(
            any(item["resource"] == "cpus" for item in evidence.allocation.metrics["discrepancies"])
        )

    def test_homogeneous_whole_job_cpu_layout_is_summed(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SLURM_JOB_ID": "77", "SLURM_JOB_NUM_NODES": "3",
                "SLURM_JOB_CPUS_PER_NODE": "72(x3)",
                "SLURM_CPUS_ON_NODE": "72",
            },
            clear=True,
        ):
            allocation = _observe_allocation(new_ulid())

        self.assertEqual(allocation.resources["cpus"], 216)
        self.assertEqual(allocation.resources["cpus_per_node"], 72)
        self.assertEqual(allocation.resources["cpus_per_node_allocation"], [72, 72, 72])


class RuntimeEvidenceTests(unittest.TestCase):
    def _run(self, root: Path, code: str, *, telemetry: str = "summary"):
        snapshot = inventory_snapshot(root)
        request = execution_request_from_cli(
            [sys.executable, "-c", code], cwd=root,
            resources=ResourceRequirements(cpus=1),
            execution=ExecutionConstraints(backend="direct"),
            telemetry_mode=telemetry,
            source_kind="sdk",
        )
        workload = request_to_workload(request)
        plan = resolve_execution(workload, snapshot).selected
        self.assertIsNotNone(plan)
        return execute_plan(plan, workload, new_ulid(), request=request)  # type: ignore[arg-type]

    def test_completed_and_failed_workloads_keep_bounded_runtime_groups(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            completed = self._run(
                root,
                "import time; x=bytearray(4*1024*1024); print('ok'); time.sleep(.2)",
            )
            failed = self._run(root, "import sys; print('bad', file=sys.stderr); sys.exit(9)")

        self.assertEqual(completed.state, "completed")
        self.assertEqual(failed.state, "failed")
        self.assertEqual(failed.termination.outcome, "running_then_failed")  # type: ignore[union-attr]
        self.assertIsNotNone(failed.runtime_evidence)
        self.assertEqual(
            set(failed.runtime_evidence.coverage),  # type: ignore[union-attr]
            {"process", "allocation", "cpu", "memory", "io", "gpu", "environment"},
        )
        self.assertIn(
            completed.runtime_evidence.memory.coverage,  # type: ignore[union-attr]
            {"partially_observed", "unavailable"},
        )
        self.assertEqual(failed.experiment.exit_code, 9)  # type: ignore[union-attr]

    def test_capture_is_bounded_without_hiding_live_output(self) -> None:
        displayed = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            outcome = execute_command(
                [sys.executable, "-c", "print('x' * 4096)"], Path(directory),
                stdout_stream=displayed, collect_runtime=True,
                capture_limit_bytes=128,
            )
        self.assertEqual(outcome.status, "completed")
        self.assertLessEqual(len(outcome.stdout.encode()), 128)
        self.assertGreater(len(displayed.getvalue()), 4096)
        self.assertTrue(outcome.runtime_capture["stdout_truncated"])

    @unittest.skipUnless(os.name == "posix", "signal evidence uses POSIX process semantics")
    def test_signal_termination_is_distinct_and_partial_evidence_survives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            outcome = self._run(
                Path(directory),
                "import os,signal,time; time.sleep(.15); os.kill(os.getpid(), signal.SIGTERM)",
            )
        self.assertEqual(outcome.state, "failed")
        self.assertEqual(outcome.termination.outcome, "terminated_by_signal")  # type: ignore[union-attr]
        self.assertEqual(outcome.termination.signal, signal.SIGTERM)  # type: ignore[union-attr]
        self.assertIsNotNone(outcome.runtime_evidence)

    def test_telemetry_off_is_explicit_and_does_not_fabricate_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            outcome = self._run(Path(directory), "pass", telemetry="off")
        evidence = outcome.runtime_evidence
        self.assertEqual(evidence.cpu.coverage, "unavailable")  # type: ignore[union-attr]
        self.assertNotIn("cpu_seconds", evidence.cpu.metrics)  # type: ignore[union-attr]
        self.assertIn("disabled", evidence.cpu.diagnostic)  # type: ignore[union-attr,operator]
        # Disabling the process sampler must not suppress independently observed
        # host GPU identity, but it must never invent utilization measurements.
        self.assertIn(
            evidence.gpu.coverage,  # type: ignore[union-attr]
            {"partially_observed", "unavailable", "unsupported"},
        )
        self.assertNotIn("utilization", evidence.gpu.metrics)  # type: ignore[union-attr]

    def test_v3_result_roundtrip_keeps_runtime_relationships(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            outcome = self._run(Path(directory), "print('roundtrip')")
        parsed = parse_worker_result(
            json.loads(encode_worker_result(outcome)), outcome.execution_id
        )
        self.assertEqual(parsed.protocol_version, 3)
        self.assertEqual(
            parsed.runtime_evidence.experiment_id, parsed.experiment.id  # type: ignore[union-attr]
        )
        # Exercise the JSON-shaped payload that a released worker actually emits.
        released_v2 = json.loads(json.dumps(outcome.to_dict()))
        released_v2["schema_version"] = 2
        released_v2.pop("runtime_evidence")
        released_v2.pop("termination")
        compatible = parse_worker_result(released_v2, outcome.execution_id)
        self.assertEqual(compatible.protocol_version, 2)
        self.assertIsNone(compatible.runtime_evidence)

    def test_requested_and_observed_allocation_mismatch_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            observed = AllocationObservation(
                id=new_ulid(), execution_id=new_ulid(), observed_at=utc_now(),
                resources={"nodes": 1, "cpus": 8, "gpus": 0},
                hosts=["node-a"], evidence={"environment": {}, "source": "test"},
            )
            with patch(
                "bourneprov.compute_worker._observe_allocation",
                return_value=observed,
            ), patch(
                "bourneprov.lifecycle.collect_system",
                return_value=SystemProvenance(
                    operating_system="Linux", os_version="test",
                    architecture="x86_64", hostname="gpu-a", cpu="test-cpu",
                    gpu_available=True,
                    gpus=[{"index": "0", "name": "Test GPU", "uuid": "GPU-test"}],
                    nvidia_driver_version="999.1", cuda_version="99.0",
                    cuda_version_source="test nvidia-smi",
                ),
            ):
                outcome = self._run(root, "pass")
        mismatches = outcome.runtime_evidence.allocation.metrics["discrepancies"]  # type: ignore[union-attr]
        self.assertIn(
            {"resource": "cpus", "requested": 1, "observed": 8}, mismatches
        )

    def test_visible_gpu_identity_is_partial_not_utilization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            observed = AllocationObservation(
                id=new_ulid(), execution_id=new_ulid(), observed_at=utc_now(),
                resources={"nodes": 1, "cpus": 1, "gpus": 2},
                hosts=["gpu-a"],
                evidence={
                    "environment": {"cuda_visible_devices": "0,1"},
                    "source": "compute_worker_allowlist",
                },
            )
            with patch(
                "bourneprov.compute_worker._observe_allocation",
                return_value=observed,
            ), patch(
                "bourneprov.lifecycle.collect_system",
                return_value=SystemProvenance(
                    operating_system="Linux", os_version="test",
                    architecture="x86_64", hostname="gpu-a", cpu="test-cpu",
                    gpu_available=True,
                    gpus=[{"index": "0", "name": "Test GPU", "uuid": "GPU-test"}],
                    nvidia_driver_version="999.1", cuda_version="99.0",
                    cuda_version_source="test nvidia-smi",
                ),
            ):
                outcome = self._run(root, "pass")
        gpu = outcome.runtime_evidence.gpu  # type: ignore[union-attr]
        self.assertEqual(gpu.coverage, "partially_observed")
        self.assertEqual(gpu.metrics["visible_devices"], "0,1")
        self.assertEqual(gpu.metrics["nvidia_devices"][0]["uuid"], "GPU-test")
        self.assertNotIn("utilization", gpu.metrics)

    def test_unavailable_nvidia_evidence_does_not_block_execution(self) -> None:
        unavailable = SystemProvenance(
            operating_system="Linux", os_version="test",
            architecture="x86_64", hostname="cpu-a", cpu="test-cpu",
            gpu_available=False, gpu_error="nvidia-smi executable not found",
        )
        with tempfile.TemporaryDirectory() as directory, patch(
            "bourneprov.compute_worker.collect_system", return_value=unavailable
        ), patch(
            "bourneprov.lifecycle.collect_system", return_value=unavailable
        ):
            outcome = self._run(Path(directory), "print('no-nvidia-required')")
        self.assertEqual(outcome.state, "completed")
        gpu = outcome.runtime_evidence.gpu  # type: ignore[union-attr]
        self.assertIn(gpu.coverage, {"unavailable", "unsupported"})
        self.assertNotIn("utilization", gpu.metrics)

    def test_runtime_evidence_persists_and_reopens_with_execution_view(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root)
            InventoryStore(database).save(snapshot)
            request = execution_request_from_cli(
                [sys.executable, "-c", "print('persisted')"], cwd=root,
                execution=ExecutionConstraints(backend="direct"),
                source_kind="sdk",
            )
            store = ExecutionStore(database)
            service = ExecutionService(store, InventoryStore(database))
            outcome = service.execute_request(request, snapshot).result
            reopened = ExecutionStore(database)
            evidence = reopened.runtime_evidence(outcome.execution_id)  # type: ignore[union-attr]
            view = reopened.view(outcome.execution_id)  # type: ignore[union-attr]
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence[0].execution_id, outcome.execution_id)  # type: ignore[index,union-attr]
        self.assertIsNotNone(view.runtime_evidence)
        self.assertEqual(view.termination["outcome"], "completed")  # type: ignore[index]
        rendered = format_execution(view)
        self.assertIn("Termination: completed (running)", rendered)
        self.assertIn("process:", rendered)


class ContainerExecutionTests(unittest.TestCase):
    def _plan(self, root: Path, container: ContainerExecution):
        snapshot = inventory_snapshot(root)
        request = execution_request_from_cli(
            [sys.executable, "-c", "print('scientific')", ";touch", "$HOME"],
            cwd=root, execution=ExecutionConstraints(backend="direct"),
            source_kind="sdk",
        )
        workload = request_to_workload(request)
        plan = resolve_execution(workload, snapshot).selected
        return request, workload, replace(plan, container=container)  # type: ignore[arg-type]

    def test_existing_image_exact_argv_and_no_shell_or_image_management(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            runtime = binary / "apptainer"
            runtime.write_text(
                "#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n",
                encoding="utf-8",
            )
            runtime.chmod(0o755)
            image = root / "science.sif"
            image.write_bytes(b"existing-image")
            digest = "sha256:" + hashlib.sha256(image.read_bytes()).hexdigest()
            mount = root / "input"
            mount.mkdir()
            container = ContainerExecution(
                runtime="apptainer", image=str(image), image_digest=digest,
                mounts=(ContainerMount(str(mount), "/input", True),),
            )
            request, workload, plan = self._plan(root, container)
            with patch.dict(
                os.environ,
                {"PATH": str(binary) + os.pathsep + os.environ.get("PATH", "")},
                clear=False,
            ):
                outcome = execute_plan(plan, workload, new_ulid(), request=request)

        self.assertEqual(outcome.state, "completed")
        argv = [outcome.experiment.command, *outcome.experiment.arguments]  # type: ignore[union-attr]
        self.assertEqual(argv[0], "apptainer")
        self.assertEqual(argv[-len(plan.argv):], plan.argv)
        self.assertIn("--cleanenv", argv)
        self.assertNotIn("build", argv)
        self.assertNotIn("pull", argv)
        self.assertFalse((root / "touch").exists())

    def test_missing_runtime_or_image_fails_preflight_without_science(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = root / "missing.sif"
            container = ContainerExecution(runtime="apptainer", image=str(image))
            request, workload, plan = self._plan(root, container)
            with patch("bourneprov.compute_worker.shutil.which", return_value=None):
                outcome = execute_plan(plan, workload, new_ulid(), request=request)
        self.assertEqual(outcome.state, "preflight_failed")
        self.assertIsNone(outcome.experiment)
        self.assertIn("container runtime", outcome.error)
        self.assertIn("container image", outcome.error)

    def test_container_plan_uses_v4_and_reopens_without_reinterpreting_v3(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = root / "science.sif"
            image.write_bytes(b"image")
            request, workload, plan = self._plan(
                root, ContainerExecution(runtime="singularity", image=str(image))
            )
            execution_id = new_ulid()
            path = write_staged_plan(
                root / "plan.json", execution_id, plan, workload, request
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            reopened, reopened_workload, reopened_request = _load_plan(
                path, execution_id
            )
        self.assertEqual(payload["schema_version"], 4)
        self.assertEqual(reopened.container, plan.container)
        self.assertEqual(reopened_workload, workload)
        self.assertEqual(reopened_request.id, request.id)  # type: ignore[union-attr]

    def test_current_request_uses_v4_while_released_v3_remains_readable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, workload, current = self._plan(
                root,
                ContainerExecution(runtime="apptainer", image=str(root / "science.sif")),
            )
            current = replace(current, container=None)
            execution_id = new_ulid()
            current_path = write_staged_plan(
                root / "current.json", execution_id, current, workload, request
            )
            self.assertEqual(
                json.loads(current_path.read_text(encoding="utf-8"))["schema_version"],
                4,
            )

            released_request = replace(request, request_schema_version=1)
            released_plan = replace(
                current,
                resource_shape=ResourceShape(
                    nodes=1, cpus_per_node=1, total_cpus=1
                ),
            )
            released_id = new_ulid()
            released_path = write_staged_plan(
                root / "released-v3.json", released_id,
                released_plan, workload, released_request,
            )
            self.assertEqual(
                json.loads(released_path.read_text(encoding="utf-8"))["schema_version"],
                3,
            )
            reopened, _, reopened_request = _load_plan(released_path, released_id)
        self.assertEqual(reopened.resource_shape, released_plan.resource_shape)
        self.assertEqual(reopened_request.request_schema_version, 1)  # type: ignore[union-attr]

    def test_bind_grammar_cannot_reinterpret_one_typed_mount(self) -> None:
        for path in ("/source:other", "/destination,extra"):
            with self.subTest(path=path), self.assertRaisesRegex(
                ValueError, "unsafe characters"
            ):
                ContainerMount(path, "/safe")


class SchemaSevenTests(unittest.TestCase):
    def test_schema_six_migrates_to_seven_with_runtime_table(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            ExperimentStore(database).initialize()
            snapshot = inventory_snapshot(root)
            InventoryStore(database).save(snapshot)
            request = execution_request_from_cli(
                [sys.executable, "-c", "pass"], cwd=root,
                execution=ExecutionConstraints(backend="direct"),
                source_kind="sdk",
            )
            store = ExecutionStore(database)
            planned = ExecutionService(store, InventoryStore(database)).plan_request(
                request, snapshot
            )
            self.assertIsNotNone(planned.selected)
            import sqlite3

            connection = sqlite3.connect(database)
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("DROP TABLE runtime_evidence")
            connection.execute("PRAGMA user_version = 6")
            connection.commit()
            connection.close()
            ExperimentStore(database).initialize()
            connection = sqlite3.connect(database)
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
            table = connection.execute(
                "SELECT name FROM sqlite_master WHERE name = 'runtime_evidence'"
            ).fetchone()
            request_count = connection.execute(
                "SELECT count(*) FROM execution_requests"
            ).fetchone()[0]
            plan_count = connection.execute(
                "SELECT count(*) FROM execution_plans"
            ).fetchone()[0]
            connection.close()
        self.assertEqual(version, 7)
        self.assertEqual(integrity, "ok")
        self.assertEqual(foreign_keys, [])
        self.assertIsNotNone(table)
        self.assertEqual(request_count, 1)
        self.assertEqual(plan_count, 1)


if __name__ == "__main__":
    unittest.main()
