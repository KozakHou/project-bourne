from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.backends import BackendError, PBSBackend, SlurmBackend
from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.identity import ProcessIdentity, current_process_identity
from bourneprov.ids import new_ulid
from bourneprov.inventory_storage import InventoryStore
from bourneprov.resolver import resolve_execution
from bourneprov.workload import inspect_workload, utc_now
from bourneprov.workload_models import (
    ExecutionAttempt,
    ExecutionConstraints,
    ResourceRequirements,
)
from bourneprov.workload_storage import ExecutionStore
from tests.v04_fixtures import inventory_snapshot


def command_result(
    argv: list[str], stdout: str = "", stderr: str = "", returncode: int = 0
) -> BoundedCommandResult:
    return BoundedCommandResult(tuple(argv), returncode, stdout, stderr)


class SchedulerRuntimeTests(unittest.TestCase):
    def _execution(
        self, root: Path, family: str, *, submitting_identity: str | None = None
    ):
        database = root / "bourne.sqlite3"
        snapshot = inventory_snapshot(root, scheduler_families=(family,))
        InventoryStore(database).save(snapshot)
        workload = inspect_workload(
            [sys.executable, "-c", "print('scheduler-science')"],
            cwd=root,
            resources=ResourceRequirements(cpus=1, nodes=1),
            constraints=ExecutionConstraints(backend=family),
        )
        plan = resolve_execution(workload, snapshot).selected
        self.assertIsNotNone(plan)
        store = ExecutionStore(database)
        store.save_workload(workload)
        store.save_plan(plan)  # type: ignore[arg-type]
        now = utc_now()
        execution = ExecutionAttempt(
            id=new_ulid(),
            plan_id=plan.id,  # type: ignore[union-attr]
            backend=family,
            state="planned",
            created_at=now,
            updated_at=now,
            submitting_identity=(
                current_process_identity().username
                if submitting_identity is None
                else submitting_identity
            ),
        )
        store.create_execution(execution)
        return store, snapshot, workload, plan, execution

    def _run_worker(self, execution: ExecutionAttempt) -> None:
        staging = Path(execution.staging_directory)  # type: ignore[arg-type]
        process = subprocess.run(
            [
                sys.executable,
                str(staging / "worker.pyz"),
                str(staging / "plan.json"),
                str(staging / "result.json"),
                execution.id,
            ],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(process.returncode, 0, process.stderr)

    def test_slurm_queued_running_worker_result_completes_experiment(self) -> None:
        calls: list[list[str]] = []
        status_count = 0

        def runner(argv, **_kwargs):
            nonlocal status_count
            values = list(argv)
            calls.append(values)
            command = Path(values[0]).name
            if command == "sbatch":
                return command_result(values, stdout="123\n")
            if command == "squeue":
                status_count += 1
                if status_count == 1:
                    return command_result(values, stdout="PENDING\n")
                self._run_worker(store.get_execution(execution.id))
                return command_result(values, stdout="RUNNING\n")
            self.fail(f"unexpected command: {values}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                result = backend.wait(
                    store.get_execution(execution.id), poll_seconds=0.001
                )

        self.assertEqual(result.state, "completed")
        self.assertIsNotNone(result.experiment)
        self.assertEqual([Path(call[0]).name for call in calls], ["sbatch", "squeue", "squeue"])

    def test_slurm_completed_accounting_without_result_is_collection_failed(self) -> None:
        self._assert_slurm_terminal_without_result("COMPLETED", "completed")

    def test_slurm_failed_accounting_without_result_is_collection_failed(self) -> None:
        self._assert_slurm_terminal_without_result("FAILED", "failed")

    def _assert_slurm_terminal_without_result(
        self, reported_state: str, normalized_state: str
    ) -> None:
        active_count = 0

        def runner(argv, **_kwargs):
            nonlocal active_count
            values = list(argv)
            command = Path(values[0]).name
            if command == "sbatch":
                return command_result(values, stdout="123\n")
            if command == "squeue":
                active_count += 1
                return command_result(
                    values, stdout="RUNNING\n" if active_count == 1 else ""
                )
            if command == "sacct":
                return command_result(values, stdout=f"123|{reported_state}\n")
            self.fail(f"unexpected command: {values}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaisesRegex(BackendError, "not established"):
                    backend.wait(
                        store.get_execution(execution.id), poll_seconds=0.001
                    )
            current = store.get_execution(execution.id)
            job = store.get_scheduler_job(execution.id)
            details = store.events(execution.id)[-1].details
            experiment_id = store.experiment_id(execution.id)

        self.assertEqual(current.state, "collection_failed")
        self.assertEqual(job.state, normalized_state)  # type: ignore[union-attr]
        self.assertEqual(details["observation_source"], "terminal_accounting")
        self.assertFalse(details["scientific_completion_established"])
        self.assertIsNone(experiment_id)

    def test_slurm_unobservable_without_accounting_is_finite_non_success(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "sbatch":
                return command_result(values, stdout="123\n")
            return command_result(values)

        def which(name: str) -> str | None:
            return None if name == "sacct" else f"/usr/bin/{name}"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", side_effect=which):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaisesRegex(BackendError, "not established"):
                    backend.wait(store.get_execution(execution.id), poll_seconds=0.001)
            details = store.events(execution.id)[-1].details
            current_state = store.get_execution(execution.id).state

        self.assertEqual(current_state, "collection_failed")
        self.assertEqual(details["observation_source"], "accounting_unavailable")
        self.assertFalse(details["job_observable"])
        self.assertFalse(details["scheduler_terminal"])
        self.assertFalse(details["scientific_completion_established"])
        self.assertEqual([Path(call[0]).name for call in calls], ["sbatch", "squeue"])

    def test_slurm_accounting_error_is_distinct_and_finite(self) -> None:
        def runner(argv, **_kwargs):
            values = list(argv)
            command = Path(values[0]).name
            if command == "sbatch":
                return command_result(values, stdout="123\n")
            if command == "squeue":
                return command_result(values)
            if command == "sacct":
                return command_result(
                    values, stderr="accounting storage unavailable", returncode=1
                )
            self.fail(f"unexpected command: {values}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaisesRegex(BackendError, "not established"):
                    backend.wait(store.get_execution(execution.id), poll_seconds=0.001)
            details = store.events(execution.id)[-1].details

        self.assertEqual(details["observation_source"], "accounting_error")
        self.assertFalse(details["job_observable"])
        self.assertFalse(details["scheduler_terminal"])
        self.assertIn("accounting storage unavailable", details["diagnostic"])

    def test_slurm_extended_terminal_states_are_truthful(self) -> None:
        for scheduler_state, normalized in (
            ("OUT_OF_MEMORY", "out_of_memory"),
            ("PREEMPTED", "preempted"),
            ("NODE_FAIL", "node_fail"),
            ("TIMEOUT", "timeout"),
            ("BOOT_FAIL", "boot_fail"),
            ("DEADLINE", "deadline"),
        ):
            with self.subTest(state=scheduler_state), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                store, snapshot, workload, plan, execution = self._execution(root, "slurm")

                def runner(argv, **_kwargs):
                    values = list(argv)
                    command = Path(values[0]).name
                    if command == "sbatch":
                        return command_result(values, stdout="123\n")
                    if command == "squeue":
                        return command_result(values)
                    if command == "sacct":
                        return command_result(values, stdout=f"123|{scheduler_state}\n")
                    self.fail(f"unexpected command: {values}")

                backend = SlurmBackend(store, root / "stage", runner=runner)
                with patch(
                    "bourneprov.backends.shutil.which",
                    side_effect=lambda name: f"/usr/bin/{name}",
                ):
                    backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                    with self.assertRaises(BackendError):
                        backend.wait(store.get_execution(execution.id), poll_seconds=0.001)

                self.assertEqual(store.get_execution(execution.id).state, "collection_failed")
                self.assertEqual(store.get_scheduler_job(execution.id).state, normalized)  # type: ignore[union-attr]
                self.assertIsNone(store.experiment_id(execution.id))

    def test_result_before_scheduler_terminal_is_collected_without_query(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "sbatch":
                return command_result(values, stdout="123\n")
            self.fail("wait queried the scheduler after a valid result existed")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                self._run_worker(store.get_execution(execution.id))
                result = backend.wait(store.get_execution(execution.id))

        self.assertEqual(result.state, "completed")
        self.assertEqual([Path(call[0]).name for call in calls], ["sbatch"])

    def test_default_polling_backs_off_without_busy_polling(self) -> None:
        delays: list[float] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            if Path(values[0]).name == "sbatch":
                return command_result(values, stdout="123\n")
            return command_result(values, stdout="RUNNING\n")

        def fake_sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 6:
                self._run_worker(store.get_execution(execution.id))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with (
                patch(
                    "bourneprov.backends.shutil.which",
                    side_effect=lambda name: f"/usr/bin/{name}",
                ),
                patch("bourneprov.backends.time.sleep", side_effect=fake_sleep),
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                result = backend.wait(store.get_execution(execution.id))

        self.assertEqual(result.state, "completed")
        self.assertEqual(delays, [15.0, 22.5, 33.75, 50.625, 60.0, 60.0])

    def test_slurm_requeue_states_are_not_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            backend = SlurmBackend(
                ExecutionStore(Path(directory) / "bourne.sqlite3"),
                Path(directory) / "stage",
            )
        for state in ("requeued", "requeue_fed", "requeue_hold", "resizing"):
            self.assertNotIn(state, backend.terminal_states)

    def test_slurm_queries_never_broaden_beyond_exact_job_and_identity(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            command = Path(values[0]).name
            if command == "sbatch":
                return command_result(values, stdout="123\n")
            if command == "squeue":
                return command_result(values)
            if command == "sacct":
                return command_result(values, stdout="123|FAILED\n")
            self.fail(f"unexpected command: {values}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaises(BackendError):
                    backend.wait(store.get_execution(execution.id), poll_seconds=0.001)

        identity = current_process_identity().username
        scheduler_calls = [call for call in calls if Path(call[0]).name != "sbatch"]
        self.assertEqual(len(scheduler_calls), 2)
        for call in scheduler_calls:
            self.assertIn("--jobs", call)
            self.assertEqual(call[call.index("--jobs") + 1], "123")
            self.assertIn("--user", call)
            self.assertEqual(call[call.index("--user") + 1], identity)

    def test_pbs_visible_active_job(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "qsub":
                return command_result(values, stdout="88.server\n")
            return command_result(values, stdout="    job_state = R\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                state = backend.status(store.get_execution(execution.id))

        self.assertEqual(state, "running")
        self.assertEqual(calls[-1], ["/usr/bin/qstat", "-f", "88.server"])

    def test_pbs_finished_job_with_result_collects(self) -> None:
        def runner(argv, **_kwargs):
            values = list(argv)
            if Path(values[0]).name == "qsub":
                return command_result(values, stdout="88.server\n")
            self._run_worker(store.get_execution(execution.id))
            return command_result(values, stdout="    job_state = F\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                result = backend.wait(store.get_execution(execution.id), poll_seconds=0.001)
            experiment_id = store.experiment_id(execution.id)

        self.assertEqual(result.state, "completed")
        self.assertIsNotNone(experiment_id)

    def test_pbs_unobservable_missing_result_is_collection_failed(self) -> None:
        def runner(argv, **_kwargs):
            values = list(argv)
            if Path(values[0]).name == "qsub":
                return command_result(values, stdout="88.server\n")
            return command_result(
                values, stderr="qstat: Unknown Job Id 88.server\n", returncode=153
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaisesRegex(BackendError, "not established"):
                    backend.wait(store.get_execution(execution.id), poll_seconds=0.001)
            details = store.events(execution.id)[-1].details
            current_state = store.get_execution(execution.id).state
            experiment_id = store.experiment_id(execution.id)

        self.assertEqual(current_state, "collection_failed")
        self.assertFalse(details["job_observable"])
        self.assertFalse(details["scheduler_terminal"])
        self.assertFalse(details["scientific_completion_established"])
        self.assertIsNone(experiment_id)

    def test_pbs_status_command_error_is_finite_and_not_success(self) -> None:
        def runner(argv, **_kwargs):
            values = list(argv)
            if Path(values[0]).name == "qsub":
                return command_result(values, stdout="88.server\n")
            return command_result(values, stderr="server connection refused", returncode=2)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch(
                "bourneprov.backends.shutil.which",
                side_effect=lambda name: f"/usr/bin/{name}",
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaisesRegex(BackendError, "connection refused"):
                    backend.wait(store.get_execution(execution.id), poll_seconds=0.001)
            states = [event.state for event in store.events(execution.id)]
            experiment_id = store.experiment_id(execution.id)

        self.assertIn("scheduler_query_error", states)
        self.assertIsNone(experiment_id)

    def test_matching_trusted_identity_allows_exact_managed_job_cancel(self) -> None:
        calls: list[list[str]] = []
        trusted = ProcessIdentity(
            "alice", 1001, "posix_effective_uid_password_database"
        )

        def runner(argv, **_kwargs):
            values = list(argv)
            calls.append(values)
            if Path(values[0]).name == "sbatch":
                return command_result(values, stdout="123\n")
            return command_result(values)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(
                root, "slurm", submitting_identity="alice"
            )
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with (
                patch(
                    "bourneprov.backends.shutil.which",
                    side_effect=lambda name: f"/usr/bin/{name}",
                ),
                patch(
                    "bourneprov.backends.current_process_identity",
                    return_value=trusted,
                ),
            ):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                backend.cancel(store.get_execution(execution.id))

        self.assertEqual(calls[-1], ["/usr/bin/scancel", "--user", "alice", "123"])


if __name__ == "__main__":
    unittest.main()
