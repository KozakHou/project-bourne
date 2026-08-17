from __future__ import annotations

import getpass
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.backends import BackendError, PBSBackend, SlurmBackend, render_batch_script
from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.ids import new_ulid
from bourneprov.inventory_storage import InventoryStore
from bourneprov.resolver import resolve_execution
from bourneprov.workload import inspect_workload, utc_now
from bourneprov.workload_models import ExecutionAttempt, ExecutionConstraints, ResourceRequirements
from bourneprov.workload_storage import ExecutionStore
from tests.v04_fixtures import inventory_snapshot


def command_result(argv: list[str], stdout: str = "", stderr: str = "", returncode: int = 0):
    return BoundedCommandResult(tuple(argv), returncode, stdout, stderr)


class BackendTests(unittest.TestCase):
    def _execution(
        self, root: Path, family: str, *, argv: list[str] | None = None,
        resources: ResourceRequirements | None = None,
    ):
        database = root / "bourne.sqlite3"
        snapshot = inventory_snapshot(root, scheduler_families=(family,))
        InventoryStore(database).save(snapshot)
        workload = inspect_workload(
            argv or ["solver", "a b", ";touch", "$HOME", "`id`", "λ"], cwd=root,
            resources=resources or ResourceRequirements(cpus=4, nodes=1, walltime_seconds=60),
            constraints=ExecutionConstraints(backend=family),
        )
        plan = resolve_execution(workload, snapshot).selected
        self.assertIsNotNone(plan)
        store = ExecutionStore(database)
        store.save_workload(workload)
        store.save_plan(plan)  # type: ignore[arg-type]
        now = utc_now()
        execution = ExecutionAttempt(
            id=new_ulid(), plan_id=plan.id, backend=family, state="planned",  # type: ignore[union-attr]
            created_at=now, updated_at=now, submitting_identity=getpass.getuser(),
        )
        store.create_execution(execution)
        return store, snapshot, workload, plan, execution

    def test_batch_script_never_contains_scientific_argv_and_quotes_worker_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _store, snapshot, _workload, plan, execution = self._execution(root, "slurm")
            script = render_batch_script(
                "slurm", plan, Path("/shared/worker with space.pyz"),  # type: ignore[arg-type]
                Path("/shared/plan'$x.json"), Path("/shared/result`x`.json"),
                execution.id, target_name=snapshot.execution_targets[0].name,
            )
        for value in ("a b", ";touch", "$HOME", "`id`", "λ"):
            self.assertNotIn(value, script)
        self.assertIn("python3", script)
        self.assertIn("'/shared/worker with space.pyz'", script)
        self.assertNotIn("shell=True", script)

    def test_unsafe_scheduler_target_name_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _store, _snapshot, _workload, plan, execution = self._execution(Path(directory), "slurm")
            with self.assertRaisesRegex(ValueError, "unsafe"):
                render_batch_script(
                    "slurm", plan, Path("worker"), Path("plan"), Path("result"),  # type: ignore[arg-type]
                    execution.id, target_name="gpu\nwhoami",
                )

    def test_slurm_submission_uses_parsable_shell_free_argv_and_persists_job(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            calls.append(list(argv))
            return command_result(list(argv), stdout="12345;cluster\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                submission = backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
            job = store.get_scheduler_job(execution.id)
            current = store.get_execution(execution.id)

        self.assertEqual(submission.job_id, "12345")
        self.assertEqual(calls[0][0:2], ["/usr/bin/sbatch", "--parsable"])
        self.assertEqual(job.job_id, "12345")  # type: ignore[union-attr]
        self.assertEqual(current.state, "submitted")

    def test_pbs_submission_parses_scoped_job_id(self) -> None:
        def runner(argv, **_kwargs):
            return command_result(list(argv), stdout="88.server\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/qsub"):
                submission = backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]

        self.assertEqual(submission.job_id, "88.server")

    def test_slurm_status_is_restricted_to_known_job_and_submitting_user(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            calls.append(list(argv))
            if "--parsable" in argv:
                return command_result(list(argv), stdout="123\n")
            return command_result(list(argv), stdout="RUNNING\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                state = backend.status(store.get_execution(execution.id))

        self.assertEqual(state, "running")
        self.assertEqual(
            calls[1],
            ["/usr/bin/squeue", "--noheader", "--jobs", "123", "--user", getpass.getuser(), "--format=%T"],
        )

    def test_cancel_rejects_identity_mismatch_before_scheduler_command(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            calls.append(list(argv))
            return command_result(list(argv), stdout="123\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
            with patch("bourneprov.backends._current_identity", return_value="someone-else"):
                with self.assertRaisesRegex(BackendError, "did not submit"):
                    backend.cancel(store.get_execution(execution.id))

        self.assertEqual(len(calls), 1)

    def test_scheduler_completion_without_result_is_collection_failed(self) -> None:
        def runner(argv, **_kwargs):
            if "--parsable" in argv:
                return command_result(list(argv), stdout="123\n")
            return command_result(list(argv), stdout="COMPLETED\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                with self.assertRaisesRegex(BackendError, "unavailable"):
                    backend.wait(store.get_execution(execution.id), poll_seconds=0.01, timeout_seconds=1)
            current = store.get_execution(execution.id)

        self.assertEqual(current.state, "collection_failed")
        self.assertIsNone(store.experiment_id(execution.id))

    def test_slurm_compute_worker_result_is_collected_transactionally(self) -> None:
        def runner(argv, **_kwargs):
            return command_result(list(argv), stdout="321\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(
                root, "slurm",
                argv=[sys.executable, "-c", "print('scheduled-worker')"],
                resources=ResourceRequirements(gpus=1),
            )
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
            current = store.get_execution(execution.id)
            staging = Path(current.staging_directory)  # type: ignore[arg-type]
            process = subprocess.run(
                [
                    sys.executable, str(staging / "worker.pyz"),
                    str(staging / "plan.json"), str(staging / "result.json"),
                    execution.id,
                ],
                env={**os.environ, "CUDA_VISIBLE_DEVICES": "0"},
                capture_output=True, text=True, check=False,
            )
            result = backend.collect(store.get_execution(execution.id))
            repeated = backend.collect(store.get_execution(execution.id))
            view = store.view(execution.id)

        self.assertEqual(process.returncode, 0)
        self.assertEqual(result.state, "completed")
        self.assertEqual(repeated.experiment.id, result.experiment.id)  # type: ignore[union-attr]
        self.assertEqual(view.execution.state, "completed")
        self.assertEqual(view.experiment_id, result.experiment.id)  # type: ignore[union-attr]
        self.assertEqual(view.allocations[0].resources["gpus"], 1)
        self.assertIn("scheduled-worker", result.experiment.stdout)  # type: ignore[union-attr]

    def test_malformed_submission_and_permission_failure_are_persisted(self) -> None:
        for stdout, stderr, returncode, diagnostic in (
            ("not-a-job\n", "", 0, "invalid job ID"),
            ("", "permission denied", 1, "permission denied"),
        ):
            with self.subTest(diagnostic=diagnostic), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                store, snapshot, workload, plan, execution = self._execution(root, "slurm")

                def runner(argv, **_kwargs):
                    return command_result(list(argv), stdout=stdout, stderr=stderr, returncode=returncode)

                backend = SlurmBackend(store, root / "stage", runner=runner)
                with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                    with self.assertRaisesRegex(BackendError, diagnostic):
                        backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                current = store.get_execution(execution.id)
                self.assertEqual(current.state, "failed")

    def test_scheduler_timeout_is_not_success(self) -> None:
        def runner(argv, **_kwargs):
            return BoundedCommandResult(tuple(argv), -9, "", "", timed_out=True)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/qsub"):
                with self.assertRaisesRegex(BackendError, "timed out"):
                    backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
            current = store.get_execution(execution.id)
            experiment_id = store.experiment_id(execution.id)

        self.assertEqual(current.state, "failed")
        self.assertIsNone(experiment_id)

    def test_slurm_and_pbs_status_parsers_keep_scheduler_state_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = ExecutionStore(Path(directory) / "bourne.sqlite3")
            slurm = SlurmBackend(store, Path(directory))
            pbs = PBSBackend(store, Path(directory))
            self.assertEqual(slurm._parse_status(command_result([], stdout="PENDING\n")), "pending")
            self.assertEqual(slurm._parse_status(command_result([], stdout="FAILED\n")), "failed")
            self.assertEqual(pbs._parse_status(command_result([], stdout="    job_state = Q\n")), "queued")
            self.assertEqual(pbs._parse_status(command_result([], stdout="    job_state = R\n")), "running")
            self.assertEqual(pbs._parse_status(command_result([], stdout="    job_state = F\n")), "finished")

    def test_pbs_cancellation_uses_only_recorded_job_id_and_persists_intent(self) -> None:
        calls: list[list[str]] = []

        def runner(argv, **_kwargs):
            calls.append(list(argv))
            if Path(argv[0]).name == "qsub":
                return command_result(list(argv), stdout="88.server\n")
            return command_result(list(argv))

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "pbs")
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
                backend.cancel(store.get_execution(execution.id))
            states = [item.state for item in store.events(execution.id)]
            current = store.get_execution(execution.id)

        self.assertEqual(calls[-1], ["/usr/bin/qdel", "88.server"])
        self.assertIn("cancellation_requested", states)
        self.assertEqual(states[-1], "cancelled")
        self.assertEqual(current.state, "cancelled")

    def test_malformed_result_bundle_is_not_imported(self) -> None:
        def runner(argv, **_kwargs):
            return command_result(list(argv), stdout="123\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(root, "slurm")
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
            current = store.get_execution(execution.id)
            (Path(current.staging_directory) / "result.json").write_text("{}", encoding="utf-8")  # type: ignore[arg-type]
            with self.assertRaisesRegex(BackendError, "schema"):
                backend.collect(current)
            experiment_id = store.experiment_id(execution.id)

        self.assertIsNone(experiment_id)

    def test_result_experiment_must_match_immutable_plan(self) -> None:
        def runner(argv, **_kwargs):
            return command_result(list(argv), stdout="123\n")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, snapshot, workload, plan, execution = self._execution(
                root, "slurm", argv=[sys.executable, "-c", "print('real')"]
            )
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                backend.execute(execution, plan, workload, snapshot)  # type: ignore[arg-type]
            current = store.get_execution(execution.id)
            staging = Path(current.staging_directory)  # type: ignore[arg-type]
            subprocess.run(
                [
                    sys.executable, str(staging / "worker.pyz"),
                    str(staging / "plan.json"), str(staging / "result.json"),
                    execution.id,
                ],
                capture_output=True, text=True, check=False,
            )
            result_path = staging / "result.json"
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            payload["experiment"]["arguments"] = ["-c", "print('forged')"]
            result_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(BackendError, "immutable plan"):
                backend.collect(current)
            imported = store.experiment_id(execution.id)

        self.assertIsNone(imported)


if __name__ == "__main__":
    unittest.main()
