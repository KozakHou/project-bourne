from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from bourneprov.backends import BackendError, PBSBackend, SlurmBackend
from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.execution_outcomes import (
    build_telemetry_summary,
    evaluate_verification,
)
from bourneprov.execution_request import (
    REQUEST_KIND,
    RequestSource,
    parse_execution_request,
)
from bourneprov.execution_service import ExecutionService, request_to_workload
from bourneprov.ids import new_ulid
from bourneprov.inventory_storage import InventoryStore
from bourneprov.models import Artifact
from bourneprov.resolver import resolve_execution
from bourneprov.worker_result import load_worker_result
from bourneprov.workload import inspect_workload
from bourneprov.workload_models import AllocationObservation, ExecutionConstraints
from bourneprov.workload_storage import ExecutionStore
from tests.fixtures import experiment
from tests.v04_fixtures import inventory_snapshot


def request(
    root: Path,
    *,
    outputs: list[str] | None = None,
    checks: list[dict[str, object]] | None = None,
    telemetry: str = "summary",
    command: list[str] | None = None,
    backend: str = "direct",
    resources: dict[str, object] | None = None,
):
    value = {
        "kind": REQUEST_KIND,
        "version": 1,
        "command": command or ["solver"],
        "working_directory": ".",
        "artifacts": {"outputs": outputs or []},
        "execution": {"backend": backend},
        "telemetry": {"mode": telemetry},
        "verification": {"checks": checks or []},
    }
    if resources is not None:
        value["resources"] = resources
    return parse_execution_request(
        value, base_directory=root, source=RequestSource("sdk")
    )


def artifact(
    experiment_id: str,
    path: str,
    *,
    existence: str = "present",
    capture: str = "complete",
    size: int | None = 10,
    sha: str | None = "a" * 64,
) -> Artifact:
    return Artifact(
        id=new_ulid(), experiment_id=experiment_id, role="output",
        original_path=path, resolved_path=f"/captured/{path}",
        existence_state=existence, capture_status=capture,
        sha256=sha, size_bytes=size,
        modified_at="2026-01-01T00:00:00Z" if existence == "present" else None,
        captured_at="2026-01-01T00:00:01Z",
        capture_error=None if capture == "complete" else "capture incomplete",
    )


class VerificationTests(unittest.TestCase):
    def _evaluate(self, root: Path, checks, artifacts):
        req = request(root, outputs=["out.dat"], checks=checks)
        exp = experiment(working_directory=str(root))
        return exp, evaluate_verification(req, "01M0EXEC" + "0" * 18, exp, artifacts(exp.id))

    def test_no_checks_is_not_requested(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _exp, result = self._evaluate(Path(directory), [], lambda _id: [])
        self.assertEqual(result.aggregate_state, "not_requested")
        self.assertEqual(result.checks, ())

    def test_output_exists_present_passes_and_missing_fails(self) -> None:
        check = [{"type": "output_exists", "path": "out.dat"}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _exp, present = self._evaluate(root, check, lambda eid: [artifact(eid, "out.dat")])
            _exp, missing = self._evaluate(
                root, check,
                lambda eid: [artifact(eid, "out.dat", existence="missing", size=None, sha=None)],
            )
        self.assertEqual(present.aggregate_state, "passed")
        self.assertEqual(missing.aggregate_state, "failed")

    def test_min_bytes_pass_and_fail(self) -> None:
        check = [{"type": "output_min_bytes", "path": "out.dat", "min_bytes": 10}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _exp, passed = self._evaluate(root, check, lambda eid: [artifact(eid, "out.dat", size=10)])
            _exp, failed = self._evaluate(root, check, lambda eid: [artifact(eid, "out.dat", size=9)])
        self.assertEqual(passed.aggregate_state, "passed")
        self.assertEqual(failed.aggregate_state, "failed")

    def test_sha_match_and_mismatch(self) -> None:
        check = [{"type": "output_sha256", "path": "out.dat", "sha256": "a" * 64}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _exp, passed = self._evaluate(root, check, lambda eid: [artifact(eid, "out.dat")])
            _exp, failed = self._evaluate(root, check, lambda eid: [artifact(eid, "out.dat", sha="b" * 64)])
        self.assertEqual(passed.aggregate_state, "passed")
        self.assertEqual(failed.aggregate_state, "failed")

    def test_unreadable_and_changed_artifacts_are_unknown(self) -> None:
        checks = [
            {"type": "output_min_bytes", "path": "out.dat", "min_bytes": 1},
            {"type": "output_sha256", "path": "out.dat", "sha256": "a" * 64},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for capture in ("unreadable", "changed"):
                with self.subTest(capture=capture):
                    _exp, result = self._evaluate(
                        root, checks,
                        lambda eid, capture=capture: [
                            artifact(eid, "out.dat", capture=capture, sha=None)
                        ],
                    )
                    self.assertEqual(result.aggregate_state, "unknown")
                    self.assertEqual({item.state for item in result.checks}, {"unknown"})

    def test_aggregate_rules_preserve_unknown_and_failed(self) -> None:
        checks = [
            {"type": "output_exists", "path": "out.dat"},
            {"type": "output_sha256", "path": "out.dat", "sha256": "a" * 64},
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _exp, unknown = self._evaluate(
                root, checks,
                lambda eid: [artifact(eid, "out.dat", capture="changed", sha=None)],
            )
            failed_checks = [
                {"type": "output_exists", "path": "out.dat"},
                {"type": "output_min_bytes", "path": "out.dat", "min_bytes": 99},
            ]
            _exp, failed = self._evaluate(
                root, failed_checks, lambda eid: [artifact(eid, "out.dat", size=10)]
            )
        self.assertEqual([item.state for item in unknown.checks], ["passed", "unknown"])
        self.assertEqual(unknown.aggregate_state, "unknown")
        self.assertEqual([item.state for item in failed.checks], ["passed", "failed"])
        self.assertEqual(failed.aggregate_state, "failed")

    def test_verification_uses_captured_records_not_current_filesystem(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "out.dat"
            path.write_bytes(b"different current bytes")
            req = request(
                root, outputs=["out.dat"],
                checks=[{"type": "output_sha256", "path": "out.dat", "sha256": "a" * 64}],
            )
            exp = experiment(working_directory=str(root))
            captured = artifact(exp.id, "out.dat", sha="a" * 64)
            path.unlink()
            with patch("pathlib.Path.open", side_effect=AssertionError("must not read files")):
                result = evaluate_verification(req, "01M0EXEC" + "0" * 18, exp, [captured])
        self.assertEqual(result.aggregate_state, "passed")


class TelemetryTests(unittest.TestCase):
    def test_summary_uses_observed_execution_and_artifact_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            req = request(root, outputs=["out.dat"], resources={"cpus": 2})
            workload = request_to_workload(req)
            plan = resolve_execution(workload, inventory_snapshot(root, executable="solver")).selected
            self.assertIsNotNone(plan)
            exp = replace(
                experiment(working_directory=str(root)),
                duration_seconds=1.25,
                stdout="λ\n",
                stderr="err\n",
            )
            output = artifact(exp.id, "out.dat", size=10)
            allocation = AllocationObservation(
                id=new_ulid(), execution_id="01M0EXEC" + "0" * 18,
                observed_at=exp.ended_at, resources={"cpus": 8}, hosts=["node1"],
            )
            summary = build_telemetry_summary(
                req, plan, allocation.execution_id, exp, [output], allocation  # type: ignore[arg-type]
            )
        self.assertIsNotNone(summary)
        self.assertEqual(summary.wall_seconds, 1.25)  # type: ignore[union-attr]
        self.assertEqual(summary.stdout_bytes, len("λ\n".encode("utf-8")))  # type: ignore[union-attr]
        self.assertEqual(summary.stderr_bytes, 4)  # type: ignore[union-attr]
        self.assertEqual(summary.known_output_artifact_bytes, 10)  # type: ignore[union-attr]
        self.assertEqual(summary.requested_resources["cpus"], 2)  # type: ignore[union-attr]
        self.assertEqual(summary.allocated_resources["cpus"], 8)  # type: ignore[union-attr]
        self.assertNotIn("utilization", summary.to_dict())  # type: ignore[union-attr]

    def test_off_creates_no_summary_and_unavailable_is_not_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            req = request(root, outputs=["out.dat"], telemetry="off")
            workload = request_to_workload(req)
            plan = resolve_execution(workload, inventory_snapshot(root, executable="solver")).selected
            exp = experiment(working_directory=str(root))
            self.assertIsNone(
                build_telemetry_summary(req, plan, "execution", exp, [], None)  # type: ignore[arg-type]
            )
            summary_request = replace(req, telemetry_mode="summary")
            summary = build_telemetry_summary(
                summary_request, plan, "execution", exp, [], None  # type: ignore[arg-type]
            )
        self.assertIsNone(summary.allocated_resources)  # type: ignore[union-attr]
        self.assertIsNone(summary.known_output_artifact_bytes)  # type: ignore[union-attr]
        self.assertIn("allocated_resources", summary.unavailable)  # type: ignore[union-attr]


class OutcomeIntegrationTests(unittest.TestCase):
    def test_v05_controller_executes_v04_plan_without_inventing_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root)
            InventoryStore(database).save(snapshot)
            workload = inspect_workload(
                [sys.executable, "-c", "print('v04-plan')"],
                cwd=root,
                constraints=ExecutionConstraints(backend="direct"),
            )
            plan = resolve_execution(workload, snapshot).selected
            store = ExecutionStore(database)
            store.save_workload(workload)
            store.save_plan(plan)  # type: ignore[arg-type]
            service = ExecutionService(store, InventoryStore(database))
            with contextlib.redirect_stdout(io.StringIO()):
                result = service.execute_plan(plan.id, snapshot)  # type: ignore[union-attr]
            self.assertEqual(result.protocol_version, 1)  # type: ignore[union-attr]
            self.assertIsNone(store.request_for_execution(result.execution_id))  # type: ignore[union-attr]
            self.assertIsNone(store.telemetry(result.execution_id))  # type: ignore[union-attr]
            self.assertIsNone(store.verification(result.execution_id))  # type: ignore[union-attr]

    def test_direct_success_persists_summary_and_verification_after_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root)
            InventoryStore(database).save(snapshot)
            code = (
                "from pathlib import Path; import sys; "
                "print('out-λ'); print('err', file=sys.stderr); "
                "Path('result.txt').write_text('payload', encoding='utf-8')"
            )
            req = request(
                root,
                outputs=["result.txt"],
                checks=[
                    {"type": "output_exists", "path": "result.txt"},
                    {"type": "output_min_bytes", "path": "result.txt", "min_bytes": 7},
                ],
                command=[sys.executable, "-c", code],
            )
            service = ExecutionService(ExecutionStore(database), InventoryStore(database))
            resolution = service.plan_request(req, snapshot)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = service.execute_plan(resolution.selected.id, snapshot)  # type: ignore[union-attr]
            reopened = ExecutionStore(database)
            summary = reopened.telemetry(result.execution_id)  # type: ignore[union-attr]
            verification = reopened.verification(result.execution_id)  # type: ignore[union-attr]
            stored_request = reopened.request_for_execution(result.execution_id)  # type: ignore[union-attr]
        self.assertEqual(summary.stdout_bytes, len("out-λ\n".encode("utf-8")))  # type: ignore[union-attr]
        self.assertEqual(summary.stderr_bytes, 4)  # type: ignore[union-attr]
        self.assertEqual(summary.known_output_artifact_bytes, 7)  # type: ignore[union-attr]
        self.assertEqual(verification.aggregate_state, "passed")  # type: ignore[union-attr]
        self.assertEqual(stored_request.id, req.id)  # type: ignore[union-attr]

    def test_completed_experiment_can_have_failed_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root)
            InventoryStore(database).save(snapshot)
            req = request(
                root, outputs=["missing.txt"],
                checks=[{"type": "output_exists", "path": "missing.txt"}],
                command=[sys.executable, "-c", "print('completed')"],
            )
            service = ExecutionService(ExecutionStore(database), InventoryStore(database))
            planned = service.plan_request(req, snapshot)
            with contextlib.redirect_stdout(io.StringIO()):
                result = service.execute_plan(planned.selected.id, snapshot)  # type: ignore[union-attr]
            verification = ExecutionStore(database).verification(result.execution_id)  # type: ignore[union-attr]
        self.assertEqual(result.experiment.status, "completed")  # type: ignore[union-attr]
        self.assertEqual(verification.aggregate_state, "failed")  # type: ignore[union-attr]

    def test_failed_experiment_keeps_telemetry_and_telemetry_off_is_absent(self) -> None:
        for mode, expected in (("summary", True), ("off", False)):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database = root / "bourne.sqlite3"
                snapshot = inventory_snapshot(root)
                InventoryStore(database).save(snapshot)
                req = request(
                    root, telemetry=mode,
                    command=[sys.executable, "-c", "import sys; print('bad', file=sys.stderr); raise SystemExit(7)"],
                )
                service = ExecutionService(ExecutionStore(database), InventoryStore(database))
                planned = service.plan_request(req, snapshot)
                with contextlib.redirect_stderr(io.StringIO()):
                    result = service.execute_plan(planned.selected.id, snapshot)  # type: ignore[union-attr]
                summary = ExecutionStore(database).telemetry(result.execution_id)  # type: ignore[union-attr]
                self.assertEqual(result.experiment.status, "failed")  # type: ignore[union-attr]
                self.assertEqual(summary is not None, expected)

    def test_scheduled_v3_worker_outcomes_are_imported_transactionally(self) -> None:
        def runner(argv, **_kwargs):
            return BoundedCommandResult(tuple(argv), 0, "321\n", "")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root, scheduler_families=("slurm",))
            InventoryStore(database).save(snapshot)
            req = request(
                root, backend="slurm", resources={"cpus": 2},
                outputs=["result.txt"],
                checks=[{"type": "output_exists", "path": "result.txt"}],
                command=[sys.executable, "-c", "from pathlib import Path; Path('result.txt').write_text('ok')"],
            )
            store = ExecutionStore(database)
            service = ExecutionService(store, InventoryStore(database), staging_root=root / "stage")
            planned = service.plan_request(req, snapshot)
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                submission = service.execute_plan(planned.selected.id, snapshot, backend=backend)  # type: ignore[union-attr]
            execution = store.get_execution(submission.execution_id)  # type: ignore[union-attr]
            staging = Path(execution.staging_directory)  # type: ignore[arg-type]
            process = subprocess.run(
                [
                    sys.executable, str(staging / "worker.pyz"),
                    str(staging / "plan.json"), str(staging / "result.json"),
                    execution.id,
                ],
                env={**os.environ, "SLURM_CPUS_ON_NODE": "8", "SLURM_JOB_NUM_NODES": "1"},
                capture_output=True, text=True, check=False,
            )
            raw_result = load_worker_result(staging / "result.json", execution.id)
            collected = backend.collect(execution)
            summary = store.telemetry(execution.id)
            verification = store.verification(execution.id)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(raw_result.protocol_version, 3)
        self.assertIsNotNone(raw_result.runtime_evidence)
        self.assertEqual(collected.request_id, req.id)
        self.assertEqual(summary.requested_resources["cpus"], 2)  # type: ignore[union-attr]
        self.assertEqual(summary.allocated_resources["cpus"], 8)  # type: ignore[union-attr]
        self.assertIsNotNone(summary.scheduler_wait_seconds)  # type: ignore[union-attr]
        self.assertIn("bourne_scheduler_lifecycle_events", summary.sources)  # type: ignore[union-attr]
        self.assertEqual(verification.aggregate_state, "passed")  # type: ignore[union-attr]

    def test_pbs_uses_the_same_request_worker_and_outcome_pipeline(self) -> None:
        def runner(argv, **_kwargs):
            return BoundedCommandResult(tuple(argv), 0, "88.server\n", "")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root, scheduler_families=("pbs",))
            InventoryStore(database).save(snapshot)
            req = request(
                root, backend="pbs", resources={"cpus": 2},
                outputs=["pbs.txt"],
                checks=[{"type": "output_exists", "path": "pbs.txt"}],
                command=[sys.executable, "-c", "from pathlib import Path; Path('pbs.txt').write_text('pbs')"],
            )
            store = ExecutionStore(database)
            service = ExecutionService(store, InventoryStore(database), staging_root=root / "stage")
            planned = service.plan_request(req, snapshot)
            backend = PBSBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/qsub"):
                submission = service.execute_plan(planned.selected.id, snapshot, backend=backend)  # type: ignore[union-attr]
            execution = store.get_execution(submission.execution_id)  # type: ignore[union-attr]
            staging = Path(execution.staging_directory)  # type: ignore[arg-type]
            process = subprocess.run(
                [
                    sys.executable, str(staging / "worker.pyz"),
                    str(staging / "plan.json"), str(staging / "result.json"),
                    execution.id,
                ],
                env={**os.environ, "PBS_NP": "8", "PBS_JOBID": "88.server"},
                capture_output=True, text=True, check=False,
            )
            result = backend.collect(execution)
            summary = store.telemetry(execution.id)
            verification = store.verification(execution.id)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(result.protocol_version, 3)
        self.assertIsNotNone(result.runtime_evidence)
        self.assertEqual(result.request_id, req.id)
        self.assertEqual(summary.allocated_resources["cpus"], 8)  # type: ignore[union-attr]
        self.assertEqual(verification.aggregate_state, "passed")  # type: ignore[union-attr]

    def test_controller_rejects_outcomes_that_contradict_captured_evidence(self) -> None:
        def runner(argv, **_kwargs):
            return BoundedCommandResult(tuple(argv), 0, "321\n", "")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = inventory_snapshot(root, scheduler_families=("slurm",))
            InventoryStore(database).save(snapshot)
            req = request(
                root, backend="slurm", outputs=["missing.txt"],
                checks=[{"type": "output_exists", "path": "missing.txt"}],
                command=[sys.executable, "-c", "print('completed')"],
            )
            store = ExecutionStore(database)
            service = ExecutionService(store, InventoryStore(database), staging_root=root / "stage")
            planned = service.plan_request(req, snapshot)
            backend = SlurmBackend(store, root / "stage", runner=runner)
            with patch("bourneprov.backends.shutil.which", return_value="/usr/bin/sbatch"):
                submission = service.execute_plan(planned.selected.id, snapshot, backend=backend)  # type: ignore[union-attr]
            execution = store.get_execution(submission.execution_id)  # type: ignore[union-attr]
            staging = Path(execution.staging_directory)  # type: ignore[arg-type]
            process = subprocess.run(
                [
                    sys.executable, str(staging / "worker.pyz"),
                    str(staging / "plan.json"), str(staging / "result.json"),
                    execution.id,
                ],
                capture_output=True, text=True, check=False,
            )
            result_path = staging / "result.json"
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            payload["verification"]["aggregate_state"] = "passed"
            result_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(BackendError, "verification does not match"):
                backend.collect(execution)
            self.assertIsNone(store.experiment_id(execution.id))
            self.assertIsNone(store.telemetry(execution.id))
            self.assertIsNone(store.verification(execution.id))
        self.assertEqual(process.returncode, 0)


if __name__ == "__main__":
    unittest.main()
