from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from bourneprov.resolver import resolve_execution
from bourneprov.worker_bundle import build_worker_zipapp, write_staged_plan
from bourneprov.worker_result import WorkerResultError, load_worker_result, parse_worker_result
from bourneprov.workload import inspect_workload
from bourneprov.workload_models import ExecutionConstraints, ResourceRequirements
from tests.v04_fixtures import inventory_snapshot


class WorkerTests(unittest.TestCase):
    def _run_worker(
        self,
        root: Path,
        argv: list[str],
        *,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        resources: ResourceRequirements | None = None,
        environment: dict[str, str] | None = None,
    ):
        snapshot = inventory_snapshot(root, scheduler_families=("slurm",))
        workload = inspect_workload(
            argv, cwd=root, inputs=inputs or [], outputs=outputs or [],
            resources=resources,
            constraints=ExecutionConstraints(backend="slurm", target="slurm-target"),
        )
        plan = resolve_execution(workload, snapshot).selected
        self.assertIsNotNone(plan)
        execution_id = "01HWORKER" + "0" * 17
        worker = build_worker_zipapp(root / "worker.pyz")
        staged = write_staged_plan(
            root / "plan.json", execution_id, plan, workload  # type: ignore[arg-type]
        )
        result_path = root / "result.json"
        result = subprocess.run(
            [sys.executable, str(worker), str(staged), str(result_path), execution_id],
            capture_output=True, text=True, check=False,
            env=None if environment is None else {**os.environ, **environment},
        )
        return result, load_worker_result(result_path, execution_id)

    def test_self_contained_worker_success_captures_actual_experiment_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "input.txt"
            input_path.write_text("input", encoding="utf-8")
            code = "from pathlib import Path; print('worker-ok'); Path('output.txt').write_text('out')"
            process, result = self._run_worker(
                root, [sys.executable, "-c", code],
                inputs=["input.txt"], outputs=["output.txt"],
            )

        self.assertEqual(process.returncode, 0)
        self.assertIn("worker-ok", process.stdout)
        self.assertEqual(result.state, "completed")
        self.assertIsNotNone(result.experiment)
        self.assertEqual(result.experiment.stdout, "worker-ok\n")  # type: ignore[union-attr]
        self.assertEqual([item.role for item in result.artifacts], ["input", "output"])
        self.assertEqual(result.allocation.hosts[0], result.experiment.system.hostname)  # type: ignore[union-attr]

    def test_worker_failure_preserves_scientific_exit_and_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            process, result = self._run_worker(
                Path(directory),
                [sys.executable, "-c", "import sys; print('boom', file=sys.stderr); raise SystemExit(9)"],
            )
        self.assertEqual(process.returncode, 9)
        self.assertEqual(result.state, "failed")
        self.assertEqual(result.experiment.exit_code, 9)  # type: ignore[union-attr]
        self.assertIn("boom", result.experiment.stderr)  # type: ignore[union-attr]

    def test_preflight_missing_executable_does_not_launch_science(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "should-not-exist"
            process, result = self._run_worker(
                Path(directory), ["./missing-solver", str(marker)]
            )
        self.assertEqual(process.returncode, 75)
        self.assertEqual(result.state, "preflight_failed")
        self.assertIsNone(result.experiment)
        self.assertIn("executable", result.error)
        self.assertFalse(marker.exists())

    def test_preflight_rejects_observed_allocation_resource_shortfall(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _process, result = self._run_worker(
                Path(directory), [sys.executable, "-c", "print('must-not-run')"],
                resources=ResourceRequirements(gpus=4),
                environment={"CUDA_VISIBLE_DEVICES": "0"},
            )
        self.assertEqual(result.state, "preflight_failed")
        self.assertIn("allocated gpus", result.error)
        self.assertIsNone(result.experiment)

    def test_scientific_argv_with_shell_metacharacters_remains_exact(self) -> None:
        dangerous = ["a b", "quote'\"", "λ", ";touch injected", "$HOME", "`id`", "$(id)"]
        with tempfile.TemporaryDirectory() as directory:
            code = "import json,sys; print(json.dumps(sys.argv[1:], ensure_ascii=False))"
            _process, result = self._run_worker(
                Path(directory), [sys.executable, "-c", code, *dangerous]
            )
        self.assertEqual(json.loads(result.experiment.stdout), dangerous)  # type: ignore[union-attr]

    def test_wrong_id_malformed_and_oversized_results_are_rejected(self) -> None:
        with self.assertRaisesRegex(WorkerResultError, "execution ID"):
            parse_worker_result(
                {
                    "schema_version": 1, "execution_id": "other", "state": "unknown",
                    "created_at": "now", "experiment": None, "artifacts": [],
                    "lineage": [], "allocation": None, "preflight": {}, "error": None,
                },
                "expected",
            )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            malformed = root / "result.json"
            malformed.write_text("not json", encoding="utf-8")
            with self.assertRaisesRegex(WorkerResultError, "valid JSON"):
                load_worker_result(malformed, "expected")
            malformed.write_bytes(b"x" * (32 * 1024 * 1024 + 1))
            with self.assertRaisesRegex(WorkerResultError, "size limit"):
                load_worker_result(malformed, "expected")

    def test_json_result_data_cannot_execute_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "executed"
            payload = {
                "schema_version": 1, "execution_id": "expected", "state": "unknown",
                "created_at": "now", "experiment": None, "artifacts": [],
                "lineage": [], "allocation": None, "preflight": {},
                "error": f"__import__('pathlib').Path({str(marker)!r}).touch()",
            }
            parsed = parse_worker_result(payload, "expected")
        self.assertIn("__import__", parsed.error)
        self.assertFalse(marker.exists())

    def test_staged_plan_is_structurally_validated_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "must-not-run"
            snapshot = inventory_snapshot(root, scheduler_families=("slurm",))
            workload = inspect_workload(
                [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
                cwd=root,
                constraints=ExecutionConstraints(backend="slurm"),
            )
            plan = resolve_execution(workload, snapshot).selected
            execution_id = "01HVALID" + "0" * 18
            worker = build_worker_zipapp(root / "worker.pyz")
            staged = write_staged_plan(root / "plan.json", execution_id, plan, workload)  # type: ignore[arg-type]
            payload = json.loads(staged.read_text(encoding="utf-8"))
            payload["plan"]["arguments"] = "not-an-argv-list"
            staged.write_text(json.dumps(payload), encoding="utf-8")
            process = subprocess.run(
                [sys.executable, str(worker), str(staged), str(root / "result.json"), execution_id],
                capture_output=True, text=True, check=False,
            )
            marker_exists = marker.exists()

        self.assertEqual(process.returncode, 70)
        self.assertIn("arguments are invalid", process.stderr)
        self.assertFalse(marker_exists)


if __name__ == "__main__":
    unittest.main()
