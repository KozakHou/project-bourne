from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.backends import Submission
from bourneprov.cli import main
from bourneprov.inventory_storage import InventoryStore
from bourneprov.workload_storage import ExecutionStore
from bourneprov.storage import ExperimentStore
from tests.v04_fixtures import inventory_snapshot


class ExecutionCliTests(unittest.TestCase):
    def test_plan_requires_persisted_inventory_and_never_discovers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            error = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.cli.discover_site") as discover,
                contextlib.redirect_stderr(error),
            ):
                code = main(["plan", "--", sys.executable, "-c", "print('x')"])
        self.assertEqual(code, 2)
        self.assertIn("bourne discover", error.getvalue())
        discover.assert_not_called()

    def test_plan_is_nonexecuting_persists_immutable_plan_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            output = io.StringIO()
            marker = root / "must-not-exist"
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                code = main(
                    [
                        "plan", "--backend", "direct", "--json", "--",
                        sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()",
                    ]
                )
            payload = json.loads(output.getvalue())
            marker_exists = marker.exists()
            plan_count = ExecutionStore(database).count_plans()

        self.assertEqual(code, 0)
        self.assertFalse(marker_exists)
        self.assertIsNotNone(payload["selected"])
        self.assertEqual(payload["selected"]["backend"], "direct")
        self.assertEqual(plan_count, 1)

    def test_unknown_executable_plan_is_useful_without_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                code = main(["plan", "--json", "--", "./unknown-solver", "case.dat"])
            payload = json.loads(output.getvalue())
        self.assertEqual(code, 2)
        self.assertIsNone(payload["selected"])
        self.assertEqual(payload["candidates"][0]["compatibility_state"], "incompatible")
        self.assertIn("executable", str(payload["candidates"][0]))

    def test_execute_direct_streams_and_links_actual_experiment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            output = io.StringIO()
            error = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
                contextlib.redirect_stderr(error),
            ):
                code = main(
                    ["execute", "--backend", "direct", "--", sys.executable, "-c", "print('direct-live')"]
                )
            store = ExecutionStore(database)
            execution = store.list_executions()[0]
            view = store.view(execution.id)

        self.assertEqual(code, 0)
        self.assertIn("direct-live", output.getvalue())
        self.assertEqual(view.execution.state, "completed")
        self.assertIsNotNone(view.experiment_id)
        self.assertEqual(view.plan.requested_resources, view.workload.resources)

    def test_execute_existing_plan_and_lifecycle_list_show_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            environment = {"BOURNE_DB": str(database)}
            planned = io.StringIO()
            with patch.dict(os.environ, environment), contextlib.redirect_stdout(planned):
                self.assertEqual(
                    main(["plan", "--backend", "direct", "--json", "--", sys.executable, "-c", "print('planned')"]),
                    0,
                )
            plan_id = json.loads(planned.getvalue())["selected"]["id"]
            with patch.dict(os.environ, environment), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(["execute", "--plan", plan_id]), 0)
            listed = io.StringIO()
            shown = io.StringIO()
            with patch.dict(os.environ, environment):
                with contextlib.redirect_stdout(listed):
                    self.assertEqual(main(["execution", "list", "--json"]), 0)
                with contextlib.redirect_stdout(shown):
                    self.assertEqual(main(["execution", "show", "@1", "--json"]), 0)
            list_payload = json.loads(listed.getvalue())
            show_payload = json.loads(shown.getvalue())

        self.assertEqual(len(list_payload), 1)
        self.assertEqual(show_payload["execution"]["state"], "completed")
        self.assertIsNotNone(show_payload["experiment_id"])

    def test_scheduled_execute_reports_submission_without_claiming_experiment_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(
                inventory_snapshot(root, scheduler_families=("slurm",))
            )
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch(
                    "bourneprov.backends.SlurmBackend.execute",
                    side_effect=lambda execution, *_args: Submission("slurm", "123", execution.id),
                ),
                contextlib.redirect_stdout(output),
            ):
                code = main(
                    ["execute", "--backend", "slurm", "--json", "--", sys.executable, "-c", "print('not run')"]
                )
            payload = json.loads(output.getvalue())

        self.assertEqual(code, 0)
        self.assertEqual(payload["state"], "submitted")
        self.assertEqual(payload["job_id"], "123")
        self.assertNotIn("experiment_id", payload)

    def test_direct_execution_preserves_inputs_outputs_and_lineage(self) -> None:
        from tests.fixtures import experiment

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            parent = experiment(working_directory=str(root))
            ExperimentStore(database).save(parent)
            InventoryStore(database).save(inventory_snapshot(root))
            (root / "input.txt").write_text("input", encoding="utf-8")
            code_text = "from pathlib import Path; Path('output.txt').write_text('output')"
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.cli.Path.cwd", return_value=root),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                code = main(
                    [
                        "execute", "--backend", "direct",
                        "--input", "input.txt", "--output", "output.txt",
                        "--derived-from", parent.id, "--",
                        sys.executable, "-c", code_text,
                    ]
                )
            execution = ExecutionStore(database).list_executions()[0]
            experiment_id = ExecutionStore(database).experiment_id(execution.id)
            artifacts = ExperimentStore(database).list_artifacts(experiment_id)  # type: ignore[arg-type]
            lineage = ExperimentStore(database).get_lineage(experiment_id)  # type: ignore[arg-type]

        self.assertEqual(code, 0)
        self.assertEqual([item.role for item in artifacts], ["input", "output"])
        self.assertEqual(lineage.parent_experiment_id, parent.id)  # type: ignore[union-attr]

    def test_unknown_executable_runs_only_when_explicitly_executed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            executable = root / "bourne_unknown_solver"
            marker = root / "ran.txt"
            executable.write_text(
                f"#!{sys.executable}\nfrom pathlib import Path\nPath({str(marker)!r}).write_text('ran')\nprint('unknown-ok')\n",
                encoding="utf-8",
            )
            executable.chmod(0o755)
            InventoryStore(database).save(inventory_snapshot(root))
            environment = {"BOURNE_DB": str(database)}
            with (
                patch.dict(os.environ, environment),
                patch("bourneprov.cli.Path.cwd", return_value=root),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main(["plan", "--backend", "direct", "--", "./bourne_unknown_solver"]), 0)
            self.assertFalse(marker.exists())
            with (
                patch.dict(os.environ, environment),
                patch("bourneprov.cli.Path.cwd", return_value=root),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                code = main(["execute", "--backend", "direct", "--", "./bourne_unknown_solver"])
            ran = marker.read_text(encoding="utf-8")

        self.assertEqual(code, 0)
        self.assertEqual(ran, "ran")

    def test_direct_preflight_detects_executable_that_disappeared_after_planning(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            executable = root / "temporary-solver"
            executable.write_text(f"#!{sys.executable}\nprint('must-not-run')\n", encoding="utf-8")
            executable.chmod(0o755)
            InventoryStore(database).save(inventory_snapshot(root))
            environment = {"BOURNE_DB": str(database)}
            planned = io.StringIO()
            with (
                patch.dict(os.environ, environment),
                patch("bourneprov.cli.Path.cwd", return_value=root),
                contextlib.redirect_stdout(planned),
            ):
                self.assertEqual(
                    main(["plan", "--backend", "direct", "--json", "--", "./temporary-solver"]),
                    0,
                )
            plan_id = json.loads(planned.getvalue())["selected"]["id"]
            executable.unlink()
            output = io.StringIO()
            with patch.dict(os.environ, environment), contextlib.redirect_stdout(output):
                code = main(["execute", "--plan", plan_id, "--json"])
            payload = json.loads(output.getvalue())
            execution = ExecutionStore(database).list_executions()[0]

        self.assertEqual(code, 2)
        self.assertEqual(payload["state"], "preflight_failed")
        self.assertIsNone(payload["experiment_id"])
        self.assertEqual(execution.state, "preflight_failed")

    def test_direct_scientific_failure_preserves_exit_code_and_experiment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                code = main(
                    ["execute", "--backend", "direct", "--", sys.executable, "-c", "raise SystemExit(7)"]
                )
            execution = ExecutionStore(database).list_executions()[0]
            view = ExecutionStore(database).view(execution.id)
            experiment_record = ExperimentStore(database).get(view.experiment_id)  # type: ignore[arg-type]

        self.assertEqual(code, 7)
        self.assertEqual(view.execution.state, "failed")
        self.assertEqual(experiment_record.exit_code, 7)


if __name__ == "__main__":
    unittest.main()
