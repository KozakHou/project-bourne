from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.cli import main
from bourneprov.storage import ExperimentStore
from tests.fixtures import experiment, system_provenance


@contextlib.contextmanager
def working_directory(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


class CliTests(unittest.TestCase):
    def test_run_records_success_and_forwards_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                working_directory(root),
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.lifecycle.collect_system", return_value=system_provenance()),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                exit_code = main(
                    ["run", sys.executable, "-c", "print('hello from experiment')"]
                )

            records = ExperimentStore(database).list_recent()

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue(), "hello from experiment\n")
        self.assertIn("Bourne recorded", stderr.getvalue())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].stdout, "hello from experiment\n")
        self.assertEqual(records[0].arguments[0], "-c")

    def test_run_records_failure_before_returning_child_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            with (
                working_directory(root),
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.lifecycle.collect_system", return_value=system_provenance()),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                exit_code = main(
                    [
                        "run",
                        sys.executable,
                        "-c",
                        "import sys; print('bad', file=sys.stderr); raise SystemExit(9)",
                    ]
                )

            record = ExperimentStore(database).list_recent()[0]

        self.assertEqual(exit_code, 9)
        self.assertEqual(record.status, "failed")
        self.assertEqual(record.exit_code, 9)
        self.assertEqual(record.stderr, "bad\n")

    def test_list_displays_required_summary_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            record = experiment()
            ExperimentStore(database).save(record)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["list"])

        rendered = output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn(record.id[:10], rendered)
        self.assertNotIn(record.id, rendered)
        self.assertIn(record.status, rendered)
        self.assertIn(record.started_at, rendered)
        self.assertIn("1.000 s", rendered)
        self.assertIn("solver case.yaml --steps 4", rendered)

    def test_list_full_id_displays_canonical_ulid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            record = experiment()
            ExperimentStore(database).save(record)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["list", "--full-id"])

        self.assertEqual(exit_code, 0)
        self.assertIn(record.id, output.getvalue())

    def test_show_displays_full_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            record = experiment()
            ExperimentStore(database).save(record)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["show", record.id])

        rendered = output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn(f"Experiment: {record.id}", rendered)
        self.assertIn("Git provenance:", rendered)
        self.assertIn("System provenance:", rendered)
        self.assertIn("answer=42", rendered)

    def test_compare_clearly_displays_differing_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            first = experiment()
            second = experiment(
                id="01H00000000000000000000001",
                status="failed",
                exit_code=4,
                arguments=["case.yaml", "--steps", "8"],
            )
            store = ExperimentStore(database)
            store.save(first)
            store.save(second)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["compare", first.id, second.id])

        rendered = output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("Differing fields:", rendered)
        self.assertIn("arguments", rendered)
        self.assertIn("exit_code", rendered)
        self.assertIn('A: "completed"', rendered)
        self.assertIn('B: "failed"', rendered)

    def test_compare_accepts_relative_references(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            older = experiment(stdout="older\n")
            newer = experiment(
                id="01H00000000000000000000001",
                stdout="newer\n",
                started_at="2026-01-02T00:00:00.000000Z",
                ended_at="2026-01-02T00:00:01.000000Z",
            )
            store = ExperimentStore(database)
            store.save(older)
            store.save(newer)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["compare", "@2", "@1"])

        self.assertEqual(exit_code, 0)
        self.assertIn(f"Comparing {older.id} -> {newer.id}", output.getvalue())

    def test_show_relative_reference_uses_configured_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first_database = Path(directory) / "first.sqlite3"
            second_database = Path(directory) / "second.sqlite3"
            first = experiment(id="01HFIRST" + "0" * 18, stdout="first database\n")
            second = experiment(id="01HSECOND" + "0" * 17, stdout="second database\n")
            ExperimentStore(first_database).save(first)
            ExperimentStore(second_database).save(second)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(second_database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["show", "@1"])

        self.assertEqual(exit_code, 0)
        self.assertIn(second.id, output.getvalue())
        self.assertNotIn(first.id, output.getvalue())

    def test_ambiguous_reference_returns_candidates_without_guessing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            first = experiment(id="01HAAA" + "0" * 20)
            second = experiment(id="01HAAA" + "1" * 20)
            store = ExperimentStore(database)
            store.save(first)
            store.save(second)
            error = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stderr(error),
            ):
                exit_code = main(["show", "01HAAA"])

        self.assertEqual(exit_code, 2)
        self.assertIn("Ambiguous experiment reference", error.getvalue())
        self.assertIn(first.id, error.getvalue())
        self.assertIn(second.id, error.getvalue())

    def test_persistence_across_independent_cli_invocations_without_optional_tools(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            environment = os.environ.copy()
            environment.update(
                {
                    "BOURNE_DB": str(database),
                    "PYTHONPATH": str(project_root / "src"),
                    "PATH": "",
                }
            )
            run = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "bourneprov",
                    "run",
                    sys.executable,
                    "-c",
                    "print('persisted independently')",
                ],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            listed = subprocess.run(
                [sys.executable, "-m", "bourneprov", "list"],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("persisted independently", run.stdout)
        self.assertEqual(listed.returncode, 0, listed.stderr)
        self.assertIn("completed", listed.stdout)
        self.assertIn(sys.executable, listed.stdout)


if __name__ == "__main__":
    unittest.main()
