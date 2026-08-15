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

from bourneprov.artifacts import capture_artifact
from bourneprov.cli import main
from bourneprov.models import ExperimentLineage
from bourneprov.storage import ExperimentStore
from bourneprov.tracing import (
    AmbiguousArtifactReference,
    MissingArtifactReference,
    trace_artifact,
)
from tests.fixtures import experiment, system_provenance


class LineageTests(unittest.TestCase):
    def _run_derived(self, reference: str, seed_second: bool = False) -> tuple[str, str]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        database = root / "bourne.sqlite3"
        parent = experiment(id="01HPARENT" + "0" * 17)
        store = ExperimentStore(database)
        store.save(parent)
        if seed_second:
            store.save(
                experiment(
                    id="01HNEWER" + "1" * 18,
                    started_at="2026-01-02T00:00:00.000000Z",
                    ended_at="2026-01-02T00:00:01.000000Z",
                )
            )
        with (
            patch.dict(os.environ, {"BOURNE_DB": str(database)}),
            patch("bourneprov.lifecycle.collect_system", return_value=system_provenance()),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            exit_code = main(
                [
                    "run",
                    "--derived-from",
                    reference,
                    "--",
                    sys.executable,
                    "-c",
                    "pass",
                ]
            )
        self.assertEqual(exit_code, 0)
        child = ExperimentStore(database).get_by_recency(1)
        lineage = ExperimentStore(database).get_lineage(child.id)
        self.assertIsNotNone(lineage)
        return parent.id, lineage.parent_experiment_id  # type: ignore[union-attr]

    def test_parent_can_use_full_ulid(self) -> None:
        parent, recorded = self._run_derived("01HPARENT" + "0" * 17)
        self.assertEqual(recorded, parent)

    def test_parent_can_use_unique_case_insensitive_prefix(self) -> None:
        parent, recorded = self._run_derived("01hparent")
        self.assertEqual(recorded, parent)

    def test_parent_can_use_latest(self) -> None:
        parent, recorded = self._run_derived("latest")
        self.assertEqual(recorded, parent)

    def test_parent_can_use_at_one(self) -> None:
        parent, recorded = self._run_derived("@1")
        self.assertEqual(recorded, parent)

    def test_parent_can_use_at_two(self) -> None:
        parent, recorded = self._run_derived("@2", seed_second=True)
        self.assertEqual(recorded, parent)

    def test_missing_parent_prevents_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            error = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stderr(error),
            ):
                exit_code = main(
                    [
                        "run",
                        "--derived-from",
                        "missing-parent",
                        "--",
                        sys.executable,
                        "-c",
                        "raise SystemExit('must not execute')",
                    ]
                )
            count = ExperimentStore(database).count()

        self.assertEqual(exit_code, 2)
        self.assertEqual(count, 0)
        self.assertIn("No experiment matches", error.getvalue())

    def test_show_displays_immediate_ancestry_and_old_records_have_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            parent = experiment(id="01HPARENT" + "0" * 17)
            child = experiment(
                id="01HCHILD" + "1" * 18,
                started_at="2026-01-02T00:00:00.000000Z",
                ended_at="2026-01-02T00:00:01.000000Z",
            )
            store = ExperimentStore(database)
            store.save(parent)
            store.save_record(
                child,
                lineage=[
                    ExperimentLineage(
                        child_experiment_id=child.id,
                        parent_experiment_id=parent.id,
                        relationship="derived_from",
                        created_at=child.ended_at,
                    )
                ],
            )
            child_output = io.StringIO()
            parent_output = io.StringIO()
            with patch.dict(os.environ, {"BOURNE_DB": str(database)}):
                with contextlib.redirect_stdout(child_output):
                    self.assertEqual(main(["show", child.id]), 0)
                with contextlib.redirect_stdout(parent_output):
                    self.assertEqual(main(["show", parent.id]), 0)

        self.assertIn("Derived from:", child_output.getvalue())
        self.assertIn(parent.id, child_output.getvalue())
        self.assertIn("No parent experiment.", parent_output.getvalue())


class TraceTests(unittest.TestCase):
    def _two_generation_fixture(
        self, root: Path
    ) -> tuple[ExperimentStore, str, str, Path]:
        database = root / "bourne.sqlite3"
        store = ExperimentStore(database)
        parent = experiment(id="01HPARENT" + "0" * 17)
        result_a = root / "result_A.csv"
        result_a.write_text("x,value\n1,2\n", encoding="utf-8")
        parent_output = capture_artifact(parent.id, "output", "result_A.csv", root)
        store.save_record(parent, [parent_output])

        child = experiment(
            id="01HCHILD" + "1" * 18,
            command="postprocess",
            arguments=["result_A.csv", "result_B.csv"],
            started_at="2026-01-02T00:00:00.000000Z",
            ended_at="2026-01-02T00:00:01.000000Z",
        )
        child_input = capture_artifact(child.id, "input", "result_A.csv", root)
        result_b = root / "result_B.csv"
        result_b.write_text("x,value\n1,4\n", encoding="utf-8")
        child_output = capture_artifact(child.id, "output", "result_B.csv", root)
        store.save_record(
            child,
            [child_input, child_output],
            [
                ExperimentLineage(
                    child_experiment_id=child.id,
                    parent_experiment_id=parent.id,
                    relationship="derived_from",
                    created_at=child.ended_at,
                )
            ],
        )
        return store, parent.id, child.id, result_b

    def test_trace_known_output_across_two_generations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, parent_id, child_id, _ = self._two_generation_fixture(root)
            traced = trace_artifact(store, "result_B.csv", cwd=root)

        self.assertEqual(traced.producer.id, child_id)
        self.assertEqual(traced.inputs[0].original_path, "result_A.csv")
        self.assertEqual([item.id for item in traced.ancestry], [parent_id])

    def test_current_content_distinguishes_versions_at_same_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ExperimentStore(root / "bourne.sqlite3")
            output = root / "result.csv"
            first = experiment(id="01HFIRST" + "0" * 18)
            output.write_text("first\n", encoding="utf-8")
            store.save_record(first, [capture_artifact(first.id, "output", "result.csv", root)])
            second = experiment(
                id="01HSECOND" + "1" * 17,
                started_at="2026-01-02T00:00:00.000000Z",
                ended_at="2026-01-02T00:00:01.000000Z",
            )
            output.write_text("second\n", encoding="utf-8")
            store.save_record(
                second, [capture_artifact(second.id, "output", "result.csv", root)]
            )
            traced = trace_artifact(store, "result.csv", cwd=root)

        self.assertEqual(traced.producer.id, second.id)

    def test_moved_file_can_be_traced_by_content_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, _, child_id, original = self._two_generation_fixture(root)
            moved = root / "renamed result.csv"
            original.rename(moved)
            traced = trace_artifact(store, moved.name, cwd=root)

        self.assertEqual(traced.producer.id, child_id)

    def test_missing_artifact_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ExperimentStore(root / "bourne.sqlite3")
            with self.assertRaises(MissingArtifactReference):
                trace_artifact(store, "unknown-output.dat", cwd=root)

    def test_ambiguous_historical_path_does_not_guess(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ExperimentStore(root / "bourne.sqlite3")
            output = root / "result.csv"
            for index, content in enumerate(("first\n", "second\n")):
                record = experiment(
                    id=f"01HAMB{index}" + str(index) * 19,
                    started_at=f"2026-01-0{index + 1}T00:00:00.000000Z",
                    ended_at=f"2026-01-0{index + 1}T00:00:01.000000Z",
                )
                output.write_text(content, encoding="utf-8")
                store.save_record(
                    record, [capture_artifact(record.id, "output", "result.csv", root)]
                )
            output.unlink()

            with self.assertRaises(AmbiguousArtifactReference) as caught:
                trace_artifact(store, "result.csv", cwd=root)

        self.assertEqual(len(caught.exception.matches), 2)

    def test_trace_survives_independent_cli_process(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, parent_id, child_id, _ = self._two_generation_fixture(root)
            environment = os.environ.copy()
            environment.update(
                {
                    "BOURNE_DB": str(store.path),
                    "PYTHONPATH": str(project_root / "src"),
                }
            )
            traced = subprocess.run(
                [sys.executable, "-m", "bourneprov", "trace", "result_B.csv"],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(traced.returncode, 0, traced.stderr)
        self.assertIn(child_id, traced.stdout)
        self.assertIn(parent_id, traced.stdout)
        self.assertIn("result_A.csv", traced.stdout)


if __name__ == "__main__":
    unittest.main()
