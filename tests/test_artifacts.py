from __future__ import annotations

import hashlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.artifacts import HASH_CHUNK_SIZE, capture_artifact, sha256_file
from bourneprov.lifecycle import run_and_record
from bourneprov.storage import ExperimentStore
from tests.fixtures import system_provenance


class ArtifactCaptureTests(unittest.TestCase):
    def test_sha256_is_correct(self) -> None:
        payload = b"scientific input\x00with bytes\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.dat"
            path.write_bytes(payload)
            digest = sha256_file(path)

        self.assertEqual(digest, hashlib.sha256(payload).hexdigest())

    def test_large_file_hashing_uses_bounded_stream_reads(self) -> None:
        payload = b"x" * (HASH_CHUNK_SIZE * 2 + 17)

        class GuardedStream(io.BytesIO):
            def read(self, size: int = -1) -> bytes:
                if size < 1 or size > HASH_CHUNK_SIZE:
                    raise AssertionError(f"unbounded artifact read: {size}")
                return super().read(size)

        stream = GuardedStream(payload)
        with patch.object(Path, "open", return_value=stream):
            digest = sha256_file(Path("large.bin"))

        self.assertEqual(digest, hashlib.sha256(payload).hexdigest())

    def test_missing_input_is_recorded_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = capture_artifact(
                "01H00000000000000000000000", "input", "missing input.dat", root
            )

        self.assertFalse(artifact.exists)
        self.assertIsNone(artifact.sha256)
        self.assertIsNone(artifact.size_bytes)
        self.assertTrue(artifact.resolved_path.endswith("missing input.dat"))

    def test_input_is_captured_before_execution_and_outputs_after(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            first_input = root / "input one.txt"
            second_input = root / "測試.dat"
            first_input.write_text("before\n", encoding="utf-8")
            second_input.write_text("second\n", encoding="utf-8")
            expected_input_hash = hashlib.sha256(b"before\n").hexdigest()
            code = (
                "from pathlib import Path; "
                "Path('input one.txt').write_text('after\\n'); "
                "Path('result one.csv').write_text('value\\n42\\n'); "
                "Path('figure output.txt').write_text('plot\\n')"
            )
            with patch(
                "bourneprov.lifecycle.collect_system", return_value=system_provenance()
            ):
                experiment = run_and_record(
                    [sys.executable, "-c", code],
                    ExperimentStore(database),
                    cwd=root,
                    input_paths=["input one.txt", "測試.dat"],
                    output_paths=["result one.csv", "figure output.txt"],
                )
            artifacts = ExperimentStore(database).list_artifacts(experiment.id)

        inputs = [item for item in artifacts if item.role == "input"]
        outputs = [item for item in artifacts if item.role == "output"]
        self.assertEqual(len(inputs), 2)
        self.assertEqual(len(outputs), 2)
        self.assertEqual(inputs[0].sha256, expected_input_hash)
        self.assertTrue(all(item.exists and item.sha256 for item in outputs))

    def test_same_path_with_changed_content_creates_distinct_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "result.csv"
            output.write_text("version one\n", encoding="utf-8")
            first = capture_artifact("01H" + "1" * 23, "output", "result.csv", root)
            output.write_text("version two\n", encoding="utf-8")
            second = capture_artifact("01H" + "2" * 23, "output", "result.csv", root)

        self.assertEqual(first.resolved_path, second.resolved_path)
        self.assertNotEqual(first.id, second.id)
        self.assertNotEqual(first.sha256, second.sha256)

    def test_same_content_at_different_paths_keeps_records_and_content_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.dat").write_bytes(b"same")
            (root / "b.dat").write_bytes(b"same")
            first = capture_artifact("01H" + "1" * 23, "input", "a.dat", root)
            second = capture_artifact("01H" + "1" * 23, "input", "b.dat", root)

        self.assertNotEqual(first.id, second.id)
        self.assertNotEqual(first.resolved_path, second.resolved_path)
        self.assertEqual(first.sha256, second.sha256)

    def test_failed_experiment_retains_input_and_missing_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            (root / "config.json").write_text("{}\n", encoding="utf-8")
            with patch(
                "bourneprov.lifecycle.collect_system", return_value=system_provenance()
            ):
                experiment = run_and_record(
                    [sys.executable, "-c", "raise SystemExit(7)"],
                    ExperimentStore(database),
                    cwd=root,
                    input_paths=["config.json"],
                    output_paths=["never-created.csv"],
                )
            artifacts = ExperimentStore(database).list_artifacts(experiment.id)

        self.assertEqual(experiment.status, "failed")
        self.assertEqual(experiment.exit_code, 7)
        self.assertTrue(artifacts[0].exists)
        self.assertFalse(artifacts[1].exists)
        self.assertEqual(artifacts[1].role, "output")


if __name__ == "__main__":
    unittest.main()
