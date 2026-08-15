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
from bourneprov.models import Artifact, ArtifactCaptureStatus, ArtifactExistenceState
from bourneprov.presentation import format_show
from bourneprov.storage import ExperimentStore
from tests.fixtures import experiment, system_provenance


class ArtifactCaptureTests(unittest.TestCase):
    @staticmethod
    def _artifact(
        *,
        suffix: str,
        existence_state: ArtifactExistenceState,
        capture_status: ArtifactCaptureStatus,
    ) -> Artifact:
        present = existence_state == "present"
        complete_digest = present and capture_status == "complete"
        return Artifact(
            id=f"01HARTIFACT{suffix}".ljust(26, "0"),
            experiment_id="01H00000000000000000000000",
            role="output",
            original_path=f"state-{suffix}.dat",
            resolved_path=f"/work/state-{suffix}.dat",
            existence_state=existence_state,
            capture_status=capture_status,
            sha256="a" * 64 if complete_digest else None,
            size_bytes=1 if present else None,
            modified_at="2026-01-01T00:00:00.000000Z" if present else None,
            captured_at="2026-01-01T00:00:01.000000Z",
            capture_error=(
                None
                if complete_digest or existence_state == "missing"
                else f"{capture_status} test state"
            ),
        )

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

        self.assertEqual(artifact.existence_state, "missing")
        self.assertEqual(artifact.capture_status, "complete")
        self.assertIsNone(artifact.sha256)
        self.assertIsNone(artifact.size_bytes)
        self.assertTrue(artifact.resolved_path.endswith("missing input.dat"))

    def test_regular_file_is_present_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "readable.dat").write_bytes(b"readable")
            artifact = capture_artifact(
                "01H00000000000000000000000", "input", "readable.dat", root
            )

        self.assertEqual(artifact.existence_state, "present")
        self.assertEqual(artifact.capture_status, "complete")
        self.assertEqual(artifact.sha256, hashlib.sha256(b"readable").hexdigest())

    def test_inspection_error_is_unknown_and_unreadable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(Path, "stat", side_effect=PermissionError("denied")):
                artifact = capture_artifact(
                    "01H00000000000000000000000", "input", "blocked.dat", root
                )

        self.assertEqual(artifact.existence_state, "unknown")
        self.assertEqual(artifact.capture_status, "unreadable")
        self.assertIsNone(artifact.sha256)
        self.assertIn("could not inspect artifact", artifact.capture_error or "")

    def test_non_regular_object_is_present_and_unsupported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "declared-directory").mkdir()
            artifact = capture_artifact(
                "01H00000000000000000000000",
                "output",
                "declared-directory",
                root,
            )

        self.assertEqual(artifact.existence_state, "present")
        self.assertEqual(artifact.capture_status, "unsupported")
        self.assertIsNone(artifact.sha256)

    def test_fingerprint_read_error_is_present_and_unreadable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "blocked.dat").write_bytes(b"present")
            with patch(
                "bourneprov.artifacts.sha256_file",
                side_effect=PermissionError("denied"),
            ):
                artifact = capture_artifact(
                    "01H00000000000000000000000", "input", "blocked.dat", root
                )

        self.assertEqual(artifact.existence_state, "present")
        self.assertEqual(artifact.capture_status, "unreadable")
        self.assertIsNone(artifact.sha256)
        self.assertEqual(artifact.size_bytes, len(b"present"))

    def test_change_during_hashing_is_present_and_changed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "changing.dat"
            path.write_bytes(b"before")

            def change_while_hashing(captured_path: Path) -> str:
                digest = hashlib.sha256(captured_path.read_bytes()).hexdigest()
                captured_path.write_bytes(b"after with a different size")
                return digest

            with patch(
                "bourneprov.artifacts.sha256_file", side_effect=change_while_hashing
            ):
                artifact = capture_artifact(
                    "01H00000000000000000000000", "output", "changing.dat", root
                )

        self.assertEqual(artifact.existence_state, "present")
        self.assertEqual(artifact.capture_status, "changed")
        self.assertIsNone(artifact.sha256)
        self.assertIn("changed while", artifact.capture_error or "")

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
        self.assertTrue(
            all(
                item.existence_state == "present"
                and item.capture_status == "complete"
                and item.sha256
                for item in outputs
            )
        )

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
        self.assertEqual(artifacts[0].existence_state, "present")
        self.assertEqual(artifacts[0].capture_status, "complete")
        self.assertEqual(artifacts[1].existence_state, "missing")
        self.assertEqual(artifacts[1].capture_status, "complete")
        self.assertEqual(artifacts[1].role, "output")

    def test_every_artifact_state_persists_and_reloads(self) -> None:
        states: list[tuple[ArtifactExistenceState, ArtifactCaptureStatus]] = [
            ("present", "complete"),
            ("missing", "complete"),
            ("unknown", "unreadable"),
            ("present", "unreadable"),
            ("present", "unsupported"),
            ("present", "changed"),
        ]
        artifacts = [
            self._artifact(
                suffix=str(index),
                existence_state=existence_state,
                capture_status=capture_status,
            )
            for index, (existence_state, capture_status) in enumerate(states)
        ]
        with tempfile.TemporaryDirectory() as directory:
            store = ExperimentStore(Path(directory) / "bourne.sqlite3")
            store.save_record(experiment(), artifacts)
            reloaded = store.list_artifacts("01H00000000000000000000000")

        self.assertEqual(
            {(item.existence_state, item.capture_status) for item in reloaded},
            set(states),
        )
        self.assertTrue(
            all(
                item.sha256 is not None
                if (item.existence_state, item.capture_status)
                == ("present", "complete")
                else item.sha256 is None
                for item in reloaded
            )
        )

    def test_show_distinguishes_every_artifact_state(self) -> None:
        states: list[tuple[ArtifactExistenceState, ArtifactCaptureStatus]] = [
            ("present", "complete"),
            ("missing", "complete"),
            ("unknown", "unreadable"),
            ("present", "unreadable"),
            ("present", "unsupported"),
            ("present", "changed"),
        ]
        artifacts = [
            self._artifact(
                suffix=str(index),
                existence_state=existence_state,
                capture_status=capture_status,
            )
            for index, (existence_state, capture_status) in enumerate(states)
        ]

        rendered = format_show(experiment(), artifacts)

        self.assertIn("State: present", rendered)
        self.assertIn("State: missing", rendered)
        self.assertIn("State: unreadable (existence unknown)", rendered)
        self.assertIn("State: unreadable (present)", rendered)
        self.assertIn("State: unsupported (present)", rendered)
        self.assertIn("State: changed during capture (present)", rendered)
        self.assertIn("Existence: unknown", rendered)
        self.assertIn("Capture status: changed", rendered)


if __name__ == "__main__":
    unittest.main()
