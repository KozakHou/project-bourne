from __future__ import annotations

import io
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.execution import execute_command
from bourneprov.storage import ExperimentStore


class ExecutionTests(unittest.TestCase):
    def test_success_captures_stdout_and_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = execute_command(
                [
                    sys.executable,
                    "-c",
                    "import sys; print('|'.join(sys.argv[1:]))",
                    "alpha",
                    "two words",
                ],
                Path(directory),
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout, "alpha|two words\n")
        self.assertEqual(result.stderr, "")
        self.assertGreaterEqual(result.duration_seconds, 0)

    def test_failure_captures_stderr_and_preserves_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = execute_command(
                [
                    sys.executable,
                    "-c",
                    "import sys; print('boom', file=sys.stderr); raise SystemExit(23)",
                ],
                Path(directory),
            )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.exit_code, 23)
        self.assertEqual(result.stderr, "boom\n")

    def test_missing_executable_is_a_recordable_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = execute_command(
                ["bourne-command-that-does-not-exist"], Path(directory)
            )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.exit_code, 127)
        self.assertIn("command not found", result.stderr)

    def test_keyboard_interrupt_terminates_and_marks_the_experiment(self) -> None:
        class InterruptingProcess:
            returncode = -15
            pid = 12345
            stdout = io.BytesIO(b"partial output\n")
            stderr = io.BytesIO(b"")

            def wait(self, timeout=None):  # type: ignore[no-untyped-def]
                raise KeyboardInterrupt

        process = InterruptingProcess()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("bourneprov.execution.subprocess.Popen", return_value=process),
            patch("bourneprov.execution._stop_interrupted_process") as stop,
        ):
            result = execute_command(["solver"], Path(directory))

        stop.assert_called_once_with(process)
        self.assertEqual(result.status, "interrupted")
        self.assertEqual(result.exit_code, 130)
        self.assertEqual(result.stdout, "partial output\n")

    def test_output_is_streamed_before_process_completion(self) -> None:
        class NotifyingStream(io.StringIO):
            def __init__(self) -> None:
                super().__init__()
                self.written = threading.Event()

            def write(self, value: str) -> int:
                result = super().write(value)
                self.written.set()
                return result

        destination = NotifyingStream()
        result: list[object] = []
        with tempfile.TemporaryDirectory() as directory:
            worker = threading.Thread(
                target=lambda: result.append(
                    execute_command(
                        [
                            sys.executable,
                            "-u",
                            "-c",
                            "import time; print('first', flush=True); "
                            "time.sleep(0.75); print('last', flush=True)",
                        ],
                        Path(directory),
                        stdout_stream=destination,
                    )
                )
            )
            worker.start()
            self.assertTrue(destination.written.wait(timeout=2))
            self.assertTrue(worker.is_alive(), "output was delayed until process completion")
            worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(destination.getvalue(), "first\nlast\n")
        self.assertEqual(result[0].stdout, "first\nlast\n")

    def test_mixed_stdout_and_stderr_do_not_deadlock(self) -> None:
        chunks = 512
        chunk_size = 2048
        code = (
            "import os\n"
            f"chunk_out = b'o' * {chunk_size}\n"
            f"chunk_err = b'e' * {chunk_size}\n"
            f"for _ in range({chunks}):\n"
            "    os.write(1, chunk_out)\n"
            "    os.write(2, chunk_err)\n"
        )
        result: list[object] = []
        with tempfile.TemporaryDirectory() as directory:
            worker = threading.Thread(
                target=lambda: result.append(
                    execute_command([sys.executable, "-c", code], Path(directory))
                )
            )
            worker.start()
            worker.join(timeout=10)

        self.assertFalse(worker.is_alive(), "mixed output filled a pipe and deadlocked")
        self.assertEqual(len(result[0].stdout), chunks * chunk_size)
        self.assertEqual(len(result[0].stderr), chunks * chunk_size)

    @unittest.skipUnless(os.name == "posix", "process-group signals require POSIX")
    def test_sigint_records_interruption_and_terminates_descendant_group(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        child_code = (
            "import pathlib, signal, sys, time\n"
            "ready = pathlib.Path(sys.argv[1])\n"
            "terminated = pathlib.Path(sys.argv[2])\n"
            "def stop(signum, frame):\n"
            "    terminated.write_text('terminated', encoding='utf-8')\n"
            "    raise SystemExit(0)\n"
            "signal.signal(signal.SIGTERM, stop)\n"
            "ready.write_text('ready', encoding='utf-8')\n"
            "while True:\n"
            "    time.sleep(1)\n"
        )
        parent_code = (
            "import subprocess, sys, time\n"
            "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]])\n"
            "while True:\n"
            "    time.sleep(1)\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            ready = root / "descendant-ready"
            terminated = root / "descendant-terminated"
            environment = os.environ.copy()
            environment.update(
                {"BOURNE_DB": str(database), "PYTHONPATH": str(project_root / "src")}
            )
            bourne = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "bourneprov",
                    "run",
                    sys.executable,
                    "-c",
                    parent_code,
                    child_code,
                    str(ready),
                    str(terminated),
                ],
                cwd=root,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(ready.exists(), "descendant did not start")

                bourne.send_signal(signal.SIGINT)
                stdout, stderr = bourne.communicate(timeout=10)
            finally:
                if bourne.poll() is None:
                    bourne.send_signal(signal.SIGINT)
                    bourne.communicate(timeout=10)
            records = ExperimentStore(database).list_recent()
            descendant_terminated = terminated.exists()

        self.assertEqual(bourne.returncode, 130, stderr)
        self.assertTrue(descendant_terminated, "descendant did not receive SIGTERM")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].status, "interrupted")
        self.assertEqual(records[0].exit_code, 130)


if __name__ == "__main__":
    unittest.main()
