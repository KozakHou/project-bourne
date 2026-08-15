"""Arbitrary command execution with captured process semantics."""

from __future__ import annotations

import codecs
import locale
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Sequence, TextIO

from .models import ExecutionResult


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _exit_code_for_returncode(returncode: int) -> int:
    return 128 + abs(returncode) if returncode < 0 else returncode


def _write_live(stream: TextIO | None, text: str, lock: threading.Lock) -> None:
    if stream is None or not text:
        return
    try:
        with lock:
            stream.write(text)
            stream.flush()
    except (BrokenPipeError, OSError, ValueError):
        # A closed display stream must not block capture or alter the experiment.
        pass


def _pump_stream(
    pipe: BinaryIO,
    destination: TextIO | None,
    capture: list[str],
    encoding: str,
    lock: threading.Lock,
) -> None:
    decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
    read = getattr(pipe, "read1", pipe.read)
    try:
        while True:
            chunk = read(64 * 1024)
            if not chunk:
                break
            text = decoder.decode(chunk)
            capture.append(text)
            _write_live(destination, text, lock)
        final = decoder.decode(b"", final=True)
        if final:
            capture.append(final)
            _write_live(destination, final, lock)
    finally:
        pipe.close()


def _process_group_options() -> dict[str, bool]:
    # A dedicated POSIX session gives Bourne a process group containing only
    # the experiment and its descendants. Other platforms retain direct-child
    # termination until an equally safe native strategy is implemented.
    return {"start_new_session": True} if os.name == "posix" else {}


def _signal_process_tree(process: subprocess.Popen[bytes], sig: signal.Signals) -> None:
    if os.name == "posix":
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        return

    if process.poll() is not None:
        return
    if sig == signal.SIGTERM:
        process.terminate()
    else:
        process.kill()


def _stop_interrupted_process(process: subprocess.Popen[bytes]) -> None:
    _signal_process_tree(process, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        _signal_process_tree(process, signal.SIGKILL)
        process.wait()


def execute_command(
    argv: Sequence[str],
    cwd: Path,
    stdout_stream: TextIO | None = None,
    stderr_stream: TextIO | None = None,
) -> ExecutionResult:
    """Execute *argv*, tee output live when requested, and preserve it."""

    if not argv:
        raise ValueError("a command is required")

    started_at = _utc_now()
    started_monotonic = time.monotonic()
    encoding = locale.getpreferredencoding(False) or "utf-8"

    try:
        process = subprocess.Popen(
            list(argv),
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **_process_group_options(),
        )
    except FileNotFoundError:
        ended_at = _utc_now()
        message = f"bourne: command not found: {argv[0]}\n"
        _write_live(stderr_stream, message, threading.Lock())
        return ExecutionResult(
            status="failed",
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=time.monotonic() - started_monotonic,
            exit_code=127,
            stdout="",
            stderr=message,
        )
    except PermissionError:
        ended_at = _utc_now()
        message = f"bourne: permission denied: {argv[0]}\n"
        _write_live(stderr_stream, message, threading.Lock())
        return ExecutionResult(
            status="failed",
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=time.monotonic() - started_monotonic,
            exit_code=126,
            stdout="",
            stderr=message,
        )
    except OSError as exc:
        ended_at = _utc_now()
        message = f"bourne: could not execute {argv[0]}: {exc}\n"
        _write_live(stderr_stream, message, threading.Lock())
        return ExecutionResult(
            status="failed",
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=time.monotonic() - started_monotonic,
            exit_code=126,
            stdout="",
            stderr=message,
        )

    if process.stdout is None or process.stderr is None:  # pragma: no cover - Popen contract
        raise RuntimeError("failed to create process output pipes")

    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    output_lock = threading.Lock()
    readers = [
        threading.Thread(
            target=_pump_stream,
            args=(process.stdout, stdout_stream, stdout_parts, encoding, output_lock),
            name="bourne-stdout",
        ),
        threading.Thread(
            target=_pump_stream,
            args=(process.stderr, stderr_stream, stderr_parts, encoding, output_lock),
            name="bourne-stderr",
        ),
    ]
    for reader in readers:
        reader.start()

    interrupted = False
    try:
        process.wait()
    except KeyboardInterrupt:
        interrupted = True
        _stop_interrupted_process(process)
    finally:
        for reader in readers:
            reader.join()

    stdout = "".join(stdout_parts)
    stderr = "".join(stderr_parts)

    ended_at = _utc_now()
    duration = time.monotonic() - started_monotonic
    if interrupted:
        status = "interrupted"
        exit_code = 130
    else:
        exit_code = _exit_code_for_returncode(process.returncode)
        status = "completed" if exit_code == 0 else "failed"

    return ExecutionResult(
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )
