"""Small shell-free subprocess helper with time and output bounds."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class BoundedCommandResult:
    argv: tuple[str, ...]
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool = False
    truncated: bool = False


def run_bounded_command(
    argv: Sequence[str],
    *,
    timeout: float = 5.0,
    max_output_bytes: int = 1024 * 1024,
    input_bytes: bytes | None = None,
    shell: bool = False,
) -> BoundedCommandResult:
    """Run one trusted argv while draining and bounding both output streams."""

    if not argv:
        raise ValueError("an argument vector is required")
    if shell:
        raise ValueError("bounded commands never use a shell")
    process = subprocess.Popen(
        list(argv),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE if input_bytes is not None else None,
        shell=False,
        start_new_session=os.name == "posix",
    )
    if process.stdout is None or process.stderr is None:  # pragma: no cover
        raise RuntimeError("failed to create provider output pipes")
    captures = [bytearray(), bytearray()]
    truncated = [False, False]

    def drain(stream: Any, destination: bytearray, index: int) -> None:
        try:
            while chunk := stream.read(64 * 1024):
                remaining = max_output_bytes - len(destination)
                if remaining > 0:
                    destination.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    truncated[index] = True
        finally:
            stream.close()

    readers = [
        threading.Thread(target=drain, args=(process.stdout, captures[0], 0), daemon=True),
        threading.Thread(target=drain, args=(process.stderr, captures[1], 1), daemon=True),
    ]
    for reader in readers:
        reader.start()
    if input_bytes is not None:
        if process.stdin is None:  # pragma: no cover
            raise RuntimeError("failed to create provider input pipe")
        try:
            process.stdin.write(input_bytes)
        except BrokenPipeError:
            pass
        finally:
            process.stdin.close()
    timed_out = False
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
        process.wait()
    finally:
        for reader in readers:
            reader.join()
    return BoundedCommandResult(
        argv=tuple(argv),
        returncode=process.returncode,
        stdout=captures[0].decode("utf-8", errors="replace"),
        stderr=captures[1].decode("utf-8", errors="replace"),
        timed_out=timed_out,
        truncated=any(truncated),
    )
