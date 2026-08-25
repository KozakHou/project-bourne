"""Low-overhead, execution-scoped process telemetry with explicit coverage."""

from __future__ import annotations

import os
import platform
import threading
from pathlib import Path
from typing import Any

SAMPLE_INTERVAL_SECONDS = 0.10


class ProcessRuntimeCollector:
    """Sample one process tree without observing unrelated machine workloads."""

    def __init__(self, pid: int):
        self.pid = pid
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._seen: set[int] = set()
        self._maximum: dict[int, dict[str, int]] = {}
        self._peak_rss = 0
        self._peak_processes = 0
        self._samples = 0
        self._diagnostic: str | None = None
        self._supported = os.name == "posix" and Path(f"/proc/{pid}/stat").exists()

    def start(self) -> None:
        if not self._supported:
            return
        self._sample()
        self._thread = threading.Thread(
            target=self._run, name="bourne-runtime", daemon=True
        )
        self._thread.start()

    def finish(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._sample()
        if not self._supported:
            return {
                "collector": "unsupported",
                "platform": platform.system() or "unknown",
                "diagnostic": "execution-scoped /proc process telemetry is unavailable",
            }
        with self._lock:
            cpu_ticks = sum(item.get("cpu_ticks", 0) for item in self._maximum.values())
            read_bytes = sum(item.get("read_bytes", 0) for item in self._maximum.values())
            write_bytes = sum(item.get("write_bytes", 0) for item in self._maximum.values())
            try:
                clock_ticks = os.sysconf("SC_CLK_TCK")
            except (OSError, ValueError):
                clock_ticks = 100
            return {
                "collector": "linux_procfs",
                "root_pid": self.pid,
                "observed_processes": len(self._seen),
                "peak_concurrent_processes": self._peak_processes,
                "samples": self._samples,
                "cpu_seconds": cpu_ticks / clock_ticks,
                "peak_rss_bytes": self._peak_rss,
                "read_bytes": read_bytes,
                "write_bytes": write_bytes,
                "sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
                "diagnostic": self._diagnostic,
            }

    def _run(self) -> None:
        while not self._stop.wait(SAMPLE_INTERVAL_SECONDS):
            self._sample()

    def _sample(self) -> None:
        if not self._supported:
            return
        try:
            pids = _process_tree(self.pid)
            records = [_proc_record(pid) for pid in pids]
        except OSError as exc:
            self._diagnostic = f"process telemetry sampling failed: {exc}"[:4096]
            return
        records = [record for record in records if record is not None]
        if not records:
            return
        rss = sum(record["rss_bytes"] for record in records)
        with self._lock:
            self._samples += 1
            self._peak_rss = max(self._peak_rss, rss)
            self._peak_processes = max(self._peak_processes, len(records))
            for record in records:
                pid = record.pop("pid")
                self._seen.add(pid)
                current = self._maximum.setdefault(pid, {})
                for key, value in record.items():
                    current[key] = max(current.get(key, 0), value)


def _process_tree(root: int) -> list[int]:
    pending = [root]
    seen: set[int] = set()
    while pending and len(seen) < 16384:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        try:
            raw = Path(f"/proc/{pid}/task/{pid}/children").read_text(encoding="ascii")
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
        pending.extend(int(item) for item in raw.split() if item.isdigit())
    return sorted(seen)


def _proc_record(pid: int) -> dict[str, int] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 2 :].split()
    if len(fields) < 22:
        return None
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError):
        page_size = 4096
    record = {
        "pid": pid,
        "cpu_ticks": int(fields[11]) + int(fields[12]),
        "rss_bytes": max(0, int(fields[21])) * page_size,
        "read_bytes": 0,
        "write_bytes": 0,
    }
    try:
        for line in Path(f"/proc/{pid}/io").read_text(encoding="ascii").splitlines():
            key, separator, value = line.partition(":")
            if separator and key in {"read_bytes", "write_bytes"}:
                record[key] = max(0, int(value.strip()))
    except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError):
        pass
    return record
