"""Portable system and optional NVIDIA runtime collection."""

from __future__ import annotations

import csv
import io
import json
import platform
import re
import shutil
from pathlib import Path

from ..bounded_subprocess import run_bounded_command
from ..models import SystemProvenance

_TIMEOUT_SECONDS = 5
_MAX_OUTPUT_BYTES = 1024 * 1024
_CUDA_PATTERN = re.compile(r"CUDA Version:\s*([^\s|]+)", re.IGNORECASE)


def collect_cpu() -> str | None:
    """Return a useful CPU description using portable data first."""

    processor = platform.processor().strip()
    architecture = platform.machine().strip()
    if processor and processor.casefold() != architecture.casefold():
        return processor

    cpuinfo = Path("/proc/cpuinfo")
    try:
        for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
            key, separator, value = line.partition(":")
            if separator and key.strip().lower() in {"model name", "hardware", "processor"}:
                description = value.strip()
                if description and not description.isdigit():
                    return description
    except OSError:
        pass

    lscpu = shutil.which("lscpu")
    if lscpu:
        try:
            completed = run_bounded_command(
                [lscpu, "--json"],
                timeout=_TIMEOUT_SECONDS,
                max_output_bytes=_MAX_OUTPUT_BYTES,
            )
            if completed.returncode == 0:
                payload = json.loads(completed.stdout)
                models = [
                    item.get("data", "").strip()
                    for item in payload.get("lscpu", [])
                    if item.get("field", "").rstrip(":") == "Model name"
                ]
                unique_models = list(dict.fromkeys(model for model in models if model))
                if unique_models:
                    return " / ".join(unique_models)
        except (OSError, json.JSONDecodeError, AttributeError):
            pass
    return architecture or None


def _run_nvidia_smi(executable: str, *arguments: str) -> tuple[str | None, str | None]:
    try:
        completed = run_bounded_command(
            [executable, *arguments],
            timeout=_TIMEOUT_SECONDS,
            max_output_bytes=_MAX_OUTPUT_BYTES,
        )
    except OSError as exc:
        return None, f"nvidia-smi failed: {exc}"
    if completed.timed_out:
        return None, "nvidia-smi timed out"
    if completed.returncode != 0:
        detail = (
            completed.stderr.strip()
            or completed.stdout.strip()
            or f"nvidia-smi exited with code {completed.returncode}"
        )
        return None, detail
    return completed.stdout, None


def collect_nvidia() -> dict[str, object]:
    """Collect active NVIDIA state without treating it as required."""

    executable = shutil.which("nvidia-smi")
    if executable is None:
        return {
            "gpu_available": False,
            "gpus": [],
            "nvidia_driver_version": None,
            "cuda_version": None,
            "cuda_version_source": None,
            "gpu_error": "nvidia-smi executable not found",
        }

    query_output, query_error = _run_nvidia_smi(
        executable,
        "--query-gpu=index,name,uuid,driver_version,memory.total",
        "--format=csv,noheader,nounits",
    )
    if query_output is None:
        return {
            "gpu_available": False,
            "gpus": [],
            "nvidia_driver_version": None,
            "cuda_version": None,
            "cuda_version_source": None,
            "gpu_error": query_error,
        }

    gpus: list[dict[str, str]] = []
    for row in csv.reader(io.StringIO(query_output)):
        if len(row) < 5:
            continue
        index, name, uuid, driver, memory_total = (part.strip() for part in row[:5])
        gpus.append(
            {
                "index": index,
                "name": name,
                "uuid": uuid,
                "memory_total_mib": memory_total,
            }
        )

    driver_version = None
    if gpus:
        first_row = next(csv.reader(io.StringIO(query_output)), [])
        if len(first_row) >= 4:
            driver_version = first_row[3].strip() or None

    summary_output, summary_error = _run_nvidia_smi(executable)
    cuda_version = None
    if summary_output:
        match = _CUDA_PATTERN.search(summary_output)
        cuda_version = match.group(1) if match else None

    errors = [message for message in (query_error, summary_error) if message]
    if not gpus and not errors:
        errors.append("nvidia-smi reported no GPUs")

    return {
        "gpu_available": bool(gpus),
        "gpus": gpus,
        "nvidia_driver_version": driver_version,
        "cuda_version": cuda_version,
        "cuda_version_source": (
            "maximum CUDA version supported by the active driver, reported by nvidia-smi"
            if cuda_version
            else None
        ),
        "gpu_error": "; ".join(errors) or None,
    }


def collect_system() -> SystemProvenance:
    """Collect portable host data plus optional active NVIDIA metadata."""

    nvidia = collect_nvidia()
    return SystemProvenance(
        operating_system=platform.system() or "unknown",
        os_version=platform.version() or "unknown",
        architecture=platform.machine() or "unknown",
        hostname=platform.node() or "unknown",
        cpu=collect_cpu(),
        gpu_available=bool(nvidia["gpu_available"]),
        gpus=list(nvidia["gpus"]),  # type: ignore[arg-type]
        nvidia_driver_version=nvidia["nvidia_driver_version"],  # type: ignore[arg-type]
        cuda_version=nvidia["cuda_version"],  # type: ignore[arg-type]
        cuda_version_source=nvidia["cuda_version_source"],  # type: ignore[arg-type]
        gpu_error=nvidia["gpu_error"],  # type: ignore[arg-type]
    )
