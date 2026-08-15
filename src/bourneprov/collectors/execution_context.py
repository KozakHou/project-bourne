"""Conservative, allow-listed execution-context observations."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path
from typing import Mapping, Sequence

from ..models import ExecutionContext

_SAFE_ENVIRONMENT_HINTS = {
    "VIRTUAL_ENV": "virtual_environment",
    "CONDA_PREFIX": "conda_prefix",
    "CONDA_DEFAULT_ENV": "conda_environment",
}


def resolve_executable(
    requested: str,
    cwd: Path,
    environment: Mapping[str, str] | None = None,
) -> str | None:
    """Resolve an executable exactly as observed, without changing the environment."""

    env = os.environ if environment is None else environment
    has_separator = os.sep in requested or bool(os.altsep and os.altsep in requested)
    if has_separator:
        candidate = Path(requested)
        if not candidate.is_absolute():
            candidate = cwd / candidate
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            return None
        return str(resolved) if resolved.is_file() and os.access(resolved, os.X_OK) else None

    resolved = shutil.which(requested, path=env.get("PATH"))
    return str(Path(resolved).resolve(strict=False)) if resolved else None


def _containerized() -> bool | None:
    """Return true only for conservative local marker-file evidence."""

    for marker in (Path("/.dockerenv"), Path("/run/.containerenv")):
        try:
            if marker.is_file():
                return True
        except OSError:
            pass
    return None


def collect_execution_context(
    argv: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str] | None = None,
) -> ExecutionContext:
    """Collect only explicit safe fields; arbitrary environment values are never read."""

    if not argv:
        raise ValueError("a command is required")
    env = os.environ if environment is None else environment
    hints = {
        field: value
        for variable, field in _SAFE_ENVIRONMENT_HINTS.items()
        if (value := env.get(variable))
    }
    return ExecutionContext(
        requested_executable=argv[0],
        resolved_executable=resolve_executable(argv[0], cwd, env),
        recorder_executable=str(Path(sys.executable).resolve(strict=False)),
        environment_hints=hints,
        containerized=_containerized(),
    )
