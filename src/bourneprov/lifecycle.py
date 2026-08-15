"""Experiment lifecycle coordination."""

from __future__ import annotations

import platform
from pathlib import Path
from typing import Sequence, TextIO

from .collectors import collect_git, collect_system
from .execution import execute_command
from .ids import new_ulid
from .models import Experiment, GitProvenance, SystemProvenance
from .storage import ExperimentStore


def _safe_git(cwd: Path) -> GitProvenance:
    try:
        return collect_git(cwd)
    except Exception as exc:  # Collectors must never prevent experiment execution.
        return GitProvenance(available=False, error=f"Git collector failed: {exc}")


def _safe_system() -> SystemProvenance:
    try:
        return collect_system()
    except Exception as exc:  # Collectors must never prevent experiment execution.
        return SystemProvenance(
            operating_system=platform.system() or "unknown",
            os_version=platform.version() or "unknown",
            architecture=platform.machine() or "unknown",
            hostname=platform.node() or "unknown",
            cpu=None,
            gpu_available=False,
            gpu_error="GPU metadata unavailable because the system collector failed",
            collector_error=str(exc),
        )


def run_experiment(
    argv: Sequence[str],
    cwd: Path | None = None,
    stdout_stream: TextIO | None = None,
    stderr_stream: TextIO | None = None,
) -> Experiment:
    """Collect pre-execution provenance and run one experiment."""

    if not argv:
        raise ValueError("a command is required")
    execution_directory = (cwd or Path.cwd()).resolve()
    experiment_id = new_ulid()

    # Capture mutable repository and machine state before the experiment can alter it.
    git = _safe_git(execution_directory)
    system = _safe_system()
    execution = execute_command(
        argv,
        execution_directory,
        stdout_stream=stdout_stream,
        stderr_stream=stderr_stream,
    )

    return Experiment(
        id=experiment_id,
        status=execution.status,
        command=argv[0],
        arguments=list(argv[1:]),
        working_directory=str(execution_directory),
        started_at=execution.started_at,
        ended_at=execution.ended_at,
        duration_seconds=execution.duration_seconds,
        exit_code=execution.exit_code,
        stdout=execution.stdout,
        stderr=execution.stderr,
        git=git,
        system=system,
    )


def run_and_record(
    argv: Sequence[str],
    store: ExperimentStore,
    cwd: Path | None = None,
    stdout_stream: TextIO | None = None,
    stderr_stream: TextIO | None = None,
) -> Experiment:
    """Run and durably save an experiment before returning process semantics."""

    store.initialize()
    experiment = run_experiment(
        argv,
        cwd=cwd,
        stdout_stream=stdout_stream,
        stderr_stream=stderr_stream,
    )
    store.save(experiment)
    return experiment
