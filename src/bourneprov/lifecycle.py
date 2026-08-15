"""Experiment lifecycle coordination."""

from __future__ import annotations

import platform
from pathlib import Path
from typing import Sequence, TextIO

from .artifacts import capture_artifacts
from .collectors import collect_execution_context, collect_git, collect_system
from .execution import execute_command
from .ids import new_ulid
from .models import (
    ExecutionContext,
    Experiment,
    ExperimentLineage,
    GitProvenance,
    SystemProvenance,
)
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


def _safe_execution_context(argv: Sequence[str], cwd: Path) -> ExecutionContext:
    try:
        return collect_execution_context(argv, cwd)
    except Exception:  # Execution-context collection must never prevent execution.
        return ExecutionContext(requested_executable=argv[0])


def run_experiment(
    argv: Sequence[str],
    cwd: Path | None = None,
    stdout_stream: TextIO | None = None,
    stderr_stream: TextIO | None = None,
    experiment_id: str | None = None,
) -> Experiment:
    """Collect pre-execution provenance and run one experiment."""

    if not argv:
        raise ValueError("a command is required")
    execution_directory = (cwd or Path.cwd()).resolve()
    public_id = experiment_id or new_ulid()

    # Capture mutable repository and machine state before the experiment can alter it.
    git = _safe_git(execution_directory)
    system = _safe_system()
    execution_context = _safe_execution_context(argv, execution_directory)
    execution = execute_command(
        argv,
        execution_directory,
        stdout_stream=stdout_stream,
        stderr_stream=stderr_stream,
    )

    return Experiment(
        id=public_id,
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
        execution_context=execution_context,
    )


def run_and_record(
    argv: Sequence[str],
    store: ExperimentStore,
    cwd: Path | None = None,
    stdout_stream: TextIO | None = None,
    stderr_stream: TextIO | None = None,
    input_paths: Sequence[str] = (),
    output_paths: Sequence[str] = (),
    parent_experiment_id: str | None = None,
) -> Experiment:
    """Run and durably save an experiment before returning process semantics."""

    store.initialize()
    if parent_experiment_id is not None:
        # Reject stale/programmatic parent IDs before launching an expensive workload.
        store.get(parent_experiment_id)
    execution_directory = (cwd or Path.cwd()).resolve()
    experiment_id = new_ulid()
    inputs = capture_artifacts(
        experiment_id, "input", list(input_paths), execution_directory
    )
    experiment = run_experiment(
        argv,
        cwd=execution_directory,
        stdout_stream=stdout_stream,
        stderr_stream=stderr_stream,
        experiment_id=experiment_id,
    )
    outputs = capture_artifacts(
        experiment_id, "output", list(output_paths), execution_directory
    )
    lineage = (
        [
            ExperimentLineage(
                child_experiment_id=experiment_id,
                parent_experiment_id=parent_experiment_id,
                relationship="derived_from",
                created_at=experiment.ended_at,
            )
        ]
        if parent_experiment_id is not None
        else []
    )
    store.save_record(experiment, [*inputs, *outputs], lineage)
    return experiment
