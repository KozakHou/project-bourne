"""Bounded, non-invasive workload inspection."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from .ids import new_ulid
from .workload_models import (
    CapabilityRequirement,
    ExecutionConstraints,
    LauncherRequirement,
    RequirementEvidence,
    ResourceRequirements,
    WorkloadSpec,
)

_PROJECT_MARKERS = (
    "pyproject.toml",
    "requirements.txt",
    "environment.yml",
    "environment.yaml",
    "Dockerfile",
    "Makefile",
    "CMakeLists.txt",
    "Project.toml",
)
_MPI_LAUNCHERS = {"mpirun", "mpiexec", "srun"}
_MPI_COUNT_FLAGS = {"-n", "-np", "--ntasks"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def inspect_workload(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    inputs: Sequence[str] = (),
    outputs: Sequence[str] = (),
    resources: ResourceRequirements | None = None,
    constraints: ExecutionConstraints | None = None,
    parent_experiment_id: str | None = None,
) -> WorkloadSpec:
    """Create a useful spec without executing, importing, or recursively scanning.

    Inspection is deliberately limited to the command and a fixed allowlist of
    marker names in the exact working directory. Marker contents are not read.
    """

    if not argv or not argv[0]:
        raise ValueError("a scientific command is required")
    working_directory = (cwd or Path.cwd()).resolve(strict=False)
    marker_names = [
        name for name in _PROJECT_MARKERS
        if (working_directory / name).is_file()
    ]
    return compile_workload(
        argv,
        resolved_working_directory=str(working_directory),
        inputs=inputs,
        outputs=outputs,
        resources=resources,
        constraints=constraints,
        parent_experiment_id=parent_experiment_id,
        project_markers=marker_names,
        inspection_scope="argv_and_allowlisted_cwd_markers",
    )


def compile_workload(
    argv: Sequence[str],
    *,
    resolved_working_directory: str,
    inputs: Sequence[str] = (),
    outputs: Sequence[str] = (),
    resources: ResourceRequirements | None = None,
    constraints: ExecutionConstraints | None = None,
    parent_experiment_id: str | None = None,
    project_markers: Sequence[str] = (),
    inspection_scope: str = "argv_and_authoritative_working_directory",
) -> WorkloadSpec:
    """Compile a spec from an authoritative cwd without filesystem observation.

    Callers are responsible for resolving the working-directory value in its own
    execution context. This boundary deliberately treats that value as opaque: it
    does not resolve it or inspect it through the local control-plane filesystem.
    """

    if not argv or not argv[0]:
        raise ValueError("a scientific command is required")
    if not resolved_working_directory:
        raise ValueError("a resolved working directory is required")
    command = [str(item) for item in argv]
    marker_names = [str(item) for item in project_markers]
    requested = resources or ResourceRequirements()
    selected_constraints = constraints or ExecutionConstraints()
    evidence = [
        RequirementEvidence(
            subject="executable", state="explicit", source="command_argv",
            value=command[0],
        ),
        RequirementEvidence(
            subject="working_directory", state="explicit", source="command_context",
            value=resolved_working_directory,
        ),
    ]
    for name, value in vars(requested).items():
        if value is not None:
            evidence.append(
                RequirementEvidence(
                    subject=f"resources.{name}", state="explicit",
                    source="user_constraint", value=value,
                )
            )
    for name, value in vars(selected_constraints).items():
        if value not in (None, "auto"):
            evidence.append(
                RequirementEvidence(
                    subject=f"constraints.{name}", state="explicit",
                    source="user_constraint", value=value,
                )
            )
    for path in inputs:
        evidence.append(
            RequirementEvidence(
                subject="input", state="explicit", source="user_constraint", value=path,
            )
        )
    for path in outputs:
        evidence.append(
            RequirementEvidence(
                subject="output", state="explicit", source="user_constraint", value=path,
            )
        )

    launcher, inferred_ranks = _launcher_from_argv(command)
    if launcher is not None:
        evidence.append(
            RequirementEvidence(
                subject="launcher", state="explicit", source="command_argv",
                value=launcher,
            )
        )
    effective_resources = requested
    launcher_requirement: LauncherRequirement | None = None
    if requested.mpi_ranks is not None:
        launcher_requirement = LauncherRequirement(
            name=launcher, mpi_ranks=requested.mpi_ranks, evidence_state="explicit"
        )
    elif inferred_ranks is not None:
        effective_resources = ResourceRequirements(
            **{**vars(requested), "mpi_ranks": inferred_ranks}
        )
        launcher_requirement = LauncherRequirement(
            name=launcher, mpi_ranks=inferred_ranks, evidence_state="inferred"
        )
        evidence.append(
            RequirementEvidence(
                subject="resources.mpi_ranks", state="inferred",
                source="launcher_argv", value=inferred_ranks,
                detail="parsed from an explicit MPI launcher argument",
            )
        )

    for name in marker_names:
        evidence.append(
            RequirementEvidence(
                subject="project_marker", state="observed",
                source="bounded_directory_marker", value=name,
            )
        )

    executable_name = Path(command[0]).name or command[0]
    capability_requirements = [
        CapabilityRequirement(
            kind="executable", name=executable_name,
            evidence_state="explicit", required=True,
        )
    ]
    return WorkloadSpec(
        id=new_ulid(), created_at=utc_now(),
        working_directory=resolved_working_directory, executable=command[0],
        arguments=command[1:], inputs=list(inputs), outputs=list(outputs),
        resources=effective_resources,
        capability_requirements=capability_requirements,
        launcher_requirement=launcher_requirement,
        constraints=selected_constraints, evidence=evidence,
        parent_experiment_id=parent_experiment_id,
        project_markers=marker_names,
        metadata={
            "inspection_scope": inspection_scope,
            "recursive_scan": False,
            "marker_contents_read": False,
            "commands_executed": False,
        },
    )


def _launcher_from_argv(argv: list[str]) -> tuple[str | None, int | None]:
    launcher = Path(argv[0]).name.casefold()
    if launcher not in _MPI_LAUNCHERS:
        return None, None
    for index, item in enumerate(argv[1:], start=1):
        if item in _MPI_COUNT_FLAGS and index + 1 < len(argv):
            try:
                value = int(argv[index + 1])
            except ValueError:
                return launcher, None
            return launcher, value if value > 0 else None
        if item.startswith("--ntasks="):
            try:
                value = int(item.partition("=")[2])
            except ValueError:
                return launcher, None
            return launcher, value if value > 0 else None
    return launcher, None
