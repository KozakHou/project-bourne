"""Framework-independent workload planning and execution lifecycle models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from .planning_models import ResolvedEnvironment, ResourceShape

EvidenceState = Literal["explicit", "observed", "inferred", "historical", "unknown"]
BackendName = Literal["direct", "slurm", "pbs"]
CompatibilityState = Literal["compatible", "partial", "incompatible", "unknown"]
_EVIDENCE_STATES = {"explicit", "observed", "inferred", "historical", "unknown"}
_BACKENDS = {"direct", "slurm", "pbs"}
_COMPATIBILITY_STATES = {"compatible", "partial", "incompatible", "unknown"}


@dataclass(frozen=True)
class RequirementEvidence:
    """Why one workload requirement or planning decision exists."""

    subject: str
    state: EvidenceState
    source: str
    value: Any = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.state not in _EVIDENCE_STATES:
            raise ValueError(f"unsupported evidence state: {self.state}")


@dataclass(frozen=True)
class ResourceRequirements:
    cpus: int | None = None
    gpus: int | None = None
    nodes: int | None = None
    mpi_ranks: int | None = None
    memory_bytes: int | None = None
    walltime_seconds: int | None = None

    def __post_init__(self) -> None:
        for name in ("cpus", "nodes", "mpi_ranks", "memory_bytes", "walltime_seconds"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(f"{name} must be at least 1")
        if self.gpus is not None and (
            isinstance(self.gpus, bool)
            or not isinstance(self.gpus, int)
            or self.gpus < 0
        ):
            raise ValueError("gpus must be at least 0")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ResourceRequirements":
        return cls(**value)


@dataclass(frozen=True)
class CapabilityRequirement:
    kind: str
    name: str
    evidence_state: EvidenceState
    required: bool = True

    def __post_init__(self) -> None:
        if self.evidence_state not in _EVIDENCE_STATES:
            raise ValueError(f"unsupported evidence state: {self.evidence_state}")


@dataclass(frozen=True)
class LauncherRequirement:
    name: str | None
    mpi_ranks: int
    evidence_state: EvidenceState

    def __post_init__(self) -> None:
        if (
            isinstance(self.mpi_ranks, bool)
            or not isinstance(self.mpi_ranks, int)
            or self.mpi_ranks < 1
        ):
            raise ValueError("mpi_ranks must be at least 1")
        if self.evidence_state not in _EVIDENCE_STATES:
            raise ValueError(f"unsupported evidence state: {self.evidence_state}")


@dataclass(frozen=True)
class ExecutionConstraints:
    backend: str = "auto"
    target: str | None = None
    context: str | None = None

    def __post_init__(self) -> None:
        if self.backend not in {"auto", "direct", "slurm", "pbs"}:
            raise ValueError(f"unsupported backend: {self.backend}")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ExecutionConstraints":
        return cls(**value)


@dataclass(frozen=True)
class WorkloadSpec:
    id: str
    created_at: str
    working_directory: str
    executable: str
    arguments: list[str]
    inputs: list[str]
    outputs: list[str]
    resources: ResourceRequirements
    capability_requirements: list[CapabilityRequirement]
    launcher_requirement: LauncherRequirement | None
    constraints: ExecutionConstraints
    evidence: list[RequirementEvidence]
    parent_experiment_id: str | None = None
    project_markers: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.id or not self.executable or not self.working_directory:
            raise ValueError("workload ID, executable, and working directory are required")
        _validate_strings(self.arguments, "workload arguments")
        _validate_strings(self.inputs, "workload inputs")
        _validate_strings(self.outputs, "workload outputs")

    @property
    def argv(self) -> list[str]:
        return [self.executable, *self.arguments]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "WorkloadSpec":
        return cls(
            id=value["id"],
            created_at=value["created_at"],
            working_directory=value["working_directory"],
            executable=value["executable"],
            arguments=list(value["arguments"]),
            inputs=list(value["inputs"]),
            outputs=list(value["outputs"]),
            resources=ResourceRequirements.from_dict(value["resources"]),
            capability_requirements=[
                CapabilityRequirement(**item)
                for item in value["capability_requirements"]
            ],
            launcher_requirement=(
                None
                if value.get("launcher_requirement") is None
                else LauncherRequirement(**value["launcher_requirement"])
            ),
            constraints=ExecutionConstraints.from_dict(value["constraints"]),
            evidence=[RequirementEvidence(**item) for item in value["evidence"]],
            parent_experiment_id=value.get("parent_experiment_id"),
            project_markers=list(value.get("project_markers", [])),
            metadata=dict(value.get("metadata", {})),
        )


@dataclass(frozen=True)
class DecisionEvidence:
    state: EvidenceState
    subject: str
    message: str
    subject_id: str | None = None

    def __post_init__(self) -> None:
        if self.state not in _EVIDENCE_STATES:
            raise ValueError(f"unsupported evidence state: {self.state}")


@dataclass(frozen=True)
class PlanCandidate:
    backend: BackendName
    access_target_id: str
    execution_target_id: str | None
    execution_context_id: str | None
    scheduler_id: str | None
    compatibility_state: CompatibilityState
    unresolved_conditions: list[str]
    decision_evidence: list[DecisionEvidence]

    def __post_init__(self) -> None:
        if self.backend not in _BACKENDS:
            raise ValueError(f"unsupported backend: {self.backend}")
        if self.compatibility_state not in _COMPATIBILITY_STATES:
            raise ValueError(
                f"unsupported compatibility state: {self.compatibility_state}"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionPlan:
    id: str
    workload_id: str
    inventory_snapshot_id: str
    backend: BackendName
    access_target_id: str
    execution_target_id: str | None
    execution_context_id: str | None
    scheduler_id: str | None
    requested_resources: ResourceRequirements
    executable: str
    arguments: list[str]
    working_directory: str
    inputs: list[str]
    outputs: list[str]
    compatibility_state: CompatibilityState
    unresolved_conditions: list[str]
    decision_evidence: list[DecisionEvidence]
    created_at: str
    site_id: str | None = None
    resource_shape: ResourceShape | None = None
    environment: ResolvedEnvironment | None = None
    workload_variant_id: str | None = None
    selection_summary_id: str | None = None
    policy_basis: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.backend not in _BACKENDS:
            raise ValueError(f"unsupported backend: {self.backend}")
        if self.compatibility_state not in _COMPATIBILITY_STATES:
            raise ValueError(
                f"unsupported compatibility state: {self.compatibility_state}"
            )
        if not self.id or not self.workload_id or not self.inventory_snapshot_id:
            raise ValueError("plan, workload, and inventory IDs are required")
        if not self.executable or not self.working_directory:
            raise ValueError("plan executable and working directory are required")
        _validate_strings(self.arguments, "plan arguments")
        _validate_strings(self.inputs, "plan inputs")
        _validate_strings(self.outputs, "plan outputs")

    @property
    def argv(self) -> list[str]:
        return [self.executable, *self.arguments]

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["resource_shape"] = (
            None if self.resource_shape is None else self.resource_shape.to_dict()
        )
        value["environment"] = (
            None if self.environment is None else self.environment.to_dict()
        )
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ExecutionPlan":
        return cls(
            id=value["id"], workload_id=value["workload_id"],
            inventory_snapshot_id=value["inventory_snapshot_id"],
            backend=value["backend"], access_target_id=value["access_target_id"],
            execution_target_id=value.get("execution_target_id"),
            execution_context_id=value.get("execution_context_id"),
            scheduler_id=value.get("scheduler_id"),
            requested_resources=ResourceRequirements.from_dict(
                value["requested_resources"]
            ),
            executable=value["executable"], arguments=list(value["arguments"]),
            working_directory=value["working_directory"],
            inputs=list(value["inputs"]), outputs=list(value["outputs"]),
            compatibility_state=value["compatibility_state"],
            unresolved_conditions=list(value["unresolved_conditions"]),
            decision_evidence=[
                DecisionEvidence(**item) for item in value["decision_evidence"]
            ],
            created_at=value["created_at"],
            site_id=value.get("site_id"),
            resource_shape=(
                None
                if value.get("resource_shape") is None
                else ResourceShape.from_dict(value["resource_shape"])
            ),
            environment=(
                None
                if value.get("environment") is None
                else ResolvedEnvironment.from_dict(value["environment"])
            ),
            workload_variant_id=value.get("workload_variant_id"),
            selection_summary_id=value.get("selection_summary_id"),
            policy_basis=list(value.get("policy_basis", [])),
        )


@dataclass(frozen=True)
class ResolutionResult:
    """All considered candidates and the sole safe selection, if one exists."""

    candidates: list[PlanCandidate]
    selected: ExecutionPlan | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates": [item.to_dict() for item in self.candidates],
            "selected": None if self.selected is None else self.selected.to_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ExecutionAttempt:
    id: str
    plan_id: str
    backend: BackendName
    state: str
    created_at: str
    updated_at: str
    submitting_identity: str | None
    staging_directory: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class SchedulerJob:
    execution_id: str
    family: str
    job_id: str
    submitting_identity: str
    submitted_at: str
    state: str
    last_observed_at: str


@dataclass(frozen=True)
class AllocationObservation:
    id: str
    execution_id: str
    observed_at: str
    resources: dict[str, Any]
    hosts: list[str]
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionEvent:
    id: str
    execution_id: str
    occurred_at: str
    state: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionView:
    execution: ExecutionAttempt
    plan: ExecutionPlan
    workload: WorkloadSpec
    events: list[ExecutionEvent]
    scheduler_job: SchedulerJob | None
    allocations: list[AllocationObservation]
    experiment_id: str | None
    request_id: str | None = None
    telemetry: dict[str, Any] | None = None
    verification: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _validate_strings(values: list[str], label: str) -> None:
    if len(values) > 4096 or not all(isinstance(value, str) for value in values):
        raise ValueError(f"{label} must be a bounded string list")
