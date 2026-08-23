"""Shape-aware planning, environment, variant, and selection records."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ParameterClassification = Literal[
    "execution_only", "performance_tunable", "scientific_semantics", "unknown"
]
CandidateState = Literal[
    "viable", "hard_invalid", "unresolved", "policy_incompatible"
]

_PARAMETER_CLASSES = {
    "execution_only", "performance_tunable", "scientific_semantics", "unknown"
}
_CANDIDATE_STATES = {
    "viable", "hard_invalid", "unresolved", "policy_incompatible"
}
_ACTIVATION_KINDS = {"none", "module", "virtualenv", "conda", "spack"}
_MODULE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+/@-]{0,127}\Z")


def canonical_digest(value: object) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class ResourceShape:
    nodes: int | None = None
    cpus_per_node: int | None = None
    total_cpus: int | None = None
    mpi_ranks: int | None = None
    ranks_per_node: int | None = None
    threads_per_rank: int | None = None
    gpus: int | None = None
    gpus_per_node: int | None = None
    memory_bytes: int | None = None
    memory_per_node_bytes: int | None = None
    architecture: str | None = None
    node_class: str | None = None
    walltime_seconds: int | None = None
    scheduler_class: str | None = None
    placement: dict[str, Any] = field(default_factory=dict)
    evidence: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        for name in (
            "nodes", "cpus_per_node", "total_cpus", "mpi_ranks",
            "ranks_per_node", "threads_per_rank", "memory_bytes",
            "memory_per_node_bytes", "walltime_seconds",
        ):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise ValueError(f"resource shape {name} must be at least 1")
        for name in ("gpus", "gpus_per_node"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"resource shape {name} must be at least 0")
        if (
            self.nodes is not None
            and self.cpus_per_node is not None
            and self.total_cpus is not None
            and self.nodes * self.cpus_per_node != self.total_cpus
        ):
            raise ValueError("total CPUs must equal nodes times CPUs per node")
        if (
            self.nodes is not None
            and self.ranks_per_node is not None
            and self.mpi_ranks is not None
            and self.nodes * self.ranks_per_node != self.mpi_ranks
        ):
            raise ValueError("MPI ranks must equal nodes times ranks per node")
        if (
            self.nodes is not None
            and self.gpus_per_node is not None
            and self.gpus is not None
            and self.nodes * self.gpus_per_node != self.gpus
        ):
            raise ValueError("GPUs must equal nodes times GPUs per node")
        if (
            self.nodes is not None
            and self.memory_per_node_bytes is not None
            and self.memory_bytes is not None
            and self.nodes * self.memory_per_node_bytes != self.memory_bytes
        ):
            raise ValueError("memory must equal nodes times memory per node")
        if (
            self.mpi_ranks is not None
            and self.threads_per_rank is not None
            and self.total_cpus is not None
            and self.mpi_ranks * self.threads_per_rank != self.total_cpus
        ):
            raise ValueError("total CPUs must equal MPI ranks times threads per rank")

    @property
    def identity(self) -> str:
        return canonical_digest(self.to_dict())

    def value(self, name: str) -> Any:
        if name not in {
            "nodes", "cpus_per_node", "total_cpus", "mpi_ranks",
            "ranks_per_node", "threads_per_rank", "gpus", "gpus_per_node",
            "memory_bytes", "memory_per_node_bytes", "architecture",
            "node_class", "walltime_seconds", "scheduler_class",
        }:
            raise ValueError(f"unsupported resource reference: {name}")
        return getattr(self, name)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ResourceShape":
        return cls(**value)


@dataclass(frozen=True)
class EnvironmentActivation:
    kind: str = "none"
    names: tuple[str, ...] = ()
    prefix: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _ACTIVATION_KINDS:
            raise ValueError(f"unsupported environment activation: {self.kind}")
        if self.kind == "module":
            if not self.names or not all(_MODULE_NAME.fullmatch(item) for item in self.names):
                raise ValueError("module activation requires safe typed module names")
            if self.prefix is not None:
                raise ValueError("module activation does not use a prefix")
        elif self.kind in {"virtualenv", "conda", "spack"}:
            if not self.prefix or not self.prefix.startswith("/"):
                raise ValueError(f"{self.kind} activation requires an absolute prefix")
            if self.names:
                raise ValueError(f"{self.kind} activation does not use module names")
        elif self.names or self.prefix is not None:
            raise ValueError("none activation cannot carry values")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "names": list(self.names), "prefix": self.prefix}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "EnvironmentActivation":
        return cls(
            kind=value.get("kind", "none"),
            names=tuple(value.get("names", [])),
            prefix=value.get("prefix"),
        )


@dataclass(frozen=True)
class ResolvedEnvironment:
    context_id: str
    name: str
    kind: str
    state: str
    activation: EnvironmentActivation
    evidence: list[dict[str, Any]] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)

    @property
    def is_compatible(self) -> bool:
        return self.state == "compatible" and not self.unresolved

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["activation"] = self.activation.to_dict()
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ResolvedEnvironment":
        return cls(
            context_id=value["context_id"], name=value["name"], kind=value["kind"],
            state=value["state"],
            activation=EnvironmentActivation.from_dict(value["activation"]),
            evidence=list(value.get("evidence", [])),
            unresolved=list(value.get("unresolved", [])),
        )


@dataclass(frozen=True)
class CandidateReason:
    code: str
    message: str
    evidence_kind: str
    source_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PlanningCandidate:
    id: str
    resource_shape: ResourceShape
    environment: ResolvedEnvironment | None
    parameters: dict[str, Any]
    state: CandidateState
    reasons: list[CandidateReason]
    unresolved: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.state not in _CANDIDATE_STATES:
            raise ValueError(f"unsupported candidate state: {self.state}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "resource_shape": self.resource_shape.to_dict(),
            "environment": None if self.environment is None else self.environment.to_dict(),
            "parameters": self.parameters,
            "state": self.state,
            "reasons": [asdict(item) for item in self.reasons],
            "unresolved": self.unresolved,
        }


@dataclass(frozen=True)
class CandidateExploration:
    candidates: list[PlanningCandidate]
    generated_count: int
    theoretical_count: int
    hard_invalid_count: int
    viable_count: int
    truncated: bool
    coverage: str
    hard_pruned_count: int = 0
    explored_group_count: int = 0
    total_group_count: int = 0


@dataclass(frozen=True)
class CandidateSelectionSummary:
    id: str
    workload_id: str
    site_id: str
    created_at: str
    generated_count: int
    hard_invalid_count: int
    viable_count: int
    selected_candidate_id: str | None
    selected_candidate_summary: dict[str, Any] | None
    rejection_reasons: list[dict[str, Any]]
    selection_source: str
    selection_rationale: str | None
    unresolved_conditions: list[str]
    truncated: bool
    coverage: str
    hard_pruned_count: int = 0
    explored_group_count: int = 0
    total_group_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CandidateSelectionSummary":
        return cls(**value)


@dataclass(frozen=True)
class WorkloadVariant:
    id: str
    workload_id: str
    created_at: str
    original_path: str
    derived_path: str
    original_sha256: str
    derived_sha256: str
    changed_fields: list[dict[str, Any]]
    proposer: str
    classifications: dict[str, ParameterClassification]
    supporting_evidence: list[dict[str, Any]]
    approval: dict[str, Any]

    def __post_init__(self) -> None:
        if not all(value in _PARAMETER_CLASSES for value in self.classifications.values()):
            raise ValueError("workload variant has an unsupported parameter classification")
        if self.original_path == self.derived_path:
            raise ValueError("derived workload input must not replace the original path")
        for digest in (self.original_sha256, self.derived_sha256):
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError("variant content hashes must be canonical SHA-256 digests")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "WorkloadVariant":
        return cls(**value)
