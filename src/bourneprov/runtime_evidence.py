"""Versioned, execution-scoped runtime and termination evidence."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any

from .ids import new_ulid
from .workload import utc_now

RUNTIME_EVIDENCE_VERSION = 1
_COVERAGE = {
    "observed", "partially_observed", "unavailable", "unsupported", "unknown"
}
_PHASES = {"preflight", "launch", "running", "scheduler", "collection", "verification"}
_OUTCOMES = {
    "completed", "preflight_failed", "launch_failed", "running_then_failed",
    "terminated_by_signal", "scheduler_cancelled", "scheduler_timeout",
    "out_of_memory", "node_failure", "result_bundle_partial", "result_bundle_missing",
    "telemetry_unavailable", "verification_failed", "unknown",
}


@dataclass(frozen=True)
class RuntimeEvidenceGroup:
    coverage: str
    source: str
    metrics: dict[str, Any] = field(default_factory=dict)
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.coverage, str) or self.coverage not in _COVERAGE:
            raise ValueError(f"unsupported runtime evidence coverage: {self.coverage}")
        if not isinstance(self.source, str) or not self.source or len(self.source) > 256:
            raise ValueError("runtime evidence source must be bounded")
        if self.diagnostic is not None and (
            not isinstance(self.diagnostic, str) or len(self.diagnostic) > 4096
        ):
            raise ValueError("runtime evidence diagnostic exceeds the safety limit")
        if not isinstance(self.metrics, dict) or not _bounded_json(self.metrics):
            raise ValueError("runtime evidence metrics must be bounded JSON")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeEvidenceGroup":
        if set(value) - {"coverage", "source", "metrics", "diagnostic"}:
            raise ValueError("runtime evidence group contains unknown fields")
        return cls(
            coverage=value["coverage"], source=value["source"],
            metrics=dict(value.get("metrics", {})),
            diagnostic=value.get("diagnostic"),
        )


@dataclass(frozen=True)
class RuntimeEvidence:
    id: str
    execution_id: str
    experiment_id: str | None
    observed_at: str
    process: RuntimeEvidenceGroup
    allocation: RuntimeEvidenceGroup
    cpu: RuntimeEvidenceGroup
    memory: RuntimeEvidenceGroup
    io: RuntimeEvidenceGroup
    gpu: RuntimeEvidenceGroup
    environment: RuntimeEvidenceGroup
    schema_version: int = RUNTIME_EVIDENCE_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RUNTIME_EVIDENCE_VERSION:
            raise ValueError("unsupported runtime evidence version")
        if not all(
            isinstance(item, str) and item
            for item in (self.id, self.execution_id, self.observed_at)
        ):
            raise ValueError("runtime evidence identity is required")
        if self.experiment_id is not None and not isinstance(self.experiment_id, str):
            raise ValueError("runtime evidence experiment identity is invalid")
        if not all(
            isinstance(getattr(self, name), RuntimeEvidenceGroup)
            for name in ("process", "allocation", "cpu", "memory", "io", "gpu", "environment")
        ):
            raise ValueError("runtime evidence groups must be typed")

    @property
    def coverage(self) -> dict[str, str]:
        return {
            name: getattr(self, name).coverage
            for name in ("process", "allocation", "cpu", "memory", "io", "gpu", "environment")
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeEvidence":
        if set(value) - {
            "id", "execution_id", "experiment_id", "observed_at", "process",
            "allocation", "cpu", "memory", "io", "gpu", "environment",
            "schema_version",
        }:
            raise ValueError("runtime evidence contains unknown fields")
        return cls(
            id=value["id"], execution_id=value["execution_id"],
            experiment_id=value.get("experiment_id"),
            observed_at=value["observed_at"],
            process=RuntimeEvidenceGroup.from_dict(value["process"]),
            allocation=RuntimeEvidenceGroup.from_dict(value["allocation"]),
            cpu=RuntimeEvidenceGroup.from_dict(value["cpu"]),
            memory=RuntimeEvidenceGroup.from_dict(value["memory"]),
            io=RuntimeEvidenceGroup.from_dict(value["io"]),
            gpu=RuntimeEvidenceGroup.from_dict(value["gpu"]),
            environment=RuntimeEvidenceGroup.from_dict(value["environment"]),
            schema_version=value.get("schema_version", RUNTIME_EVIDENCE_VERSION),
        )


@dataclass(frozen=True)
class TerminationEvidence:
    phase: str
    outcome: str
    source: str
    exit_code: int | None = None
    signal: int | None = None
    scheduler_state: str | None = None
    result_evidence: str = "complete"
    telemetry_evidence: str = "unknown"
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.phase, str) or self.phase not in _PHASES:
            raise ValueError(f"unsupported termination phase: {self.phase}")
        if not isinstance(self.outcome, str) or self.outcome not in _OUTCOMES:
            raise ValueError(f"unsupported termination outcome: {self.outcome}")
        if not isinstance(self.source, str) or not self.source or len(self.source) > 256:
            raise ValueError("termination evidence source must be bounded")
        for label, value in (("exit code", self.exit_code), ("signal", self.signal)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                raise ValueError(f"termination {label} must be an integer")
        if self.scheduler_state is not None and not isinstance(self.scheduler_state, str):
            raise ValueError("termination scheduler state must be a string")
        if self.result_evidence not in {"complete", "partial", "missing", "unknown"}:
            raise ValueError("unsupported result evidence state")
        if self.telemetry_evidence not in {
            "observed", "partial", "unavailable", "unsupported", "unknown"
        }:
            raise ValueError("unsupported telemetry evidence state")
        if self.diagnostic is not None and (
            not isinstance(self.diagnostic, str) or len(self.diagnostic) > 4096
        ):
            raise ValueError("termination diagnostic exceeds the safety limit")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TerminationEvidence":
        if set(value) - {
            "phase", "outcome", "source", "exit_code", "signal",
            "scheduler_state", "result_evidence", "telemetry_evidence",
            "diagnostic",
        }:
            raise ValueError("termination evidence contains unknown fields")
        return cls(**value)


def unavailable_runtime_evidence(
    execution_id: str,
    *,
    experiment_id: str | None,
    allocation: RuntimeEvidenceGroup,
    environment: RuntimeEvidenceGroup,
    diagnostic: str,
) -> RuntimeEvidence:
    unavailable = RuntimeEvidenceGroup(
        coverage="unavailable", source="runtime_collector", diagnostic=diagnostic
    )
    return RuntimeEvidence(
        id=new_ulid(), execution_id=execution_id, experiment_id=experiment_id,
        observed_at=utc_now(), process=unavailable, allocation=allocation,
        cpu=unavailable, memory=unavailable, io=unavailable,
        gpu=RuntimeEvidenceGroup(
            coverage="unknown", source="runtime_collector",
            diagnostic="GPU telemetry was not established",
        ),
        environment=environment,
    )


def _bounded_json(value: object, *, depth: int = 0) -> bool:
    if depth > 10:
        return False
    if value is None or isinstance(value, (bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, str):
        return len(value) <= 16 * 1024
    if isinstance(value, list):
        return len(value) <= 4096 and all(_bounded_json(item, depth=depth + 1) for item in value)
    if isinstance(value, dict):
        return len(value) <= 256 and all(
            isinstance(key, str) and len(key) <= 256
            and _bounded_json(item, depth=depth + 1)
            for key, item in value.items()
        )
    return False
