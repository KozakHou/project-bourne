"""Low-overhead execution telemetry and deterministic evidence verification."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Any, Mapping, Sequence

from .execution_request import ExecutionRequest, VerificationCheckSpec
from .ids import new_ulid
from .models import Artifact, Experiment
from .workload_models import AllocationObservation, ExecutionPlan


@dataclass(frozen=True)
class ExecutionTelemetrySummary:
    id: str
    request_id: str
    execution_id: str
    experiment_id: str
    created_at: str
    state: str
    sources: tuple[str, ...]
    coverage: tuple[str, ...]
    wall_seconds: float | None
    stdout_bytes: int | None
    stderr_bytes: int | None
    known_input_artifact_bytes: int | None
    known_output_artifact_bytes: int | None
    scheduler_wait_seconds: float | None
    requested_resources: dict[str, int | None]
    allocated_resources: dict[str, Any] | None
    unavailable: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.state not in {"complete", "partial", "unavailable"}:
            raise ValueError(f"unsupported telemetry state: {self.state}")
        for name in (
            "wall_seconds",
            "stdout_bytes",
            "stderr_bytes",
            "known_input_artifact_bytes",
            "known_output_artifact_bytes",
            "scheduler_wait_seconds",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"telemetry {name} must not be negative")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionTelemetrySummary":
        required_strings = (
            "id", "request_id", "execution_id", "experiment_id", "created_at", "state"
        )
        sources = value.get("sources")
        coverage = value.get("coverage")
        unavailable = value.get("unavailable", [])
        requested = value.get("requested_resources")
        allocated = value.get("allocated_resources")
        if (
            not all(isinstance(value.get(name), str) for name in required_strings)
            or not _string_sequence(sources)
            or not _string_sequence(coverage)
            or not _string_sequence(unavailable)
            or not isinstance(requested, dict)
            or set(requested) - {
                "cpus", "gpus", "nodes", "mpi_ranks", "memory_bytes",
                "walltime_seconds",
            }
            or (allocated is not None and not isinstance(allocated, dict))
        ):
            raise ValueError("telemetry summary structure is invalid")
        return cls(
            id=value["id"],
            request_id=value["request_id"],
            execution_id=value["execution_id"],
            experiment_id=value["experiment_id"],
            created_at=value["created_at"],
            state=value["state"],
            sources=tuple(sources),
            coverage=tuple(coverage),
            wall_seconds=value.get("wall_seconds"),
            stdout_bytes=value.get("stdout_bytes"),
            stderr_bytes=value.get("stderr_bytes"),
            known_input_artifact_bytes=value.get("known_input_artifact_bytes"),
            known_output_artifact_bytes=value.get("known_output_artifact_bytes"),
            scheduler_wait_seconds=value.get("scheduler_wait_seconds"),
            requested_resources=dict(requested),
            allocated_resources=(
                None
                if value.get("allocated_resources") is None
                else dict(allocated)
            ),
            unavailable=tuple(unavailable),
        )


@dataclass(frozen=True)
class VerificationCheckResult:
    id: str
    verification_run_id: str
    ordinal: int
    check_type: str
    output_path: str
    state: str
    evidence: dict[str, Any]

    def __post_init__(self) -> None:
        if self.state not in {"passed", "failed", "unknown"}:
            raise ValueError(f"unsupported verification check state: {self.state}")
        if self.ordinal < 0:
            raise ValueError("verification check ordinal must not be negative")
        if self.check_type not in {
            "output_exists", "output_min_bytes", "output_sha256"
        }:
            raise ValueError(f"unsupported verification check: {self.check_type}")
        if not self.id or not self.verification_run_id or not self.output_path:
            raise ValueError("verification check identity and output path are required")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "VerificationCheckResult":
        if (
            not all(
                isinstance(value.get(name), str)
                for name in (
                    "id", "verification_run_id", "check_type", "output_path", "state"
                )
            )
            or isinstance(value.get("ordinal"), bool)
            or not isinstance(value.get("ordinal"), int)
            or not isinstance(value.get("evidence"), dict)
        ):
            raise ValueError("verification check structure is invalid")
        return cls(
            id=value["id"],
            verification_run_id=value["verification_run_id"],
            ordinal=value["ordinal"],
            check_type=value["check_type"],
            output_path=value["output_path"],
            state=value["state"],
            evidence=dict(value["evidence"]),
        )


@dataclass(frozen=True)
class VerificationRun:
    id: str
    request_id: str
    execution_id: str
    experiment_id: str
    aggregate_state: str
    evaluated_at: str
    source: str
    checks: tuple[VerificationCheckResult, ...]

    def __post_init__(self) -> None:
        if self.aggregate_state not in {
            "passed",
            "failed",
            "unknown",
            "not_requested",
        }:
            raise ValueError(
                f"unsupported verification aggregate state: {self.aggregate_state}"
            )
        if any(item.verification_run_id != self.id for item in self.checks):
            raise ValueError("verification checks must belong to their run")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "request_id": self.request_id,
            "execution_id": self.execution_id,
            "experiment_id": self.experiment_id,
            "aggregate_state": self.aggregate_state,
            "evaluated_at": self.evaluated_at,
            "source": self.source,
            "checks": [item.to_dict() for item in self.checks],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "VerificationRun":
        required = (
            "id", "request_id", "execution_id", "experiment_id",
            "aggregate_state", "evaluated_at", "source",
        )
        checks = value.get("checks", [])
        if (
            not all(isinstance(value.get(name), str) for name in required)
            or not isinstance(checks, list)
        ):
            raise ValueError("verification run structure is invalid")
        return cls(
            id=value["id"],
            request_id=value["request_id"],
            execution_id=value["execution_id"],
            experiment_id=value["experiment_id"],
            aggregate_state=value["aggregate_state"],
            evaluated_at=value["evaluated_at"],
            source=value["source"],
            checks=tuple(
                VerificationCheckResult.from_dict(item)
                for item in checks
            ),
        )


def build_telemetry_summary(
    request: ExecutionRequest,
    plan: ExecutionPlan,
    execution_id: str,
    experiment: Experiment,
    artifacts: Sequence[Artifact],
    allocation: AllocationObservation | None,
    *,
    scheduler_wait_seconds: float | None = None,
) -> ExecutionTelemetrySummary | None:
    """Summarize facts Bourne already observed; perform no sampling."""

    if request.telemetry_mode == "off":
        return None
    unavailable: list[str] = []
    input_total = _artifact_total(
        [item for item in artifacts if item.role == "input"]
    )
    output_total = _artifact_total(
        [item for item in artifacts if item.role == "output"]
    )
    if request.artifacts.inputs and input_total is None:
        unavailable.append("known_input_artifact_bytes")
    if request.artifacts.outputs and output_total is None:
        unavailable.append("known_output_artifact_bytes")
    if plan.backend in {"slurm", "pbs", "lsf"} and scheduler_wait_seconds is None:
        unavailable.append("scheduler_wait_seconds")
    allocated = None if allocation is None else dict(allocation.resources)
    if allocated is None:
        unavailable.append("allocated_resources")
    sources = ["bourne_experiment_record"]
    coverage = ["execution_wall_time", "captured_process_output"]
    if artifacts:
        sources.append("bourne_artifact_records")
        coverage.append("declared_artifact_capture")
    if allocation is not None:
        sources.append("bourne_compute_allocation_observation")
        coverage.append("allocated_resource_summary")
    if scheduler_wait_seconds is not None:
        sources.append("bourne_scheduler_lifecycle_events")
        coverage.append("scheduler_queue_interval")
    return ExecutionTelemetrySummary(
        id=new_ulid(),
        request_id=request.id,
        execution_id=execution_id,
        experiment_id=experiment.id,
        created_at=experiment.ended_at,
        state="partial" if unavailable else "complete",
        sources=tuple(sources),
        coverage=tuple(coverage),
        wall_seconds=experiment.duration_seconds,
        stdout_bytes=len(experiment.stdout.encode("utf-8")),
        stderr_bytes=len(experiment.stderr.encode("utf-8")),
        known_input_artifact_bytes=input_total,
        known_output_artifact_bytes=output_total,
        scheduler_wait_seconds=scheduler_wait_seconds,
        requested_resources=asdict(plan.requested_resources),
        allocated_resources=allocated,
        unavailable=tuple(unavailable),
    )


def evaluate_verification(
    request: ExecutionRequest,
    execution_id: str,
    experiment: Experiment,
    artifacts: Sequence[Artifact],
) -> VerificationRun:
    """Evaluate request checks exclusively from captured output Artifact records."""

    run_id = new_ulid()
    outputs: dict[str, Artifact] = {}
    for artifact in artifacts:
        if artifact.role == "output":
            outputs.setdefault(artifact.original_path, artifact)
    results = tuple(
        _evaluate_check(run_id, ordinal, check, outputs.get(check.path))
        for ordinal, check in enumerate(request.verification_checks)
    )
    states = {item.state for item in results}
    if not results:
        aggregate = "not_requested"
    elif "failed" in states:
        aggregate = "failed"
    elif "unknown" in states:
        aggregate = "unknown"
    else:
        aggregate = "passed"
    return VerificationRun(
        id=run_id,
        request_id=request.id,
        execution_id=execution_id,
        experiment_id=experiment.id,
        aggregate_state=aggregate,
        evaluated_at=experiment.ended_at,
        source="captured_output_artifact_records",
        checks=results,
    )


def add_scheduler_wait(
    summary: ExecutionTelemetrySummary,
    *,
    submitted_at: str,
    execution_started_at: str,
) -> ExecutionTelemetrySummary:
    """Add a timestamp-established queue interval without claiming utilization."""

    try:
        submitted = _timestamp(submitted_at)
        started = _timestamp(execution_started_at)
        seconds = (started - submitted).total_seconds()
    except (TypeError, ValueError):
        return summary
    if seconds < 0:
        return summary
    unavailable = tuple(
        item for item in summary.unavailable if item != "scheduler_wait_seconds"
    )
    sources = tuple(
        dict.fromkeys((*summary.sources, "bourne_scheduler_lifecycle_events"))
    )
    coverage = tuple(
        dict.fromkeys((*summary.coverage, "scheduler_queue_interval"))
    )
    return replace(
        summary,
        scheduler_wait_seconds=seconds,
        sources=sources,
        coverage=coverage,
        unavailable=unavailable,
        state="complete" if not unavailable else "partial",
    )


def _artifact_total(artifacts: Sequence[Artifact]) -> int | None:
    if not artifacts:
        return None
    if any(
        item.existence_state != "present"
        or item.capture_status != "complete"
        or item.size_bytes is None
        for item in artifacts
    ):
        return None
    return sum(item.size_bytes for item in artifacts if item.size_bytes is not None)


def _evaluate_check(
    run_id: str,
    ordinal: int,
    check: VerificationCheckSpec,
    artifact: Artifact | None,
) -> VerificationCheckResult:
    evidence: dict[str, Any] = {
        "evidence_source": "captured_output_artifact_record",
        "declared_output_path": check.path,
    }
    if artifact is None:
        state = "unknown"
        evidence["reason"] = "captured output artifact record is unavailable"
    else:
        evidence.update(
            {
                "artifact_id": artifact.id,
                "existence_state": artifact.existence_state,
                "capture_status": artifact.capture_status,
                "size_bytes": artifact.size_bytes,
                "sha256": artifact.sha256,
            }
        )
        state = _check_state(check, artifact, evidence)
    return VerificationCheckResult(
        id=new_ulid(),
        verification_run_id=run_id,
        ordinal=ordinal,
        check_type=check.type,
        output_path=check.path,
        state=state,
        evidence=evidence,
    )


def _check_state(
    check: VerificationCheckSpec,
    artifact: Artifact,
    evidence: dict[str, Any],
) -> str:
    if check.type == "output_exists":
        if artifact.existence_state == "present":
            return "passed"
        if artifact.existence_state == "missing":
            return "failed"
        evidence["reason"] = "artifact existence was not established"
        return "unknown"
    if artifact.existence_state == "missing":
        return "failed"
    if artifact.existence_state != "present" or artifact.capture_status != "complete":
        evidence["reason"] = "artifact capture is not complete"
        return "unknown"
    if check.type == "output_min_bytes":
        evidence["expected_min_bytes"] = check.min_bytes
        if artifact.size_bytes is None:
            evidence["reason"] = "artifact size is unavailable"
            return "unknown"
        return "passed" if artifact.size_bytes >= check.min_bytes else "failed"  # type: ignore[operator]
    evidence["expected_sha256"] = check.sha256
    if artifact.sha256 is None:
        evidence["reason"] = "artifact SHA-256 is unavailable"
        return "unknown"
    return "passed" if artifact.sha256.casefold() == check.sha256 else "failed"


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _string_sequence(value: object) -> bool:
    return isinstance(value, (list, tuple)) and all(
        isinstance(item, str) for item in value
    )
