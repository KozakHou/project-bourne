"""Bounded, non-executable scheduler worker result bundles."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .execution_outcomes import ExecutionTelemetrySummary, VerificationRun
from .models import (
    Artifact,
    ExecutionContext,
    Experiment,
    ExperimentLineage,
    GitProvenance,
    SystemProvenance,
)
from .workload_models import AllocationObservation

RELEASED_V04_RESULT_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 2
MAX_RESULT_BUNDLE_BYTES = 32 * 1024 * 1024
MAX_RESULT_ARTIFACTS = 4096
MAX_RESULT_LINEAGE = 16


class WorkerResultError(ValueError):
    pass


@dataclass(frozen=True)
class WorkerResult:
    execution_id: str
    state: str
    created_at: str
    experiment: Experiment | None
    artifacts: list[Artifact]
    lineage: list[ExperimentLineage]
    allocation: AllocationObservation | None
    preflight: dict[str, Any]
    error: str | None = None
    request_id: str | None = None
    telemetry: ExecutionTelemetrySummary | None = None
    verification: VerificationRun | None = None
    protocol_version: int = RESULT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        from dataclasses import asdict

        value = {
            "schema_version": self.protocol_version,
            "execution_id": self.execution_id,
            "state": self.state,
            "created_at": self.created_at,
            "experiment": None if self.experiment is None else self.experiment.to_dict(),
            "artifacts": [asdict(item) for item in self.artifacts],
            "lineage": [asdict(item) for item in self.lineage],
            "allocation": None if self.allocation is None else asdict(self.allocation),
            "preflight": self.preflight,
            "error": self.error,
        }
        if self.protocol_version >= 2:
            value.update(
                {
                    "request_id": self.request_id,
                    "telemetry": (
                        None if self.telemetry is None else self.telemetry.to_dict()
                    ),
                    "verification": (
                        None
                        if self.verification is None
                        else self.verification.to_dict()
                    ),
                }
            )
        return value


def encode_worker_result(result: WorkerResult) -> bytes:
    raw = json.dumps(
        result.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    if len(raw) > MAX_RESULT_BUNDLE_BYTES:
        raise WorkerResultError(
            f"worker result exceeds {MAX_RESULT_BUNDLE_BYTES} bytes"
        )
    return raw


def load_worker_result(path: Path, execution_id: str) -> WorkerResult:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise WorkerResultError(f"worker result is unavailable: {exc}") from exc
    if size > MAX_RESULT_BUNDLE_BYTES:
        raise WorkerResultError("worker result exceeds the size limit")
    try:
        raw = path.read_bytes()
        if len(raw) > MAX_RESULT_BUNDLE_BYTES:
            raise WorkerResultError("worker result exceeds the size limit")
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkerResultError(f"worker result is not valid JSON: {exc}") from exc
    return parse_worker_result(value, execution_id)


def parse_worker_result(value: object, execution_id: str) -> WorkerResult:
    if not isinstance(value, dict):
        raise WorkerResultError("worker result must be a JSON object")
    protocol_version = value.get("schema_version")
    if protocol_version not in {
        RELEASED_V04_RESULT_SCHEMA_VERSION,
        RESULT_SCHEMA_VERSION,
    }:
        raise WorkerResultError("unsupported worker result schema")
    if value.get("execution_id") != execution_id:
        raise WorkerResultError("worker result execution ID does not match")
    state = _string(value, "state", 64)
    if state not in {
        "completed", "failed", "interrupted", "preflight_failed",
        "collection_failed", "cancelled", "unknown",
    }:
        raise WorkerResultError("worker result state is invalid")
    created_at = _string(value, "created_at", 128)
    error = value.get("error")
    if error is not None and (not isinstance(error, str) or len(error) > 4096):
        raise WorkerResultError("worker result error is invalid")
    preflight = value.get("preflight")
    if not isinstance(preflight, dict) or not _bounded_json(preflight):
        raise WorkerResultError("worker preflight evidence is invalid")
    raw_artifacts = value.get("artifacts")
    raw_lineage = value.get("lineage")
    if not isinstance(raw_artifacts, list) or len(raw_artifacts) > MAX_RESULT_ARTIFACTS:
        raise WorkerResultError("worker artifact list is invalid")
    if not isinstance(raw_lineage, list) or len(raw_lineage) > MAX_RESULT_LINEAGE:
        raise WorkerResultError("worker lineage list is invalid")
    try:
        artifacts = [Artifact(**_object(item, "artifact")) for item in raw_artifacts]
        lineage = [
            ExperimentLineage(**_object(item, "lineage")) for item in raw_lineage
        ]
        experiment = _experiment(value.get("experiment"))
        allocation = _allocation(value.get("allocation"), execution_id)
    except (TypeError, KeyError, ValueError) as exc:
        raise WorkerResultError(f"worker result structure is invalid: {exc}") from exc
    if experiment is None and (artifacts or lineage):
        raise WorkerResultError("artifacts and lineage require an experiment")
    if experiment is not None:
        if any(item.experiment_id != experiment.id for item in artifacts):
            raise WorkerResultError("artifact experiment IDs do not match")
        if any(item.child_experiment_id != experiment.id for item in lineage):
            raise WorkerResultError("lineage child experiment IDs do not match")
        expected = "completed" if experiment.status == "completed" else experiment.status
        if state != expected:
            raise WorkerResultError("worker state does not match experiment status")
    elif state not in {"preflight_failed", "collection_failed", "cancelled", "unknown"}:
        raise WorkerResultError("a terminal workload state requires an experiment")
    request_id: str | None = None
    telemetry: ExecutionTelemetrySummary | None = None
    verification: VerificationRun | None = None
    if protocol_version == RESULT_SCHEMA_VERSION:
        raw_request_id = value.get("request_id")
        if not isinstance(raw_request_id, str) or not raw_request_id:
            raise WorkerResultError("worker result request ID is invalid")
        request_id = raw_request_id
        try:
            raw_telemetry = value.get("telemetry")
            telemetry = (
                None
                if raw_telemetry is None
                else ExecutionTelemetrySummary.from_dict(
                    _object(raw_telemetry, "telemetry")
                )
            )
            raw_verification = value.get("verification")
            verification = (
                None
                if raw_verification is None
                else VerificationRun.from_dict(
                    _object(raw_verification, "verification")
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkerResultError(f"worker outcome structure is invalid: {exc}") from exc
        if experiment is None:
            if telemetry is not None or verification is not None:
                raise WorkerResultError("worker outcomes require an experiment")
        else:
            if verification is None:
                raise WorkerResultError("version-2 worker result requires verification")
            for outcome in (telemetry, verification):
                if outcome is not None and (
                    outcome.request_id != request_id
                    or outcome.execution_id != execution_id
                    or outcome.experiment_id != experiment.id
                ):
                    raise WorkerResultError("worker outcome relationships do not match")
    return WorkerResult(
        execution_id=execution_id, state=state, created_at=created_at,
        experiment=experiment, artifacts=artifacts, lineage=lineage,
        allocation=allocation, preflight=preflight, error=error,
        request_id=request_id,
        telemetry=telemetry,
        verification=verification,
        protocol_version=protocol_version,
    )


def _experiment(value: object) -> Experiment | None:
    if value is None:
        return None
    item = _object(value, "experiment")
    required_strings = (
        "id", "status", "command", "working_directory", "started_at",
        "ended_at", "stdout", "stderr",
    )
    if not all(isinstance(item.get(name), str) for name in required_strings):
        raise WorkerResultError("experiment string fields are invalid")
    arguments = item.get("arguments")
    if (
        not isinstance(arguments, list)
        or len(arguments) > 4096
        or not all(isinstance(argument, str) for argument in arguments)
    ):
        raise WorkerResultError("experiment arguments are invalid")
    if item["status"] not in {"completed", "failed", "interrupted"}:
        raise WorkerResultError("experiment status is invalid")
    duration = item.get("duration_seconds")
    exit_code = item.get("exit_code")
    if (
        isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
        or isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
    ):
        raise WorkerResultError("experiment process fields are invalid")
    return Experiment(
        id=item["id"], schema_version=item.get("schema_version", 2),
        status=item["status"], command=item["command"],
        arguments=arguments, working_directory=item["working_directory"],
        started_at=item["started_at"], ended_at=item["ended_at"],
        duration_seconds=item["duration_seconds"], exit_code=item["exit_code"],
        stdout=item["stdout"], stderr=item["stderr"],
        git=GitProvenance.from_dict(item["git"]),
        system=SystemProvenance.from_dict(item["system"]),
        execution_context=ExecutionContext.from_dict(item["execution_context"]),
    )


def _allocation(value: object, execution_id: str) -> AllocationObservation | None:
    if value is None:
        return None
    item = _object(value, "allocation")
    if item.get("execution_id") != execution_id:
        raise WorkerResultError("allocation execution ID does not match")
    resources = item.get("resources")
    hosts = item.get("hosts")
    evidence = item.get("evidence", {})
    if (
        not isinstance(resources, dict)
        or not isinstance(hosts, list)
        or not all(isinstance(host, str) and len(host) <= 1024 for host in hosts)
        or not isinstance(evidence, dict)
        or not _bounded_json(resources)
        or not _bounded_json(evidence)
    ):
        raise WorkerResultError("allocation evidence is invalid")
    return AllocationObservation(
        id=item["id"], execution_id=execution_id,
        observed_at=item["observed_at"], resources=resources,
        hosts=hosts, evidence=evidence,
    )


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not _bounded_json(value):
        raise WorkerResultError(f"{label} must be a bounded JSON object")
    return value


def _string(value: dict[str, Any], key: str, limit: int) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item or len(item) > limit:
        raise WorkerResultError(f"worker result {key} is invalid")
    return item


def _bounded_json(value: object, *, depth: int = 0) -> bool:
    if depth > 12:
        return False
    if value is None or isinstance(value, (bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, str):
        return len(value) <= MAX_RESULT_BUNDLE_BYTES
    if isinstance(value, list):
        return len(value) <= MAX_RESULT_ARTIFACTS and all(
            _bounded_json(item, depth=depth + 1) for item in value
        )
    if isinstance(value, dict):
        return len(value) <= 256 and all(
            isinstance(key, str)
            and len(key) <= 256
            and _bounded_json(item, depth=depth + 1)
            for key, item in value.items()
        )
    return False
