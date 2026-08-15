"""Explicit, framework-independent provenance models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ArtifactExistenceState = Literal["present", "missing", "unknown"]
ArtifactCaptureStatus = Literal["complete", "unreadable", "unsupported", "changed"]


@dataclass(frozen=True)
class GitProvenance:
    available: bool
    repository_root: str | None = None
    commit_sha: str | None = None
    branch: str | None = None
    dirty: bool | None = None
    error: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "GitProvenance":
        return cls(**value)


@dataclass(frozen=True)
class SystemProvenance:
    operating_system: str
    os_version: str
    architecture: str
    hostname: str
    cpu: str | None
    gpu_available: bool
    gpus: list[dict[str, str]] = field(default_factory=list)
    nvidia_driver_version: str | None = None
    cuda_version: str | None = None
    cuda_version_source: str | None = None
    gpu_error: str | None = None
    collector_error: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SystemProvenance":
        return cls(**value)


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    started_at: str
    ended_at: str
    duration_seconds: float
    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ExecutionContext:
    """Small, safe observations about where a workload was launched."""

    requested_executable: str
    resolved_executable: str | None = None
    recorder_executable: str | None = None
    environment_hints: dict[str, str] = field(default_factory=dict)
    containerized: bool | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ExecutionContext":
        return cls(**value)


@dataclass(frozen=True)
class Artifact:
    """One captured filesystem artifact version associated with an experiment."""

    id: str
    experiment_id: str
    role: str
    original_path: str
    resolved_path: str
    existence_state: ArtifactExistenceState
    capture_status: ArtifactCaptureStatus
    sha256: str | None
    size_bytes: int | None
    modified_at: str | None
    captured_at: str
    capture_error: str | None = None


@dataclass(frozen=True)
class ExperimentLineage:
    """A directed semantic relationship between two experiments."""

    child_experiment_id: str
    parent_experiment_id: str
    relationship: str
    created_at: str


@dataclass(frozen=True)
class Experiment:
    id: str
    status: str
    command: str
    arguments: list[str]
    working_directory: str
    started_at: str
    ended_at: str
    duration_seconds: float
    exit_code: int
    stdout: str
    stderr: str
    git: GitProvenance
    system: SystemProvenance
    execution_context: ExecutionContext
    schema_version: int = 2

    @property
    def argv(self) -> list[str]:
        return [self.command, *self.arguments]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
