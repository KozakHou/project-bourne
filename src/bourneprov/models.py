"""Explicit, framework-independent provenance models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


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
    schema_version: int = 1

    @property
    def argv(self) -> list[str]:
        return [self.command, *self.arguments]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
