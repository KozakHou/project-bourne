"""Durable site identity and evidence-bearing site policy models."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Any, Literal

SiteKind = Literal["local", "remote_ssh"]
SiteEvidenceKind = Literal[
    "observed_now",
    "site_declared",
    "user_declared",
    "historical",
    "inferred",
    "unknown",
]
PolicyInterpretation = Literal["hard_constraint", "advisory", "unresolved"]

_SITE_KINDS = {"local", "remote_ssh"}
_EVIDENCE_KINDS = {
    "observed_now", "site_declared", "user_declared", "historical",
    "inferred", "unknown",
}
_INTERPRETATIONS = {"hard_constraint", "advisory", "unresolved"}
_SITE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")


@dataclass(frozen=True)
class Site:
    """One local or SSH access context; authentication remains in OpenSSH."""

    id: str
    name: str
    kind: SiteKind
    created_at: str
    ssh_host: str | None = None
    ssh_username: str | None = None
    ssh_port: int | None = None
    scheduler_hint: str | None = None
    local_project_root: str | None = None
    remote_project_root: str | None = None
    remote_worker_path: str | None = None

    def __post_init__(self) -> None:
        if not self.id or not _SITE_NAME.fullmatch(self.name):
            raise ValueError("site ID and a safe site name are required")
        if self.kind not in _SITE_KINDS:
            raise ValueError(f"unsupported site kind: {self.kind}")
        if self.scheduler_hint not in {None, "slurm", "pbs", "none", "auto"}:
            raise ValueError("scheduler hint must be slurm, pbs, none, or auto")
        if self.ssh_port is not None and (
            isinstance(self.ssh_port, bool)
            or not isinstance(self.ssh_port, int)
            or not 1 <= self.ssh_port <= 65535
        ):
            raise ValueError("SSH port must be between 1 and 65535")
        if self.kind == "remote_ssh" and not self.ssh_host:
            raise ValueError("remote SSH sites require an SSH host or alias")
        if self.kind == "local" and any(
            value is not None
            for value in (self.ssh_host, self.ssh_username, self.ssh_port)
        ):
            raise ValueError("local sites cannot contain SSH connection fields")
        if self.kind == "local" and any(
            value is not None
            for value in (self.remote_project_root, self.remote_worker_path)
        ):
            raise ValueError("local sites cannot contain remote worker fields")
        for value, label in (
            (self.ssh_host, "SSH host"),
            (self.ssh_username, "SSH username"),
            (self.remote_worker_path, "remote worker path"),
        ):
            if value is not None and (
                not value or any(character in value for character in "\r\n\0")
            ):
                raise ValueError(f"{label} is invalid")
        if self.kind == "remote_ssh" and self.remote_project_root is not None:
            if (
                not self.remote_project_root.startswith("/")
                or ".." in PurePosixPath(self.remote_project_root).parts
            ):
                raise ValueError("remote project root must be an absolute POSIX path")

    @property
    def ssh_destination(self) -> str:
        if self.kind != "remote_ssh" or self.ssh_host is None:
            raise ValueError("local sites do not have an SSH destination")
        return (
            self.ssh_host
            if self.ssh_username is None
            else f"{self.ssh_username}@{self.ssh_host}"
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Site":
        allowed = {field.name for field in __import__("dataclasses").fields(cls)}
        extra = set(value) - allowed
        if extra:
            raise ValueError(f"unsupported site fields: {', '.join(sorted(extra))}")
        return cls(**value)


@dataclass(frozen=True)
class SitePolicyClaim:
    """A structured policy assertion whose provenance is never discarded."""

    id: str
    site_id: str
    subject: str
    property: str
    value: Any
    evidence_kind: SiteEvidenceKind
    interpretation_status: PolicyInterpretation
    source_identity: str
    created_at: str
    source_identifier: str | None = None
    source_url: str | None = None
    retrieved_at: str | None = None
    document_date: str | None = None
    content_digest: str | None = None

    def __post_init__(self) -> None:
        if not all(
            isinstance(item, str) and item
            for item in (
                self.id, self.site_id, self.subject, self.property,
                self.source_identity, self.created_at,
            )
        ):
            raise ValueError("policy identity, subject, property, and source are required")
        if self.evidence_kind not in _EVIDENCE_KINDS:
            raise ValueError(f"unsupported policy evidence kind: {self.evidence_kind}")
        if self.interpretation_status not in _INTERPRETATIONS:
            raise ValueError(
                f"unsupported policy interpretation: {self.interpretation_status}"
            )
        if (
            self.interpretation_status == "hard_constraint"
            and self.evidence_kind not in {"site_declared", "user_declared"}
        ):
            raise ValueError(
                "only site- or user-declared policy can be a hard constraint"
            )
        if self.content_digest is not None and not re.fullmatch(
            r"sha256:[0-9a-f]{64}", self.content_digest
        ):
            raise ValueError("content digest must use canonical sha256:<hex> form")

    @property
    def is_hard(self) -> bool:
        return self.interpretation_status == "hard_constraint"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SitePolicyClaim":
        return cls(**value)
