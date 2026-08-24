"""Durable site identity and evidence-bearing site policy models."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
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
PolicyScope = Literal[
    "global", "scheduler_class", "queue", "partition", "node_class", "account"
]

_SITE_KINDS = {"local", "remote_ssh"}
_EVIDENCE_KINDS = {
    "observed_now", "site_declared", "user_declared", "historical",
    "inferred", "unknown",
}
_INTERPRETATIONS = {"hard_constraint", "advisory", "unresolved"}
_POLICY_SCOPES = {
    "global", "scheduler_class", "queue", "partition", "node_class", "account"
}
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
        if self.scheduler_hint not in {None, "slurm", "pbs", "lsf", "none", "auto"}:
            raise ValueError("scheduler hint must be slurm, pbs, lsf, none, or auto")
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
class PolicyApplicability:
    """Typed scope identifying the resource shapes to which a claim applies."""

    scope: PolicyScope = "global"
    value: str | None = None

    def __post_init__(self) -> None:
        if self.scope not in _POLICY_SCOPES:
            raise ValueError(f"unsupported policy applicability scope: {self.scope}")
        if self.scope == "global":
            if self.value is not None:
                raise ValueError("global policy applicability cannot have a value")
        elif not isinstance(self.value, str) or not self.value or len(self.value) > 256:
            raise ValueError("scoped policy applicability requires a bounded value")
        if self.value is not None and any(character in self.value for character in "\r\n\0"):
            raise ValueError("policy applicability value is invalid")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "PolicyApplicability":
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
    applicability: PolicyApplicability = field(default_factory=PolicyApplicability)

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
        if not isinstance(self.applicability, PolicyApplicability):
            raise ValueError("policy applicability must use the typed scope model")
        if self.content_digest is not None and not re.fullmatch(
            r"sha256:[0-9a-f]{64}", self.content_digest
        ):
            raise ValueError("content digest must use canonical sha256:<hex> form")
        for item, label, maximum in (
            (self.id, "policy ID", 128),
            (self.site_id, "policy site ID", 128),
            (self.subject, "policy subject", 256),
            (self.property, "policy property", 256),
            (self.source_identity, "policy source identity", 512),
            (self.created_at, "policy creation time", 128),
            (self.source_identifier, "policy source identifier", 2048),
            (self.source_url, "policy source URL", 2048),
            (self.retrieved_at, "policy retrieval time", 128),
            (self.document_date, "policy document date", 128),
        ):
            if item is not None and (
                len(item) > maximum or any(character in item for character in "\r\n\0")
            ):
                raise ValueError(f"{label} is invalid or too long")
        _validate_policy_value(self.value)
        try:
            encoded_value = json.dumps(
                self.value, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
        except (RecursionError, TypeError, ValueError) as error:
            raise ValueError("policy value must be structured JSON data") from error
        if len(encoded_value.encode("utf-8")) > 16_384:
            raise ValueError("policy value exceeds the 16 KiB structured-data limit")

    @property
    def is_hard(self) -> bool:
        return self.interpretation_status == "hard_constraint"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SitePolicyClaim":
        document = dict(value)
        applicability = document.get("applicability")
        if isinstance(applicability, dict):
            document["applicability"] = PolicyApplicability.from_dict(applicability)
        return cls(**document)


def _validate_policy_value(value: Any, depth: int = 0) -> None:
    if depth > 8:
        raise ValueError("policy value exceeds the structured-data depth limit")
    if value is None or isinstance(value, (bool, int, float, str)):
        return
    if isinstance(value, list):
        if len(value) > 256:
            raise ValueError("policy value exceeds the structured-data item limit")
        for item in value:
            _validate_policy_value(item, depth + 1)
        return
    if isinstance(value, dict):
        if len(value) > 256 or not all(
            isinstance(key, str) and len(key) <= 256 for key in value
        ):
            raise ValueError("policy value has invalid or excessive structured keys")
        for item in value.values():
            _validate_policy_value(item, depth + 1)
        return
    raise ValueError("policy value must contain only structured JSON data")
