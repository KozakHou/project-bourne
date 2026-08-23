"""Generic, immutable compute-site discovery records."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

ProviderStatus = Literal["complete", "unavailable", "partial", "error", "timeout"]


@dataclass(frozen=True)
class CurrentIdentity:
    id: str
    snapshot_id: str
    username: str | None
    uid: int | None
    primary_gid: int | None
    groups: list[dict[str, Any]] = field(default_factory=list)
    home: str | None = None
    provider: str = "identity"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveredTarget:
    """An observed access target or abstract execution-target class."""

    id: str
    snapshot_id: str
    parent_target_id: str | None
    kind: str
    role: str
    name: str
    locator: str | None
    state: str
    visible: bool | None
    authorization: str
    provider: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageResource:
    id: str
    snapshot_id: str
    target_id: str | None
    path: str
    role_hints: list[str]
    exists: bool | None
    readable: bool | None
    writable: bool | None
    searchable: bool | None
    mount_point: str | None
    filesystem_type: str | None
    mount_read_only: bool | None
    provider: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchedulerResource:
    id: str
    snapshot_id: str
    access_target_id: str | None
    family: str
    state: str
    provider: str
    current_allocation: dict[str, Any] = field(default_factory=dict)
    execution_target_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveredExecutionContext:
    """A candidate context, distinct from a recorded experiment context."""

    id: str
    snapshot_id: str
    target_id: str | None
    context_key: str
    kind: str
    name: str
    locator: str | None
    state: str
    provider: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Capability:
    id: str
    snapshot_id: str
    context_id: str
    kind: str
    name: str
    locator: str | None
    observation_state: str
    provider: str
    classifications: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveryEvidence:
    id: str
    snapshot_id: str
    subject_type: str
    subject_id: str
    provider: str
    evidence_type: str
    observed_now: bool
    historical_only: bool
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderResult:
    id: str
    snapshot_id: str
    provider: str
    status: ProviderStatus
    started_at: str
    ended_at: str
    duration_seconds: float
    diagnostic: str | None = None
    truncated: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InventorySnapshot:
    id: str
    captured_at: str
    working_directory: str
    site_label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    identity: CurrentIdentity | None = None
    targets: list[DiscoveredTarget] = field(default_factory=list)
    storage: list[StorageResource] = field(default_factory=list)
    schedulers: list[SchedulerResource] = field(default_factory=list)
    execution_contexts: list[DiscoveredExecutionContext] = field(default_factory=list)
    capabilities: list[Capability] = field(default_factory=list)
    evidence: list[DiscoveryEvidence] = field(default_factory=list)
    providers: list[ProviderResult] = field(default_factory=list)

    @property
    def current_target(self) -> DiscoveredTarget | None:
        return next((target for target in self.targets if target.role == "access_target"), None)

    @property
    def execution_targets(self) -> list[DiscoveredTarget]:
        return [target for target in self.targets if target.role != "access_target"]

    def to_dict(self) -> dict[str, Any]:
        """Return a stable structured representation for APIs and JSON output."""

        return {
            "snapshot": {
                "id": self.id,
                "captured_at": self.captured_at,
                "site_label": self.site_label,
                "working_directory": self.working_directory,
                "metadata": self.metadata,
            },
            "identity": None if self.identity is None else asdict(self.identity),
            "current_target": (
                None if self.current_target is None else asdict(self.current_target)
            ),
            "storage": [asdict(item) for item in self.storage],
            "scheduler": [asdict(item) for item in self.schedulers],
            "execution_targets": [asdict(item) for item in self.execution_targets],
            "execution_contexts": [asdict(item) for item in self.execution_contexts],
            "capabilities": [asdict(item) for item in self.capabilities],
            "evidence": [asdict(item) for item in self.evidence],
            "providers": [asdict(item) for item in self.providers],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "InventorySnapshot":
        """Decode the stable API representation used by typed remote discovery."""

        snapshot = value.get("snapshot")
        if not isinstance(snapshot, dict):
            raise ValueError("inventory snapshot envelope is missing")
        targets: list[DiscoveredTarget] = []
        current = value.get("current_target")
        if current is not None:
            targets.append(DiscoveredTarget(**current))
        targets.extend(DiscoveredTarget(**item) for item in value.get("execution_targets", []))
        identity = value.get("identity")
        return cls(
            id=snapshot["id"], captured_at=snapshot["captured_at"],
            working_directory=snapshot["working_directory"],
            site_label=snapshot.get("site_label"),
            metadata=dict(snapshot.get("metadata", {})),
            identity=None if identity is None else CurrentIdentity(**identity),
            targets=targets,
            storage=[StorageResource(**item) for item in value.get("storage", [])],
            schedulers=[SchedulerResource(**item) for item in value.get("scheduler", [])],
            execution_contexts=[
                DiscoveredExecutionContext(**item)
                for item in value.get("execution_contexts", [])
            ],
            capabilities=[Capability(**item) for item in value.get("capabilities", [])],
            evidence=[DiscoveryEvidence(**item) for item in value.get("evidence", [])],
            providers=[ProviderResult(**item) for item in value.get("providers", [])],
        )
