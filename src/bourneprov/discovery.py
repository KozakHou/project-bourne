"""Reusable compute-site discovery orchestration and inventory queries."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from . import __version__
from .discovery_providers import (
    DiscoveryProvider,
    DiscoveryRequest,
    DiscoveryState,
    ProviderOutput,
    default_providers,
    run_bounded_command,
)
from .ids import new_ulid
from .inventory_models import (
    Capability,
    DiscoveryEvidence,
    DiscoveredExecutionContext,
    InventorySnapshot,
    ProviderResult,
)
from .inventory_storage import InventoryStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def discover_site(
    store: InventoryStore,
    *,
    cwd: Path | None = None,
    environment: Mapping[str, str] | None = None,
    providers: Sequence[DiscoveryProvider] | None = None,
) -> InventorySnapshot:
    """Run isolated read-only providers and persist one immutable snapshot."""

    snapshot_id = new_ulid()
    captured_at = _utc_now()
    working_directory = (cwd or Path.cwd()).resolve(strict=False)
    request = DiscoveryRequest(
        snapshot_id=snapshot_id,
        cwd=working_directory,
        environment=os.environ if environment is None else environment,
        store=store,
        runner=run_bounded_command,
    )
    state = DiscoveryState()
    results: list[ProviderResult] = []
    selected = list(default_providers() if providers is None else providers)
    names = [provider.name for provider in selected]
    if len(names) != len(set(names)):
        raise ValueError("discovery provider names must be unique")

    for provider in selected:
        started_at = _utc_now()
        started_monotonic = time.monotonic()
        try:
            output = provider.discover(request, state)
        except Exception as exc:
            output = ProviderOutput(
                status="error",
                diagnostic=f"provider raised {type(exc).__name__}",
            )
        ended_at = _utc_now()
        state.merge(output)
        results.append(
            ProviderResult(
                id=new_ulid(), snapshot_id=snapshot_id, provider=provider.name,
                status=output.status, started_at=started_at, ended_at=ended_at,
                duration_seconds=max(0.0, time.monotonic() - started_monotonic),
                diagnostic=output.diagnostic, truncated=output.truncated,
                metadata=output.metadata,
            )
        )

    snapshot = InventorySnapshot(
        id=snapshot_id,
        captured_at=captured_at,
        working_directory=str(working_directory),
        site_label=None,
        metadata={
            "bourne_version": __version__,
            "schema_version": 4,
            "observation_scope": "current_identity_authorized_surface",
        },
        identity=state.identity,
        targets=sorted(state.targets, key=lambda item: (item.role, item.kind, item.name, item.id)),
        storage=sorted(state.storage, key=lambda item: (item.path, item.id)),
        schedulers=sorted(state.schedulers, key=lambda item: (item.family, item.id)),
        execution_contexts=sorted(
            state.contexts, key=lambda item: (item.kind, item.name, item.id)
        ),
        capabilities=sorted(
            state.capabilities,
            key=lambda item: (item.name, item.locator or "", item.id),
        ),
        evidence=sorted(
            state.evidence,
            key=lambda item: (item.subject_type, item.subject_id, item.id),
        ),
        providers=sorted(results, key=lambda item: item.provider),
    )
    store.save(snapshot)
    return snapshot


@dataclass(frozen=True)
class CapabilityMatch:
    capability: Capability
    context: DiscoveredExecutionContext
    evidence: list[DiscoveryEvidence]

    def to_dict(self) -> dict[str, object]:
        from dataclasses import asdict

        return {
            "capability": asdict(self.capability),
            "context": asdict(self.context),
            "evidence": [asdict(item) for item in self.evidence],
        }


def find_capabilities(snapshot: InventorySnapshot, name: str) -> list[CapabilityMatch]:
    """Return every exact-name match without ranking or selecting a winner."""

    contexts = {item.id: item for item in snapshot.execution_contexts}
    evidence: dict[str, list[DiscoveryEvidence]] = {}
    for item in snapshot.evidence:
        if item.subject_type == "capability":
            evidence.setdefault(item.subject_id, []).append(item)
    return [
        CapabilityMatch(item, contexts[item.context_id], evidence.get(item.id, []))
        for item in snapshot.capabilities
        if item.name == name and item.context_id in contexts
    ]
