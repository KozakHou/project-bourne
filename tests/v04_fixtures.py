from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

from bourneprov.ids import new_ulid
from bourneprov.inventory_models import (
    Capability,
    DiscoveredExecutionContext,
    DiscoveredTarget,
    InventorySnapshot,
    SchedulerResource,
)
from bourneprov.workload import utc_now


def inventory_snapshot(
    root: Path,
    *,
    scheduler_families: tuple[str, ...] = (),
    target_names: tuple[str, ...] | None = None,
    gpu_count: int = 0,
    executable: str | None = None,
    historical_executable: str | None = None,
) -> InventorySnapshot:
    snapshot_id = new_ulid()
    access = DiscoveredTarget(
        id=new_ulid(), snapshot_id=snapshot_id, parent_target_id=None,
        kind="host", role="access_target", name="fixture-host",
        locator="local://fixture-host", state="observed", visible=True,
        authorization="observed-authorized", provider="fixture",
        metadata={
            "system": {
                "gpus": [
                    {"index": str(index), "name": "fixture-gpu"}
                    for index in range(gpu_count)
                ]
            }
        },
    )
    current = DiscoveredExecutionContext(
        id=new_ulid(), snapshot_id=snapshot_id, target_id=access.id,
        context_key="current", kind="system", name="current environment",
        locator=access.locator, state="active", provider="fixture",
    )
    capabilities: list[Capability] = []
    if executable is not None:
        capabilities.append(
            Capability(
                id=new_ulid(), snapshot_id=snapshot_id, context_id=current.id,
                kind="executable", name=executable, locator=f"/bin/{executable}",
                observation_state="observed", provider="fixture",
            )
        )
    if historical_executable is not None:
        capabilities.append(
            Capability(
                id=new_ulid(), snapshot_id=snapshot_id, context_id=current.id,
                kind="executable", name=historical_executable,
                locator=f"/retired/{historical_executable}",
                observation_state="historical", provider="fixture",
            )
        )
    targets = [access]
    schedulers: list[SchedulerResource] = []
    names = target_names or tuple(f"{family}-target" for family in scheduler_families)
    for index, family in enumerate(scheduler_families):
        target = DiscoveredTarget(
            id=new_ulid(), snapshot_id=snapshot_id, parent_target_id=access.id,
            kind="scheduler_target_class", role="execution_target_class",
            name=names[index], locator=f"{family}://fixture/{names[index]}",
            state="up", visible=True, authorization="unknown", provider="fixture",
            metadata=(
                {
                    "scheduler": "slurm", "visible_nodes": "4",
                    "cpus_per_node": "32", "generic_resources": "gpu:a100:4",
                }
                if family == "slurm"
                else {
                    "scheduler": "pbs", "resources_max.ncpus": "64",
                    "resources_max.mem": "512gb",
                }
            ),
        )
        targets.append(target)
        schedulers.append(
            SchedulerResource(
                id=new_ulid(), snapshot_id=snapshot_id,
                access_target_id=access.id, family=family, state="observed",
                provider="fixture", execution_target_ids=[target.id],
            )
        )
    return InventorySnapshot(
        id=snapshot_id, captured_at=utc_now(), working_directory=str(root.resolve()),
        metadata={"schema_version": 4}, targets=targets,
        schedulers=schedulers, execution_contexts=[current],
        capabilities=capabilities,
    )
