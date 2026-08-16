from __future__ import annotations

from pathlib import Path
from typing import Mapping

from bourneprov.discovery_providers import DiscoveryRequest, DiscoveryState
from bourneprov.inventory_models import DiscoveredExecutionContext, DiscoveredTarget
from bourneprov.inventory_storage import InventoryStore

SNAPSHOT_ID = "01JINVENTORY00000000000000"
TARGET_ID = "01JTARGET00000000000000000"
CONTEXT_ID = "01JCONTEXT0000000000000000"


def request(
    root: Path,
    environment: Mapping[str, str] | None = None,
    *,
    runner=None,
    max_path_directories: int = 256,
    max_directory_entries: int = 50_000,
) -> DiscoveryRequest:
    values = {"PATH": "", "HOME": str(root)} if environment is None else environment
    kwargs = {}
    if runner is not None:
        kwargs["runner"] = runner
    return DiscoveryRequest(
        snapshot_id=SNAPSHOT_ID,
        cwd=root,
        environment=values,
        store=InventoryStore(root / "bourne.sqlite3"),
        max_path_directories=max_path_directories,
        max_directory_entries=max_directory_entries,
        **kwargs,
    )


def state() -> DiscoveryState:
    target = DiscoveredTarget(
        id=TARGET_ID,
        snapshot_id=SNAPSHOT_ID,
        parent_target_id=None,
        kind="host",
        role="access_target",
        name="test-host",
        locator="local://test-host",
        state="observed",
        visible=True,
        authorization="observed-authorized",
        provider="fixture",
    )
    context = DiscoveredExecutionContext(
        id=CONTEXT_ID,
        snapshot_id=SNAPSHOT_ID,
        target_id=TARGET_ID,
        context_key="current",
        kind="system",
        name="current environment",
        locator="local://test-host",
        state="active",
        provider="fixture",
    )
    return DiscoveryState(targets=[target], contexts=[context])
