from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from bourneprov.discovery import discover_site
from bourneprov.discovery_providers import (
    CurrentEnvironmentProvider,
    CurrentTargetProvider,
    DiscoveryRequest,
    DiscoveryState,
    IdentityProvider,
    ProviderOutput,
)
from bourneprov.inventory_references import resolve_inventory
from bourneprov.inventory_storage import InventoryStore
from tests.fixtures import system_provenance


class RaisingProvider:
    name = "raising"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        raise RuntimeError("isolated failure")


class SuccessfulProvider:
    name = "successful"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        return ProviderOutput(metadata={"preserved": True})


class InventoryStorageTests(unittest.TestCase):
    def _discover(self, store: InventoryStore, root: Path):
        with patch(
            "bourneprov.discovery_providers.collect_system",
            return_value=system_provenance(),
        ):
            return discover_site(
                store,
                cwd=root,
                environment={"PATH": "", "HOME": str(root)},
                providers=[
                    IdentityProvider(), CurrentTargetProvider(),
                    CurrentEnvironmentProvider(),
                ],
            )

    def test_snapshot_creation_persistence_reopen_and_queryable_topology(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            original = self._discover(InventoryStore(database), root)
            reloaded = InventoryStore(database).get(original.id)
            with closing(sqlite3.connect(database)) as connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                target_count = connection.execute(
                    "SELECT count(*) FROM discovered_targets"
                ).fetchone()[0]
                context_count = connection.execute(
                    "SELECT count(*) FROM discovered_execution_contexts"
                ).fetchone()[0]
                foreign_key_errors = connection.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()

        self.assertEqual(version, 3)
        self.assertEqual(reloaded, original)
        self.assertEqual(target_count, 1)
        self.assertEqual(context_count, 1)
        self.assertEqual(foreign_key_errors, [])
        self.assertEqual(reloaded.current_target.role, "access_target")  # type: ignore[union-attr]
        self.assertEqual(reloaded.execution_contexts[0].target_id, reloaded.current_target.id)  # type: ignore[union-attr]

    def test_second_discovery_is_independent_and_latest_is_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            first = self._discover(store, root)
            second = self._discover(store, root)
            first_reloaded = store.get(first.id)
            count = store.count()
            latest = resolve_inventory(store, "latest").id
            previous = resolve_inventory(store, "@2").id

        self.assertNotEqual(first.id, second.id)
        self.assertEqual(count, 2)
        self.assertEqual(latest, second.id)
        self.assertEqual(previous, first.id)
        self.assertEqual(first_reloaded, first)

    def test_provider_failure_does_not_erase_successful_provider_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = discover_site(
                InventoryStore(root / "bourne.sqlite3"),
                cwd=root,
                environment={"PATH": "", "HOME": str(root)},
                providers=[RaisingProvider(), SuccessfulProvider()],
            )

        statuses = {item.provider: item.status for item in snapshot.providers}
        self.assertEqual(statuses, {"raising": "error", "successful": "complete"})
        successful = next(item for item in snapshot.providers if item.provider == "successful")
        self.assertTrue(successful.metadata["preserved"])


if __name__ == "__main__":
    unittest.main()
