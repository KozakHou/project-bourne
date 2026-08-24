from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
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
from bourneprov.ids import new_ulid
from bourneprov.inventory_models import Capability, DiscoveryEvidence, InventorySnapshot
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

    def _unsaved_discovery(self, store: InventoryStore, root: Path) -> InventorySnapshot:
        with patch.object(store, "save") as save:
            snapshot = self._discover(store, root)
        save.assert_called_once_with(snapshot)
        return snapshot

    def _with_capability(
        self, snapshot: InventorySnapshot, *, historical: bool = False
    ) -> InventorySnapshot:
        capability = Capability(
            id=new_ulid(), snapshot_id=snapshot.id,
            context_id=snapshot.execution_contexts[0].id,
            kind="executable", name="fixture-solver", locator="/opt/fixture-solver",
            observation_state="historical" if historical else "observed",
            provider="fixture",
        )
        evidence = DiscoveryEvidence(
            id=new_ulid(), snapshot_id=snapshot.id, subject_type="capability",
            subject_id=capability.id, provider="fixture",
            evidence_type="fixture_history" if historical else "fixture_observation",
            observed_now=not historical, historical_only=historical,
        )
        return replace(snapshot, capabilities=[capability], evidence=[evidence])

    def test_valid_target_evidence_saves(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._unsaved_discovery(store, root)
            target_evidence = next(
                item for item in snapshot.evidence if item.subject_type == "target"
            )
            snapshot = replace(snapshot, evidence=[target_evidence])
            store.save(snapshot)
            reloaded = store.get(snapshot.id)

        self.assertEqual(reloaded.evidence, [target_evidence])

    def test_valid_capability_evidence_saves(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._with_capability(self._unsaved_discovery(store, root))
            store.save(snapshot)
            reloaded = store.get(snapshot.id)

        self.assertEqual(reloaded.capabilities, snapshot.capabilities)
        self.assertEqual(reloaded.evidence, snapshot.evidence)

    def test_valid_historical_evidence_saves(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._with_capability(
                self._unsaved_discovery(store, root), historical=True
            )
            store.save(snapshot)
            reloaded = store.get(snapshot.id)

        self.assertFalse(reloaded.evidence[0].observed_now)
        self.assertTrue(reloaded.evidence[0].historical_only)

    def test_unknown_evidence_subject_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._unsaved_discovery(store, root)
            evidence = next(
                item for item in snapshot.evidence if item.subject_type == "target"
            )
            snapshot = replace(
                snapshot, evidence=[replace(evidence, subject_id=new_ulid())]
            )

            with self.assertRaisesRegex(ValueError, "declared type"):
                store.save(snapshot)

    def test_evidence_subject_id_of_wrong_type_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._unsaved_discovery(store, root)
            evidence = next(
                item for item in snapshot.evidence if item.subject_type == "target"
            )
            snapshot = replace(
                snapshot,
                evidence=[
                    replace(
                        evidence, subject_type="capability",
                        subject_id=snapshot.targets[0].id,
                    )
                ],
            )

            with self.assertRaisesRegex(ValueError, "declared type"):
                store.save(snapshot)

    def test_evidence_from_another_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._unsaved_discovery(store, root)
            evidence = next(
                item for item in snapshot.evidence if item.subject_type == "target"
            )
            snapshot = replace(
                snapshot, evidence=[replace(evidence, snapshot_id=new_ulid())]
            )

            with self.assertRaisesRegex(ValueError, "snapshot IDs"):
                store.save(snapshot)

    def test_current_and_historical_evidence_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = InventoryStore(root / "bourne.sqlite3")
            snapshot = self._unsaved_discovery(store, root)
            evidence = next(
                item for item in snapshot.evidence if item.subject_type == "target"
            )
            snapshot = replace(
                snapshot,
                evidence=[replace(evidence, observed_now=True, historical_only=True)],
            )

            with self.assertRaisesRegex(ValueError, "observed now and historical only"):
                store.save(snapshot)

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

        self.assertEqual(version, 7)
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
