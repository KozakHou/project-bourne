"""Persistence operations for immutable compute-site inventory snapshots."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .inventory_models import (
    Capability,
    CurrentIdentity,
    DiscoveredExecutionContext,
    DiscoveredTarget,
    DiscoveryEvidence,
    InventorySnapshot,
    ProviderResult,
    SchedulerResource,
    StorageResource,
)
from .storage import ExperimentStore


class InventoryNotFound(LookupError):
    pass


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _validate_evidence(snapshot: InventorySnapshot) -> None:
    subject_ids = {
        "identity": set() if snapshot.identity is None else {snapshot.identity.id},
        "target": {item.id for item in snapshot.targets},
        "storage": {item.id for item in snapshot.storage},
        "scheduler": {item.id for item in snapshot.schedulers},
        "execution_context": {item.id for item in snapshot.execution_contexts},
        "capability": {item.id for item in snapshot.capabilities},
    }
    for evidence in snapshot.evidence:
        if evidence.observed_now and evidence.historical_only:
            raise ValueError(
                "evidence cannot be both observed now and historical only"
            )
        if evidence.subject_type not in subject_ids:
            raise ValueError(
                f"unsupported evidence subject type: {evidence.subject_type}"
            )
        if evidence.subject_id not in subject_ids[evidence.subject_type]:
            raise ValueError(
                "evidence subject must belong to its declared type in the snapshot"
            )


class InventoryStore:
    """A queryable repository sharing Bourne's existing SQLite database."""

    def __init__(self, path: Path):
        self.path = path

    def initialize(self) -> None:
        ExperimentStore(self.path).initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self.initialize()
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def save(self, snapshot: InventorySnapshot) -> None:
        """Atomically insert one new immutable snapshot and all observations."""

        children: list[object] = [
            *snapshot.targets,
            *snapshot.storage,
            *snapshot.schedulers,
            *snapshot.execution_contexts,
            *snapshot.capabilities,
            *snapshot.evidence,
            *snapshot.providers,
        ]
        if snapshot.identity is not None:
            children.append(snapshot.identity)
        if any(getattr(item, "snapshot_id", None) != snapshot.id for item in children):
            raise ValueError("inventory child snapshot IDs must match the snapshot")
        context_ids = {item.id for item in snapshot.execution_contexts}
        target_ids = {item.id for item in snapshot.targets}
        if any(item.context_id not in context_ids for item in snapshot.capabilities):
            raise ValueError("capabilities must reference a context in the snapshot")
        if any(
            item.parent_target_id is not None and item.parent_target_id not in target_ids
            for item in snapshot.targets
        ):
            raise ValueError("target parents must belong to the snapshot")
        if any(
            item.target_id is not None and item.target_id not in target_ids
            for item in [*snapshot.storage, *snapshot.execution_contexts]
        ):
            raise ValueError("storage and contexts must reference a target in the snapshot")
        if any(
            (item.access_target_id is not None and item.access_target_id not in target_ids)
            or any(target_id not in target_ids for target_id in item.execution_target_ids)
            for item in snapshot.schedulers
        ):
            raise ValueError("scheduler target relationships must belong to the snapshot")
        _validate_evidence(snapshot)

        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO inventory_snapshots
                    (id, captured_at, working_directory, site_label, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot.id,
                    snapshot.captured_at,
                    snapshot.working_directory,
                    snapshot.site_label,
                    _json(snapshot.metadata),
                ),
            )
            if snapshot.identity is not None:
                item = snapshot.identity
                connection.execute(
                    """
                    INSERT INTO inventory_identities
                        (id, snapshot_id, username, uid, primary_gid, groups_json,
                         home, provider, metadata_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.id,
                        item.snapshot_id,
                        item.username,
                        item.uid,
                        item.primary_gid,
                        _json(item.groups),
                        item.home,
                        item.provider,
                        _json(item.metadata),
                    ),
                )
            connection.executemany(
                """
                INSERT INTO discovered_targets
                    (id, snapshot_id, parent_target_id, kind, role, name, locator, state, visible,
                     authorization, provider, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.parent_target_id,
                        item.kind, item.role, item.name,
                        item.locator, item.state, item.visible, item.authorization,
                        item.provider, _json(item.metadata),
                    )
                    for item in snapshot.targets
                ],
            )
            connection.executemany(
                """
                INSERT INTO storage_resources
                    (id, snapshot_id, target_id, path, role_hints_json, exists_now, readable,
                     writable, searchable, mount_point, filesystem_type,
                     mount_read_only, provider, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.target_id,
                        item.path, _json(item.role_hints),
                        item.exists, item.readable, item.writable, item.searchable,
                        item.mount_point, item.filesystem_type, item.mount_read_only,
                        item.provider, _json(item.metadata),
                    )
                    for item in snapshot.storage
                ],
            )
            connection.executemany(
                """
                INSERT INTO scheduler_resources
                    (id, snapshot_id, access_target_id, family, state, provider,
                     current_allocation_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.access_target_id,
                        item.family, item.state,
                        item.provider, _json(item.current_allocation),
                        _json(item.metadata),
                    )
                    for item in snapshot.schedulers
                ],
            )
            connection.executemany(
                """
                INSERT INTO scheduler_execution_targets (scheduler_id, target_id)
                VALUES (?, ?)
                """,
                [
                    (scheduler.id, target_id)
                    for scheduler in snapshot.schedulers
                    for target_id in scheduler.execution_target_ids
                ],
            )
            connection.executemany(
                """
                INSERT INTO discovered_execution_contexts
                    (id, snapshot_id, target_id, context_key, kind, name, locator, state,
                     provider, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.target_id,
                        item.context_key, item.kind,
                        item.name, item.locator, item.state, item.provider,
                        _json(item.metadata),
                    )
                    for item in snapshot.execution_contexts
                ],
            )
            connection.executemany(
                """
                INSERT INTO capabilities
                    (id, snapshot_id, context_id, kind, name, locator,
                     observation_state, provider, classifications_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.context_id, item.kind,
                        item.name, item.locator, item.observation_state, item.provider,
                        _json(item.classifications), _json(item.metadata),
                    )
                    for item in snapshot.capabilities
                ],
            )
            connection.executemany(
                """
                INSERT INTO discovery_evidence
                    (id, snapshot_id, subject_type, subject_id, provider,
                     evidence_type, observed_now, historical_only, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.subject_type, item.subject_id,
                        item.provider, item.evidence_type, item.observed_now,
                        item.historical_only, _json(item.details),
                    )
                    for item in snapshot.evidence
                ],
            )
            connection.executemany(
                """
                INSERT INTO provider_results
                    (id, snapshot_id, provider, status, started_at, ended_at,
                     duration_seconds, diagnostic, truncated, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id, item.snapshot_id, item.provider, item.status,
                        item.started_at, item.ended_at, item.duration_seconds,
                        item.diagnostic, item.truncated, _json(item.metadata),
                    )
                    for item in snapshot.providers
                ],
            )

    def count(self) -> int:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT count(*) AS count FROM inventory_snapshots"
            ).fetchone()
        return int(row["count"])

    def find_ids_by_prefix(self, prefix: str, limit: int = 100) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id FROM inventory_snapshots
                WHERE substr(id, 1, ?) = ?
                ORDER BY captured_at DESC, id DESC LIMIT ?
                """,
                (len(prefix), prefix, limit),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def get_by_recency(self, position: int) -> InventorySnapshot:
        if position < 1:
            raise ValueError("position must be at least 1")
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT id FROM inventory_snapshots
                ORDER BY captured_at DESC, id DESC LIMIT 1 OFFSET ?
                """,
                (position - 1,),
            ).fetchone()
        if row is None:
            raise InventoryNotFound(f"@{position}")
        return self.get(str(row["id"]))

    def get(self, snapshot_id: str) -> InventorySnapshot:
        with self._connection() as connection:
            snapshot = connection.execute(
                "SELECT * FROM inventory_snapshots WHERE id = ?", (snapshot_id,)
            ).fetchone()
            if snapshot is None:
                raise InventoryNotFound(snapshot_id)
            identity = connection.execute(
                "SELECT * FROM inventory_identities WHERE snapshot_id = ?", (snapshot_id,)
            ).fetchone()
            targets = connection.execute(
                "SELECT * FROM discovered_targets WHERE snapshot_id = ? ORDER BY role, kind, name, id",
                (snapshot_id,),
            ).fetchall()
            storage = connection.execute(
                "SELECT * FROM storage_resources WHERE snapshot_id = ? ORDER BY path, id",
                (snapshot_id,),
            ).fetchall()
            schedulers = connection.execute(
                "SELECT * FROM scheduler_resources WHERE snapshot_id = ? ORDER BY family, id",
                (snapshot_id,),
            ).fetchall()
            scheduler_links = connection.execute(
                "SELECT * FROM scheduler_execution_targets WHERE scheduler_id IN "
                "(SELECT id FROM scheduler_resources WHERE snapshot_id = ?)",
                (snapshot_id,),
            ).fetchall()
            contexts = connection.execute(
                "SELECT * FROM discovered_execution_contexts WHERE snapshot_id = ? ORDER BY kind, name, id",
                (snapshot_id,),
            ).fetchall()
            capabilities = connection.execute(
                "SELECT * FROM capabilities WHERE snapshot_id = ? ORDER BY name, locator, id",
                (snapshot_id,),
            ).fetchall()
            evidence = connection.execute(
                "SELECT * FROM discovery_evidence WHERE snapshot_id = ? ORDER BY subject_type, subject_id, id",
                (snapshot_id,),
            ).fetchall()
            providers = connection.execute(
                "SELECT * FROM provider_results WHERE snapshot_id = ? ORDER BY provider",
                (snapshot_id,),
            ).fetchall()

        return InventorySnapshot(
            id=str(snapshot["id"]),
            captured_at=str(snapshot["captured_at"]),
            working_directory=str(snapshot["working_directory"]),
            site_label=snapshot["site_label"],
            metadata=json.loads(snapshot["metadata_json"]),
            identity=None if identity is None else _identity(identity),
            targets=[_target(row) for row in targets],
            storage=[_storage(row) for row in storage],
            schedulers=[
                _scheduler(
                    row,
                    [link["target_id"] for link in scheduler_links if link["scheduler_id"] == row["id"]],
                )
                for row in schedulers
            ],
            execution_contexts=[_context(row) for row in contexts],
            capabilities=[_capability(row) for row in capabilities],
            evidence=[_evidence(row) for row in evidence],
            providers=[_provider(row) for row in providers],
        )

    def history_observations(self, limit: int = 10_000) -> list[dict[str, Any]]:
        """Return only bounded, safe experiment fields needed by history discovery."""

        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT command, status, started_at, system_json, execution_context_json
                FROM experiments ORDER BY started_at DESC, id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "command": row["command"],
                "status": row["status"],
                "started_at": row["started_at"],
                "system": json.loads(row["system_json"]),
                "execution_context": json.loads(row["execution_context_json"]),
            }
            for row in rows
        ]


def _identity(row: sqlite3.Row) -> CurrentIdentity:
    return CurrentIdentity(
        id=row["id"], snapshot_id=row["snapshot_id"], username=row["username"],
        uid=row["uid"], primary_gid=row["primary_gid"],
        groups=json.loads(row["groups_json"]), home=row["home"],
        provider=row["provider"], metadata=json.loads(row["metadata_json"]),
    )


def _target(row: sqlite3.Row) -> DiscoveredTarget:
    return DiscoveredTarget(
        id=row["id"], snapshot_id=row["snapshot_id"],
        parent_target_id=row["parent_target_id"], kind=row["kind"],
        role=row["role"], name=row["name"], locator=row["locator"],
        state=row["state"], visible=None if row["visible"] is None else bool(row["visible"]),
        authorization=row["authorization"], provider=row["provider"],
        metadata=json.loads(row["metadata_json"]),
    )


def _storage(row: sqlite3.Row) -> StorageResource:
    boolean = lambda name: None if row[name] is None else bool(row[name])
    return StorageResource(
        id=row["id"], snapshot_id=row["snapshot_id"], target_id=row["target_id"],
        path=row["path"],
        role_hints=json.loads(row["role_hints_json"]), exists=boolean("exists_now"),
        readable=boolean("readable"), writable=boolean("writable"),
        searchable=boolean("searchable"), mount_point=row["mount_point"],
        filesystem_type=row["filesystem_type"], mount_read_only=boolean("mount_read_only"),
        provider=row["provider"], metadata=json.loads(row["metadata_json"]),
    )


def _scheduler(row: sqlite3.Row, execution_target_ids: list[str]) -> SchedulerResource:
    return SchedulerResource(
        id=row["id"], snapshot_id=row["snapshot_id"],
        access_target_id=row["access_target_id"], family=row["family"],
        state=row["state"], provider=row["provider"],
        current_allocation=json.loads(row["current_allocation_json"]),
        execution_target_ids=execution_target_ids,
        metadata=json.loads(row["metadata_json"]),
    )


def _context(row: sqlite3.Row) -> DiscoveredExecutionContext:
    return DiscoveredExecutionContext(
        id=row["id"], snapshot_id=row["snapshot_id"], target_id=row["target_id"],
        context_key=row["context_key"],
        kind=row["kind"], name=row["name"], locator=row["locator"],
        state=row["state"], provider=row["provider"],
        metadata=json.loads(row["metadata_json"]),
    )


def _capability(row: sqlite3.Row) -> Capability:
    return Capability(
        id=row["id"], snapshot_id=row["snapshot_id"], context_id=row["context_id"],
        kind=row["kind"], name=row["name"], locator=row["locator"],
        observation_state=row["observation_state"], provider=row["provider"],
        classifications=json.loads(row["classifications_json"]),
        metadata=json.loads(row["metadata_json"]),
    )


def _evidence(row: sqlite3.Row) -> DiscoveryEvidence:
    return DiscoveryEvidence(
        id=row["id"], snapshot_id=row["snapshot_id"], subject_type=row["subject_type"],
        subject_id=row["subject_id"], provider=row["provider"],
        evidence_type=row["evidence_type"], observed_now=bool(row["observed_now"]),
        historical_only=bool(row["historical_only"]),
        details=json.loads(row["details_json"]),
    )


def _provider(row: sqlite3.Row) -> ProviderResult:
    return ProviderResult(
        id=row["id"], snapshot_id=row["snapshot_id"], provider=row["provider"],
        status=row["status"], started_at=row["started_at"], ended_at=row["ended_at"],
        duration_seconds=row["duration_seconds"], diagnostic=row["diagnostic"],
        truncated=bool(row["truncated"]), metadata=json.loads(row["metadata_json"]),
    )
