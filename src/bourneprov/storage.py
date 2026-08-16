"""Durable local SQLite storage and explicit schema migrations."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

from .models import (
    Artifact,
    ExecutionContext,
    Experiment,
    ExperimentLineage,
    GitProvenance,
    SystemProvenance,
)

LATEST_SCHEMA_VERSION = 3

_SCHEMA_V1 = (
    """
    CREATE TABLE experiments (
        id TEXT PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'interrupted')),
        command TEXT NOT NULL,
        arguments_json TEXT NOT NULL,
        working_directory TEXT NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT NOT NULL,
        duration_seconds REAL NOT NULL CHECK (duration_seconds >= 0),
        exit_code INTEGER NOT NULL,
        stdout TEXT NOT NULL,
        stderr TEXT NOT NULL,
        git_json TEXT NOT NULL,
        system_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX experiments_started_at
    ON experiments (started_at DESC, id DESC)
    """,
)

_MIGRATION_1_TO_2 = (
    """
    ALTER TABLE experiments
    ADD COLUMN execution_context_json TEXT NOT NULL DEFAULT '{}'
    """,
    """
    CREATE TABLE artifacts (
        id TEXT PRIMARY KEY,
        experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
        role TEXT NOT NULL CHECK (length(role) > 0),
        original_path TEXT NOT NULL,
        resolved_path TEXT NOT NULL,
        existence_state TEXT NOT NULL
            CHECK (existence_state IN ('present', 'missing', 'unknown')),
        capture_status TEXT NOT NULL
            CHECK (capture_status IN ('complete', 'unreadable', 'unsupported', 'changed')),
        sha256 TEXT,
        size_bytes INTEGER CHECK (size_bytes IS NULL OR size_bytes >= 0),
        modified_at TEXT,
        captured_at TEXT NOT NULL,
        capture_error TEXT,
        CHECK (
            (existence_state = 'present'
                AND capture_status IN ('complete', 'unreadable', 'unsupported', 'changed'))
            OR (existence_state = 'missing' AND capture_status = 'complete')
            OR (existence_state = 'unknown' AND capture_status = 'unreadable')
        ),
        CHECK (
            (existence_state = 'present' AND capture_status = 'complete'
                AND sha256 IS NOT NULL)
            OR ((existence_state <> 'present' OR capture_status <> 'complete')
                AND sha256 IS NULL)
        ),
        CHECK (
            (existence_state = 'present' AND size_bytes IS NOT NULL
                AND modified_at IS NOT NULL)
            OR (existence_state IN ('missing', 'unknown') AND size_bytes IS NULL
                AND modified_at IS NULL)
        )
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX artifacts_experiment_role
    ON artifacts (experiment_id, role, captured_at, id)
    """,
    """
    CREATE INDEX artifacts_role_resolved_path
    ON artifacts (role, resolved_path, captured_at DESC, id DESC)
    """,
    """
    CREATE INDEX artifacts_role_sha256
    ON artifacts (role, sha256, captured_at DESC, id DESC)
    """,
    """
    CREATE TABLE experiment_lineage (
        child_experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
        parent_experiment_id TEXT NOT NULL REFERENCES experiments(id) ON DELETE RESTRICT,
        relationship TEXT NOT NULL CHECK (length(relationship) > 0),
        created_at TEXT NOT NULL,
        PRIMARY KEY (child_experiment_id, relationship),
        CHECK (child_experiment_id <> parent_experiment_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX experiment_lineage_parent
    ON experiment_lineage (parent_experiment_id, relationship)
    """,
)

_MIGRATION_2_TO_3 = (
    """
    CREATE TABLE inventory_snapshots (
        id TEXT PRIMARY KEY,
        captured_at TEXT NOT NULL,
        working_directory TEXT NOT NULL,
        site_label TEXT,
        metadata_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX inventory_snapshots_captured_at
    ON inventory_snapshots (captured_at DESC, id DESC)
    """,
    """
    CREATE TABLE inventory_identities (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL UNIQUE
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        username TEXT,
        uid INTEGER,
        primary_gid INTEGER,
        groups_json TEXT NOT NULL,
        home TEXT,
        provider TEXT NOT NULL,
        metadata_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE discovered_targets (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        parent_target_id TEXT REFERENCES discovered_targets(id) ON DELETE CASCADE,
        kind TEXT NOT NULL,
        role TEXT NOT NULL,
        name TEXT NOT NULL,
        locator TEXT,
        state TEXT NOT NULL,
        visible INTEGER CHECK (visible IS NULL OR visible IN (0, 1)),
        authorization TEXT NOT NULL,
        provider TEXT NOT NULL,
        metadata_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX discovered_targets_snapshot_role
    ON discovered_targets (snapshot_id, role, kind, name)
    """,
    """
    CREATE TABLE storage_resources (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        target_id TEXT REFERENCES discovered_targets(id) ON DELETE CASCADE,
        path TEXT NOT NULL,
        role_hints_json TEXT NOT NULL,
        exists_now INTEGER CHECK (exists_now IS NULL OR exists_now IN (0, 1)),
        readable INTEGER CHECK (readable IS NULL OR readable IN (0, 1)),
        writable INTEGER CHECK (writable IS NULL OR writable IN (0, 1)),
        searchable INTEGER CHECK (searchable IS NULL OR searchable IN (0, 1)),
        mount_point TEXT,
        filesystem_type TEXT,
        mount_read_only INTEGER
            CHECK (mount_read_only IS NULL OR mount_read_only IN (0, 1)),
        provider TEXT NOT NULL,
        metadata_json TEXT NOT NULL,
        UNIQUE (snapshot_id, path)
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX storage_resources_snapshot
    ON storage_resources (snapshot_id, path)
    """,
    """
    CREATE TABLE scheduler_resources (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        access_target_id TEXT REFERENCES discovered_targets(id) ON DELETE CASCADE,
        family TEXT NOT NULL,
        state TEXT NOT NULL,
        provider TEXT NOT NULL,
        current_allocation_json TEXT NOT NULL,
        metadata_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX scheduler_resources_snapshot_family
    ON scheduler_resources (snapshot_id, family)
    """,
    """
    CREATE TABLE scheduler_execution_targets (
        scheduler_id TEXT NOT NULL
            REFERENCES scheduler_resources(id) ON DELETE CASCADE,
        target_id TEXT NOT NULL
            REFERENCES discovered_targets(id) ON DELETE CASCADE,
        PRIMARY KEY (scheduler_id, target_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE discovered_execution_contexts (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        target_id TEXT REFERENCES discovered_targets(id) ON DELETE CASCADE,
        context_key TEXT NOT NULL,
        kind TEXT NOT NULL,
        name TEXT NOT NULL,
        locator TEXT,
        state TEXT NOT NULL,
        provider TEXT NOT NULL,
        metadata_json TEXT NOT NULL,
        UNIQUE (snapshot_id, context_key)
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX discovered_contexts_snapshot_kind
    ON discovered_execution_contexts (snapshot_id, kind, name)
    """,
    """
    CREATE TABLE capabilities (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        context_id TEXT NOT NULL
            REFERENCES discovered_execution_contexts(id) ON DELETE CASCADE,
        kind TEXT NOT NULL,
        name TEXT NOT NULL,
        locator TEXT,
        observation_state TEXT NOT NULL,
        provider TEXT NOT NULL,
        classifications_json TEXT NOT NULL,
        metadata_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX capabilities_snapshot_name
    ON capabilities (snapshot_id, name, kind, context_id)
    """,
    """
    CREATE INDEX capabilities_context
    ON capabilities (context_id, kind, name)
    """,
    """
    CREATE TABLE discovery_evidence (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        evidence_type TEXT NOT NULL,
        observed_now INTEGER NOT NULL CHECK (observed_now IN (0, 1)),
        historical_only INTEGER NOT NULL CHECK (historical_only IN (0, 1)),
        details_json TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX discovery_evidence_subject
    ON discovery_evidence (snapshot_id, subject_type, subject_id)
    """,
    """
    CREATE TABLE provider_results (
        id TEXT PRIMARY KEY,
        snapshot_id TEXT NOT NULL
            REFERENCES inventory_snapshots(id) ON DELETE CASCADE,
        provider TEXT NOT NULL,
        status TEXT NOT NULL
            CHECK (status IN ('complete', 'unavailable', 'partial', 'error', 'timeout')),
        started_at TEXT NOT NULL,
        ended_at TEXT NOT NULL,
        duration_seconds REAL NOT NULL CHECK (duration_seconds >= 0),
        diagnostic TEXT,
        truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
        metadata_json TEXT NOT NULL,
        UNIQUE (snapshot_id, provider)
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX provider_results_snapshot_status
    ON provider_results (snapshot_id, status, provider)
    """,
)


class ExperimentNotFound(LookupError):
    pass


class DatabaseMigrationError(RuntimeError):
    pass


class UnsupportedDatabaseVersion(DatabaseMigrationError):
    pass


class ExperimentStore:
    """A small repository around a single SQLite database."""

    def __init__(self, path: Path):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create or transactionally migrate the database to the current schema."""

        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._connection() as connection:
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                if version > LATEST_SCHEMA_VERSION:
                    raise UnsupportedDatabaseVersion(
                        f"database schema version {version} is newer than supported "
                        f"version {LATEST_SCHEMA_VERSION}"
                    )
                if version == 0:
                    existing = connection.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                        LIMIT 1
                        """
                    ).fetchone()
                    if existing is not None:
                        raise DatabaseMigrationError(
                            "database has tables but no recognized Bourne schema version"
                        )
                    for statement in _SCHEMA_V1:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 1")
                    version = 1
                if version == 1:
                    for statement in _MIGRATION_1_TO_2:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 2")
                    version = 2
                if version == 2:
                    for statement in _MIGRATION_2_TO_3:
                        connection.execute(statement)
                    connection.execute("PRAGMA user_version = 3")
        except sqlite3.Error as exc:
            raise DatabaseMigrationError(f"could not migrate Bourne database: {exc}") from exc

    def save(self, experiment: Experiment) -> None:
        """Preserve the v0.1 API for saving an experiment without relationships."""

        self.save_record(experiment)

    def save_record(
        self,
        experiment: Experiment,
        artifacts: Sequence[Artifact] = (),
        lineage: Sequence[ExperimentLineage] = (),
    ) -> None:
        """Atomically save an experiment and all of its declared provenance."""

        if any(artifact.experiment_id != experiment.id for artifact in artifacts):
            raise ValueError("artifact experiment IDs must match the saved experiment")
        if any(item.child_experiment_id != experiment.id for item in lineage):
            raise ValueError("lineage child IDs must match the saved experiment")

        self.initialize()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO experiments (
                    id, schema_version, status, command, arguments_json,
                    working_directory, started_at, ended_at, duration_seconds,
                    exit_code, stdout, stderr, git_json, system_json,
                    execution_context_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment.id,
                    experiment.schema_version,
                    experiment.status,
                    experiment.command,
                    _json(experiment.arguments),
                    experiment.working_directory,
                    experiment.started_at,
                    experiment.ended_at,
                    experiment.duration_seconds,
                    experiment.exit_code,
                    experiment.stdout,
                    experiment.stderr,
                    _json(experiment.to_dict()["git"]),
                    _json(experiment.to_dict()["system"]),
                    _json(experiment.to_dict()["execution_context"]),
                ),
            )
            connection.executemany(
                """
                INSERT INTO artifacts (
                    id, experiment_id, role, original_path, resolved_path,
                    existence_state, capture_status, sha256, size_bytes,
                    modified_at, captured_at, capture_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.id,
                        item.experiment_id,
                        item.role,
                        item.original_path,
                        item.resolved_path,
                        item.existence_state,
                        item.capture_status,
                        item.sha256,
                        item.size_bytes,
                        item.modified_at,
                        item.captured_at,
                        item.capture_error,
                    )
                    for item in artifacts
                ],
            )
            connection.executemany(
                """
                INSERT INTO experiment_lineage (
                    child_experiment_id, parent_experiment_id, relationship, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        item.child_experiment_id,
                        item.parent_experiment_id,
                        item.relationship,
                        item.created_at,
                    )
                    for item in lineage
                ],
            )

    def get(self, experiment_id: str) -> Experiment:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM experiments WHERE id = ?", (experiment_id,)
            ).fetchone()
        if row is None:
            raise ExperimentNotFound(experiment_id)
        return _experiment_from_row(row)

    def get_by_recency(self, position: int) -> Experiment:
        """Return the one-based *position* in newest-first order."""

        if position < 1:
            raise ValueError("position must be at least 1")
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM experiments
                ORDER BY started_at DESC, id DESC
                LIMIT 1 OFFSET ?
                """,
                (position - 1,),
            ).fetchone()
        if row is None:
            raise ExperimentNotFound(f"@{position}")
        return _experiment_from_row(row)

    def find_ids_by_prefix(self, prefix: str, limit: int = 100) -> list[str]:
        """Return canonical IDs beginning with *prefix* in newest-first order."""

        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id FROM experiments
                WHERE substr(id, 1, ?) = ?
                ORDER BY started_at DESC, id DESC
                LIMIT ?
                """,
                (len(prefix), prefix, limit),
            ).fetchall()
        return [row["id"] for row in rows]

    def count(self) -> int:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT count(*) AS count FROM experiments"
            ).fetchone()
        return int(row["count"])

    def list_recent(self, limit: int = 20) -> list[Experiment]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM experiments ORDER BY started_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_experiment_from_row(row) for row in rows]

    def list_artifacts(self, experiment_id: str, role: str | None = None) -> list[Artifact]:
        """Return artifacts for one experiment without N+1 experiment queries."""

        self.initialize()
        parameters: tuple[str, ...]
        if role is None:
            sql = """
                SELECT * FROM artifacts WHERE experiment_id = ?
                ORDER BY role, captured_at, id
            """
            parameters = (experiment_id,)
        else:
            sql = """
                SELECT * FROM artifacts WHERE experiment_id = ? AND role = ?
                ORDER BY captured_at, id
            """
            parameters = (experiment_id, role)
        with self._connection() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [_artifact_from_row(row) for row in rows]

    def find_output_artifacts(
        self,
        *,
        resolved_path: str | None = None,
        sha256: str | None = None,
    ) -> list[Artifact]:
        """Find output versions by stored path/content identity for tracing."""

        if resolved_path is None and sha256 is None:
            raise ValueError("resolved_path or sha256 is required")
        clauses = ["role = 'output'"]
        parameters: list[str] = []
        if resolved_path is not None:
            clauses.append("resolved_path = ?")
            parameters.append(resolved_path)
        if sha256 is not None:
            clauses.append("sha256 = ?")
            parameters.append(sha256)
            clauses.extend(
                ["existence_state = 'present'", "capture_status = 'complete'"]
            )
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM artifacts WHERE {' AND '.join(clauses)}
                ORDER BY captured_at DESC, id DESC
                """,
                parameters,
            ).fetchall()
        return [_artifact_from_row(row) for row in rows]

    def get_lineage(
        self, child_experiment_id: str, relationship: str = "derived_from"
    ) -> ExperimentLineage | None:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM experiment_lineage
                WHERE child_experiment_id = ? AND relationship = ?
                """,
                (child_experiment_id, relationship),
            ).fetchone()
        return None if row is None else _lineage_from_row(row)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _experiment_from_row(row: sqlite3.Row) -> Experiment:
    raw_context = json.loads(row["execution_context_json"])
    context = (
        ExecutionContext.from_dict(raw_context)
        if raw_context
        else ExecutionContext(requested_executable=row["command"])
    )
    return Experiment(
        id=row["id"],
        schema_version=row["schema_version"],
        status=row["status"],
        command=row["command"],
        arguments=json.loads(row["arguments_json"]),
        working_directory=row["working_directory"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        duration_seconds=row["duration_seconds"],
        exit_code=row["exit_code"],
        stdout=row["stdout"],
        stderr=row["stderr"],
        git=GitProvenance.from_dict(json.loads(row["git_json"])),
        system=SystemProvenance.from_dict(json.loads(row["system_json"])),
        execution_context=context,
    )


def _artifact_from_row(row: sqlite3.Row) -> Artifact:
    return Artifact(
        id=row["id"],
        experiment_id=row["experiment_id"],
        role=row["role"],
        original_path=row["original_path"],
        resolved_path=row["resolved_path"],
        existence_state=row["existence_state"],
        capture_status=row["capture_status"],
        sha256=row["sha256"],
        size_bytes=row["size_bytes"],
        modified_at=row["modified_at"],
        captured_at=row["captured_at"],
        capture_error=row["capture_error"],
    )


def _lineage_from_row(row: sqlite3.Row) -> ExperimentLineage:
    return ExperimentLineage(
        child_experiment_id=row["child_experiment_id"],
        parent_experiment_id=row["parent_experiment_id"],
        relationship=row["relationship"],
        created_at=row["created_at"],
    )
