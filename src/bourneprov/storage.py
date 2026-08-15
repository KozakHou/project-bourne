"""Durable local SQLite storage for experiments."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .models import Experiment, GitProvenance, SystemProvenance

_SCHEMA = """
CREATE TABLE IF NOT EXISTS experiments (
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
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS experiments_started_at
ON experiments (started_at DESC, id DESC);
"""


class ExperimentNotFound(LookupError):
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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(_SCHEMA)
            connection.execute("PRAGMA user_version = 1")

    def save(self, experiment: Experiment) -> None:
        self.initialize()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO experiments (
                    id, schema_version, status, command, arguments_json,
                    working_directory, started_at, ended_at, duration_seconds,
                    exit_code, stdout, stderr, git_json, system_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
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
            row = connection.execute("SELECT count(*) AS count FROM experiments").fetchone()
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


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _experiment_from_row(row: sqlite3.Row) -> Experiment:
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
    )
