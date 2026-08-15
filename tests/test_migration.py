from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import asdict
from pathlib import Path

from bourneprov.models import GitProvenance
from bourneprov.storage import (
    DatabaseMigrationError,
    ExperimentStore,
    UnsupportedDatabaseVersion,
)
from tests.fixtures import experiment, system_provenance

_V1_SCHEMA = """
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
) WITHOUT ROWID;
CREATE INDEX experiments_started_at
ON experiments (started_at DESC, id DESC);
PRAGMA user_version = 1;
"""


def create_v1_database(path: Path) -> list[str]:
    ids = [
        "01HAAA" + "0" * 20,
        "01HBBB" + "1" * 20,
        "01HCCC" + "2" * 20,
    ]
    git_json = json.dumps(asdict(GitProvenance(available=False)))
    system_json = json.dumps(asdict(system_provenance()))
    with closing(sqlite3.connect(path)) as connection:
        with connection:
            connection.executescript(_V1_SCHEMA)
            for index, (experiment_id, status, exit_code) in enumerate(
                zip(ids, ("completed", "failed", "interrupted"), (0, 9, 130))
            ):
                connection.execute(
                """
                INSERT INTO experiments (
                    id, schema_version, status, command, arguments_json,
                    working_directory, started_at, ended_at, duration_seconds,
                    exit_code, stdout, stderr, git_json, system_json
                ) VALUES (?, 1, ?, 'solver', '["case.dat"]', '/work', ?, ?, 1.0,
                          ?, '', '', ?, ?)
                """,
                    (
                        experiment_id,
                        status,
                        f"2026-01-0{index + 1}T00:00:00.000000Z",
                        f"2026-01-0{index + 1}T00:00:01.000000Z",
                        exit_code,
                        git_json,
                        system_json,
                    ),
                )
    return ids


class MigrationTests(unittest.TestCase):
    def test_realistic_v1_database_migrates_and_remains_reopenable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            old_ids = create_v1_database(database)
            store = ExperimentStore(database)
            store.initialize()
            old_records = [store.get(experiment_id) for experiment_id in old_ids]
            new_record = experiment(
                id="01HDDD" + "3" * 20,
                started_at="2026-01-04T00:00:00.000000Z",
                ended_at="2026-01-04T00:00:01.000000Z",
            )
            store.save(new_record)

            reopened = ExperimentStore(database)
            all_records = reopened.list_recent(limit=10)
            reloaded_new = reopened.get(new_record.id)
            with closing(sqlite3.connect(database)) as connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                artifact_columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(artifacts)")
                }
                foreign_key_errors = connection.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()

        self.assertEqual([item.status for item in old_records], [
            "completed",
            "failed",
            "interrupted",
        ])
        self.assertTrue(all(item.schema_version == 1 for item in old_records))
        self.assertTrue(
            all(item.execution_context.requested_executable == "solver" for item in old_records)
        )
        self.assertEqual(version, 2)
        self.assertIn("artifacts", tables)
        self.assertIn("experiment_lineage", tables)
        self.assertIn("existence_state", artifact_columns)
        self.assertIn("capture_status", artifact_columns)
        self.assertNotIn("exists_state", artifact_columns)
        self.assertEqual(len(all_records), 4)
        self.assertEqual(reloaded_new, new_record)
        self.assertEqual(foreign_key_errors, [])

    def test_unrecognized_schema_fails_without_resetting_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "unknown.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                with connection:
                    connection.execute("CREATE TABLE user_data (value TEXT)")
                    connection.execute("INSERT INTO user_data VALUES ('preserve me')")

            with self.assertRaises(DatabaseMigrationError):
                ExperimentStore(database).initialize()

            with closing(sqlite3.connect(database)) as connection:
                value = connection.execute("SELECT value FROM user_data").fetchone()[0]
                version = connection.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(value, "preserve me")
        self.assertEqual(version, 0)

    def test_newer_schema_version_is_rejected_without_modification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "future.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("PRAGMA user_version = 99")

            with self.assertRaises(UnsupportedDatabaseVersion):
                ExperimentStore(database).initialize()

            with closing(sqlite3.connect(database)) as connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(version, 99)


if __name__ == "__main__":
    unittest.main()
