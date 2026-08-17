from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

from bourneprov.discovery import discover_site
from bourneprov.discovery_providers import (
    CurrentEnvironmentProvider,
    CurrentTargetProvider,
    SystemCapabilityProvider,
)
from bourneprov.inventory_storage import InventoryStore
from bourneprov.models import GitProvenance
from bourneprov.storage import (
    DatabaseMigrationError,
    ExperimentStore,
    UnsupportedDatabaseVersion,
    _MIGRATION_1_TO_2,
    _MIGRATION_2_TO_3,
    _SCHEMA_V1,
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
    def test_unreleased_schema_three_migrates_transactionally_to_schema_four(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "schema-three.sqlite3"
            with closing(sqlite3.connect(database)) as connection:
                with connection:
                    for statement in (*_SCHEMA_V1, *_MIGRATION_1_TO_2, *_MIGRATION_2_TO_3):
                        connection.execute(statement)
                    connection.execute(
                        "INSERT INTO inventory_snapshots VALUES (?, ?, ?, ?, ?)",
                        (
                            "01HV030" + "0" * 19,
                            "2026-06-01T00:00:00.000000Z", "/work", "released-v0.3",
                            '{"schema_version":3}',
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO discovered_targets (
                            id, snapshot_id, parent_target_id, kind, role, name,
                            locator, state, visible, authorization, provider, metadata_json
                        ) VALUES (?, ?, NULL, 'host', 'access_target', 'v03-host',
                                  'local://v03-host', 'observed', 1,
                                  'observed-authorized', 'fixture', '{}')
                        """,
                        ("01HTARG" + "1" * 19, "01HV030" + "0" * 19),
                    )
                    connection.execute(
                        """
                        INSERT INTO discovered_execution_contexts (
                            id, snapshot_id, target_id, context_key, kind, name,
                            locator, state, provider, metadata_json
                        ) VALUES (?, ?, ?, 'current', 'system', 'current environment',
                                  'local://v03-host', 'active', 'fixture', '{}')
                        """,
                        (
                            "01HCONT" + "2" * 19, "01HV030" + "0" * 19,
                            "01HTARG" + "1" * 19,
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO capabilities (
                            id, snapshot_id, context_id, kind, name, locator,
                            observation_state, provider, classifications_json, metadata_json
                        ) VALUES (?, ?, ?, 'executable', 'solver', '/opt/solver',
                                  'observed', 'fixture', '[]', '{}')
                        """,
                        (
                            "01HCAPA" + "3" * 19, "01HV030" + "0" * 19,
                            "01HCONT" + "2" * 19,
                        ),
                    )
                    connection.execute("PRAGMA user_version = 3")

            ExperimentStore(database).initialize()
            snapshot = InventoryStore(database).get("01HV030" + "0" * 19)
            with closing(sqlite3.connect(database)) as connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
                foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()

        self.assertEqual(version, 4)
        self.assertIn("execution_plans", tables)
        self.assertIn("execution_events", tables)
        self.assertEqual(snapshot.current_target.name, "v03-host")  # type: ignore[union-attr]
        self.assertEqual(snapshot.capabilities[0].name, "solver")
        self.assertEqual(integrity, "ok")
        self.assertEqual(foreign_keys, [])

    def _migrate_released_fixture(self, version: str):
        fixture = Path(__file__).parent / "released_databases" / f"bourneprov-{version}.db"
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        database = Path(temporary.name) / "bourne.sqlite3"
        shutil.copy2(fixture, database)
        experiment_store = ExperimentStore(database)
        before_ids = [item.id for item in experiment_store.list_recent(limit=100)]
        with patch(
            "bourneprov.discovery_providers.collect_system",
            return_value=system_provenance(),
        ):
            snapshot = discover_site(
                InventoryStore(database), cwd=Path(temporary.name),
                environment={"PATH": "", "HOME": temporary.name},
                providers=[
                    CurrentTargetProvider(), CurrentEnvironmentProvider(),
                    SystemCapabilityProvider(),
                ],
            )
        reopened = InventoryStore(database).get(snapshot.id)
        after = ExperimentStore(database).list_recent(limit=100)
        with closing(sqlite3.connect(database)) as connection:
            version_after = connection.execute("PRAGMA user_version").fetchone()[0]
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        return database, before_ids, after, reopened, version_after, integrity, foreign_keys

    def test_actual_released_v011_database_migrates_to_schema_four(self) -> None:
        (
            _database, before_ids, after, snapshot, version, integrity, foreign_keys
        ) = self._migrate_released_fixture("0.1.1")

        self.assertEqual({item.status for item in after}, {"completed", "failed", "interrupted"})
        self.assertEqual({item.id for item in after}, set(before_ids))
        self.assertTrue(all(item.schema_version == 1 for item in after))
        self.assertEqual(version, 4)
        self.assertEqual(integrity, "ok")
        self.assertEqual(foreign_keys, [])
        self.assertEqual(len(snapshot.targets), 1)
        self.assertGreaterEqual(len(snapshot.capabilities), 2)

    def test_actual_released_v020_database_migrates_to_schema_four(self) -> None:
        (
            database, before_ids, after, snapshot, version, integrity, foreign_keys
        ) = self._migrate_released_fixture("0.2.0")
        store = ExperimentStore(database)
        artifacts = [item for record in after for item in store.list_artifacts(record.id)]
        lineage = [store.get_lineage(record.id) for record in after]

        self.assertEqual({item.id for item in after}, set(before_ids))
        self.assertEqual({item.status for item in after}, {"completed", "failed"})
        self.assertEqual({item.existence_state for item in artifacts}, {"present", "missing"})
        self.assertEqual({item.capture_status for item in artifacts}, {"complete"})
        self.assertTrue(any(item is not None for item in lineage))
        self.assertTrue(all(item.execution_context.requested_executable for item in after))
        self.assertEqual(version, 4)
        self.assertEqual(integrity, "ok")
        self.assertEqual(foreign_keys, [])
        self.assertEqual(len(snapshot.execution_contexts), 1)
        self.assertGreaterEqual(len(snapshot.capabilities), 2)

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
        self.assertEqual(version, 4)
        self.assertIn("artifacts", tables)
        self.assertIn("experiment_lineage", tables)
        self.assertIn("inventory_snapshots", tables)
        self.assertIn("discovered_targets", tables)
        self.assertIn("storage_resources", tables)
        self.assertIn("scheduler_resources", tables)
        self.assertIn("discovered_execution_contexts", tables)
        self.assertIn("capabilities", tables)
        self.assertIn("provider_results", tables)
        self.assertIn("workload_specs", tables)
        self.assertIn("execution_plans", tables)
        self.assertIn("execution_attempts", tables)
        self.assertIn("scheduler_jobs", tables)
        self.assertIn("allocations", tables)
        self.assertIn("execution_events", tables)
        self.assertIn("execution_experiment_links", tables)
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
