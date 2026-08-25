from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from bourneprov.storage import ExperimentNotFound, ExperimentStore
from tests.fixtures import experiment


class StorageTests(unittest.TestCase):
    def test_sqlite_creation_and_schema_use_public_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "nested" / "bourne.sqlite3"
            store = ExperimentStore(database)
            store.initialize()

            self.assertTrue(database.is_file())
            with closing(sqlite3.connect(database)) as connection:
                user_version = connection.execute("PRAGMA user_version").fetchone()[0]
                table_sql = connection.execute(
                    "SELECT sql FROM sqlite_master WHERE name = 'experiments'"
                ).fetchone()[0]

        self.assertEqual(user_version, 7)
        self.assertIn("id TEXT PRIMARY KEY", table_sql)
        self.assertIn("WITHOUT ROWID", table_sql)

    def test_experiment_persists_and_reloads_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            original = experiment()
            ExperimentStore(database).save(original)

            reloaded = ExperimentStore(database).get(original.id)

        self.assertEqual(reloaded, original)

    def test_recent_list_is_newest_first_and_limited(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = ExperimentStore(Path(directory) / "bourne.sqlite3")
            older = experiment()
            newer = experiment(
                id="01H00000000000000000000001",
                started_at="2026-01-02T00:00:00.000000Z",
                ended_at="2026-01-02T00:00:01.000000Z",
            )
            store.save(older)
            store.save(newer)

            listed = store.list_recent(limit=1)

        self.assertEqual([item.id for item in listed], [newer.id])

    def test_missing_experiment_is_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = ExperimentStore(Path(directory) / "bourne.sqlite3")
            with self.assertRaises(ExperimentNotFound):
                store.get("missing")


if __name__ == "__main__":
    unittest.main()
