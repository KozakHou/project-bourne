from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from bourneprov.references import (
    AmbiguousExperimentReference,
    MissingExperimentReference,
    RelativeExperimentReferenceOutOfRange,
    resolve_experiment,
)
from bourneprov.storage import ExperimentStore
from tests.fixtures import experiment


class ExperimentReferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.store = ExperimentStore(
            Path(self.temporary_directory.name) / "bourne.sqlite3"
        )
        self.oldest_id = "01HAAA" + "0" * 20
        self.middle_id = "01HAAA" + "1" * 20
        self.latest_id = "01HBBB" + "2" * 20
        self.store.save(
            experiment(
                id=self.oldest_id,
                started_at="2026-01-01T00:00:00.000000Z",
                ended_at="2026-01-01T00:00:01.000000Z",
            )
        )
        self.store.save(
            experiment(
                id=self.middle_id,
                started_at="2026-01-02T00:00:00.000000Z",
                ended_at="2026-01-02T00:00:01.000000Z",
            )
        )
        self.store.save(
            experiment(
                id=self.latest_id,
                started_at="2026-01-03T00:00:00.000000Z",
                ended_at="2026-01-03T00:00:01.000000Z",
            )
        )

    def test_full_ulid_resolution(self) -> None:
        self.assertEqual(resolve_experiment(self.store, self.middle_id).id, self.middle_id)

    def test_unique_prefix_resolution_is_case_insensitive(self) -> None:
        self.assertEqual(resolve_experiment(self.store, "01hbbb").id, self.latest_id)

    def test_ambiguous_prefix_lists_candidates(self) -> None:
        with self.assertRaises(AmbiguousExperimentReference) as caught:
            resolve_experiment(self.store, "01HAAA")

        self.assertEqual(caught.exception.matches, [self.middle_id, self.oldest_id])
        self.assertIn(self.middle_id, str(caught.exception))
        self.assertIn("Provide a longer prefix", str(caught.exception))

    def test_missing_prefix_is_explicit(self) -> None:
        with self.assertRaises(MissingExperimentReference):
            resolve_experiment(self.store, "01HMISSING")

    def test_latest_and_at_one_resolve_to_most_recent(self) -> None:
        self.assertEqual(resolve_experiment(self.store, "latest").id, self.latest_id)
        self.assertEqual(resolve_experiment(self.store, "@1").id, self.latest_id)

    def test_at_two_and_at_three_use_newest_first_order(self) -> None:
        self.assertEqual(resolve_experiment(self.store, "@2").id, self.middle_id)
        self.assertEqual(resolve_experiment(self.store, "@3").id, self.oldest_id)

    def test_out_of_range_relative_reference_is_explicit(self) -> None:
        with self.assertRaises(RelativeExperimentReferenceOutOfRange) as caught:
            resolve_experiment(self.store, "@4")

        self.assertEqual(caught.exception.available, 3)
        self.assertIn("out of range", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
