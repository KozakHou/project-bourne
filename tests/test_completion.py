from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.cli import main
from bourneprov.completion import completion_script, experiment_candidates
from bourneprov.storage import ExperimentStore
from tests.fixtures import experiment


class CompletionTests(unittest.TestCase):
    def test_bash_completion_generation(self) -> None:
        script = completion_script("bash")
        self.assertIn("complete -F _bourne_completion bourne", script)
        self.assertIn('bourne completion --candidates "$current"', script)
        self.assertIn("trace", script)

    def test_completion_cli_prints_generated_script(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = main(["completion", "bash"])

        self.assertEqual(exit_code, 0)
        self.assertIn("complete -F _bourne_completion bourne", output.getvalue())

    def test_zsh_completion_generation(self) -> None:
        script = completion_script("zsh")
        self.assertIn("#compdef bourne", script)
        self.assertIn("compdef _bourne bourne", script)
        self.assertIn("trace", script)

    def test_fish_completion_generation(self) -> None:
        script = completion_script("fish")
        self.assertIn("complete -c bourne", script)
        self.assertIn("__bourne_experiment_references", script)
        self.assertIn("trace", script)

    def test_candidates_include_ids_and_relative_references(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = ExperimentStore(Path(directory) / "bourne.sqlite3")
            first_id = "01HAAA" + "0" * 20
            second_id = "01HBBB" + "1" * 20
            store.save(experiment(id=first_id))
            store.save(
                experiment(
                    id=second_id,
                    started_at="2026-01-02T00:00:00.000000Z",
                    ended_at="2026-01-02T00:00:01.000000Z",
                )
            )

            all_candidates = experiment_candidates(store)
            prefixed = experiment_candidates(store, "01HBBB")

        self.assertIn("latest", all_candidates)
        self.assertIn("@1", all_candidates)
        self.assertIn("@2", all_candidates)
        self.assertIn(first_id, all_candidates)
        self.assertEqual(prefixed, [second_id])

    def test_cli_candidates_respect_configured_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "isolated.sqlite3"
            experiment_id = "01HISOLATED" + "0" * 15
            ExperimentStore(database).save(experiment(id=experiment_id))
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["completion", "--candidates", "01HISO"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(output.getvalue().strip(), experiment_id)


if __name__ == "__main__":
    unittest.main()
