from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from bourneprov.collectors.execution_context import (
    collect_execution_context,
    resolve_executable,
)
from bourneprov.lifecycle import run_and_record
from bourneprov.storage import ExperimentStore
from tests.fixtures import system_provenance


class ExecutionContextTests(unittest.TestCase):
    def test_requested_and_resolved_path_executable_are_captured(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = collect_execution_context([sys.executable, "-V"], root, {"PATH": ""})

        self.assertEqual(context.requested_executable, sys.executable)
        self.assertEqual(context.resolved_executable, str(Path(sys.executable).resolve()))
        self.assertEqual(context.recorder_executable, str(Path(sys.executable).resolve()))

    def test_path_lookup_uses_safe_standard_resolution(self) -> None:
        root = Path.cwd()
        with patch(
            "bourneprov.collectors.execution_context.shutil.which",
            return_value="/opt/tools/unknown-solver",
        ) as which:
            resolved = resolve_executable("unknown-solver", root, {"PATH": "/opt/tools"})

        which.assert_called_once_with("unknown-solver", path="/opt/tools")
        self.assertEqual(resolved, "/opt/tools/unknown-solver")

    def test_unresolved_executable_is_explicit(self) -> None:
        with patch(
            "bourneprov.collectors.execution_context.shutil.which", return_value=None
        ):
            context = collect_execution_context(
                ["software-bourne-has-never-seen"], Path.cwd(), {"PATH": ""}
            )

        self.assertEqual(context.requested_executable, "software-bourne-has-never-seen")
        self.assertIsNone(context.resolved_executable)

    def test_virtualenv_and_conda_metadata_are_allow_listed(self) -> None:
        environment = {
            "PATH": "",
            "VIRTUAL_ENV": "/safe/project/.venv",
            "CONDA_PREFIX": "/safe/conda/envs/science",
            "CONDA_DEFAULT_ENV": "science",
            "API_TOKEN": "must-not-be-recorded",
            "PASSWORD": "must-not-be-recorded",
            "UNRELATED": "must-not-be-recorded",
        }
        with patch(
            "bourneprov.collectors.execution_context.shutil.which", return_value=None
        ):
            context = collect_execution_context(["solver"], Path.cwd(), environment)

        self.assertEqual(
            context.environment_hints,
            {
                "virtual_environment": "/safe/project/.venv",
                "conda_prefix": "/safe/conda/envs/science",
                "conda_environment": "science",
            },
        )
        self.assertNotIn("must-not-be-recorded", json.dumps(context.environment_hints))

    def test_absent_environment_metadata_stays_empty(self) -> None:
        with patch(
            "bourneprov.collectors.execution_context.shutil.which", return_value=None
        ):
            context = collect_execution_context(["solver"], Path.cwd(), {"PATH": ""})

        self.assertEqual(context.environment_hints, {})

    def test_secret_variables_are_not_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            environment = {
                "PATH": os.environ.get("PATH", ""),
                "API_TOKEN": "top-secret-value",
                "VIRTUAL_ENV": "/safe/.venv",
            }
            with (
                patch.dict(os.environ, environment, clear=True),
                patch(
                    "bourneprov.lifecycle.collect_system",
                    return_value=system_provenance(),
                ),
            ):
                experiment = run_and_record(
                    [sys.executable, "-c", "pass"],
                    ExperimentStore(database),
                    cwd=root,
                )
            with closing(sqlite3.connect(database)) as connection:
                raw = connection.execute(
                    "SELECT execution_context_json FROM experiments WHERE id = ?",
                    (experiment.id,),
                ).fetchone()[0]

        self.assertNotIn("top-secret-value", raw)
        self.assertNotIn("API_TOKEN", raw)
        self.assertIn("virtual_environment", raw)


if __name__ == "__main__":
    unittest.main()
