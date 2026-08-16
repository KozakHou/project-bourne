from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.cli import main
from bourneprov.discovery import discover_site, find_capabilities
from bourneprov.discovery_providers import (
    BourneHistoryProvider,
    CurrentEnvironmentProvider,
    CurrentTargetProvider,
    IdentityProvider,
    PathExecutableProvider,
    StorageProvider,
)
from bourneprov.inventory_storage import InventoryStore
from bourneprov.models import ExecutionContext
from bourneprov.storage import ExperimentStore
from tests.fixtures import experiment, system_provenance


class HistoryDiscoveryTests(unittest.TestCase):
    def test_current_and_historical_evidence_remain_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            current = root / "current"
            current.mkdir()
            executable = current / "current_solver"
            executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            executable.chmod(0o755)
            store = ExperimentStore(database)
            store.save(
                experiment(
                    execution_context=ExecutionContext(
                        requested_executable="old_solver",
                        resolved_executable="/retired/bin/old_solver",
                        environment_hints={"conda_environment": "old"},
                    )
                )
            )
            store.save(
                experiment(
                    id="01HFAILED" + "1" * 17,
                    status="failed", exit_code=4,
                    started_at="2026-01-02T00:00:00.000000Z",
                    ended_at="2026-01-02T00:00:01.000000Z",
                    execution_context=ExecutionContext(
                        requested_executable="old_solver",
                        resolved_executable="/retired/bin/old_solver",
                    ),
                )
            )
            with patch(
                "bourneprov.discovery_providers.collect_system",
                return_value=system_provenance(),
            ):
                snapshot = discover_site(
                    InventoryStore(database), cwd=root,
                    environment={"PATH": str(current), "HOME": str(root)},
                    providers=[
                        CurrentTargetProvider(), CurrentEnvironmentProvider(),
                        PathExecutableProvider(), BourneHistoryProvider(),
                    ],
                )

        current_matches = find_capabilities(snapshot, "current_solver")
        history_matches = find_capabilities(snapshot, "old_solver")
        self.assertEqual(current_matches[0].capability.observation_state, "observed")
        self.assertTrue(current_matches[0].evidence[0].observed_now)
        self.assertEqual(history_matches[0].capability.observation_state, "historical")
        self.assertTrue(history_matches[0].evidence[0].historical_only)
        self.assertFalse(history_matches[0].evidence[0].observed_now)
        self.assertEqual(history_matches[0].capability.metadata["completed_observations"], 1)
        self.assertEqual(history_matches[0].capability.metadata["failed_observations"], 1)
        self.assertEqual(
            history_matches[0].capability.metadata["current_availability"],
            "not_established",
        )


class DiscoveryCliTests(unittest.TestCase):
    def _seed(self, database: Path, root: Path):
        providers = [
            IdentityProvider(), CurrentTargetProvider(), CurrentEnvironmentProvider(),
            StorageProvider(), PathExecutableProvider(),
        ]
        with patch(
            "bourneprov.discovery_providers.collect_system",
            return_value=system_provenance(),
        ):
            return discover_site(
                InventoryStore(database), cwd=root,
                environment={"PATH": os.path.dirname(os.__file__), "HOME": str(root)},
                providers=providers,
            )

    def test_inventory_without_snapshot_is_explicit_and_does_not_discover(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            error = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.cli.discover_site") as discover,
                contextlib.redirect_stderr(error),
            ):
                exit_code = main(["inventory"])

        self.assertEqual(exit_code, 2)
        self.assertIn("bourne discover", error.getvalue())
        discover.assert_not_called()

    def test_inventory_json_has_stable_structured_topology(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            snapshot = self._seed(database, root)
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["inventory", "--json"])
            payload = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["snapshot"]["id"], snapshot.id)
        self.assertEqual(
            set(payload),
            {
                "snapshot", "identity", "current_target", "storage", "scheduler",
                "execution_targets", "execution_contexts", "capabilities",
                "evidence", "providers",
            },
        )
        self.assertIsInstance(payload["identity"], dict)
        self.assertIsInstance(payload["current_target"], dict)
        self.assertIsInstance(payload["storage"], list)
        self.assertIsInstance(payload["providers"], list)
        self.assertIn("status", payload["providers"][0])

    def test_exact_capability_search_human_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            binary = root / "bourne_unknown_solver"
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o755)
            with patch(
                "bourneprov.discovery_providers.collect_system",
                return_value=system_provenance(),
            ):
                discover_site(
                    InventoryStore(database), cwd=root,
                    environment={"PATH": str(root), "HOME": str(root)},
                    providers=[CurrentTargetProvider(), CurrentEnvironmentProvider(), PathExecutableProvider()],
                )
            human = io.StringIO()
            machine = io.StringIO()
            with patch.dict(os.environ, {"BOURNE_DB": str(database)}):
                with contextlib.redirect_stdout(human):
                    self.assertEqual(main(["inventory", "--find", "bourne_unknown_solver"]), 0)
                with contextlib.redirect_stdout(machine):
                    self.assertEqual(
                        main(["inventory", "--find", "bourne_unknown_solver", "--json"]), 0
                    )
            payload = json.loads(machine.getvalue())

        self.assertIn("observed", human.getvalue())
        self.assertEqual(payload["query"]["name"], "bourne_unknown_solver")
        self.assertEqual(len(payload["matches"]), 1)
        self.assertEqual(payload["matches"][0]["capability"]["name"], "bourne_unknown_solver")

    def test_discover_cli_persists_and_prints_concise_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            output = io.StringIO()
            providers = [IdentityProvider(), CurrentTargetProvider(), CurrentEnvironmentProvider()]
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.discovery.default_providers", return_value=providers),
                patch(
                    "bourneprov.discovery_providers.collect_system",
                    return_value=system_provenance(),
                ),
                contextlib.redirect_stdout(output),
            ):
                exit_code = main(["discover"])
            count = InventoryStore(database).count()

        self.assertEqual(exit_code, 0)
        self.assertEqual(count, 1)
        self.assertIn("Discovered inventory:", output.getvalue())
        self.assertIn("Topology:", output.getvalue())
        self.assertIn("Capabilities:", output.getvalue())

    def test_arbitrary_environment_secrets_and_ssh_addresses_are_not_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            environment = {
                "PATH": "",
                "HOME": str(root),
                "API_TOKEN": "token-value-must-not-persist",
                "PASSWORD": "password-value-must-not-persist",
                "CLIENT_SECRET": "secret-value-must-not-persist",
                "SSH_CONNECTION": "192.0.2.10 123 192.0.2.20 22",
            }
            providers = [
                IdentityProvider(), CurrentTargetProvider(), CurrentEnvironmentProvider(),
                StorageProvider(),
            ]
            with patch(
                "bourneprov.discovery_providers.collect_system",
                return_value=system_provenance(),
            ):
                snapshot = discover_site(
                    InventoryStore(database), cwd=root,
                    environment=environment, providers=providers,
                )
            raw = database.read_bytes()

        self.assertTrue(snapshot.current_target.metadata["ssh_session"])  # type: ignore[union-attr]
        for forbidden in (
            b"token-value-must-not-persist", b"password-value-must-not-persist",
            b"secret-value-must-not-persist", b"192.0.2.10", b"192.0.2.20",
        ):
            self.assertNotIn(forbidden, raw)


if __name__ == "__main__":
    unittest.main()
