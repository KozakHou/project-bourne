from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from bourneprov.cli import main
from bourneprov.ids import new_ulid
from bourneprov.inventory_storage import InventoryStore
from bourneprov.site_models import Site
from bourneprov.site_service import SiteService
from bourneprov.workload import utc_now
from tests.test_v07_planning import provider_document
from tests.v04_fixtures import inventory_snapshot


class SiteCLITests(unittest.TestCase):
    def test_site_add_list_show_persist_only_non_secret_ssh_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bourne.sqlite3"
            environment = {"BOURNE_DB": str(database)}
            output = io.StringIO()
            with patch.dict("os.environ", environment, clear=False), contextlib.redirect_stdout(output):
                added = main(
                    [
                        "site", "add", "imperial", "--ssh", "login.example.edu",
                        "--user", "researcher", "--port", "2222",
                        "--scheduler", "slurm", "--remote-root", "/work/researcher/project",
                    ]
                )
            value = json.loads(output.getvalue())
            self.assertEqual(added, 0)
            self.assertEqual(value["kind"], "remote_ssh")
            self.assertNotIn("password", value)
            self.assertNotIn("private_key", value)

            output = io.StringIO()
            with patch.dict("os.environ", environment, clear=False), contextlib.redirect_stdout(output):
                listed = main(["site", "list", "--json"])
            self.assertEqual(listed, 0)
            self.assertEqual(json.loads(output.getvalue())[0]["name"], "imperial")

            output = io.StringIO()
            with patch.dict("os.environ", environment, clear=False), contextlib.redirect_stdout(output):
                shown = main(["site", "show", "imperial", "--json"])
            self.assertEqual(shown, 0)
            self.assertEqual(json.loads(output.getvalue())["ssh_port"], 2222)

    def test_discover_site_routes_through_structured_site_service(self) -> None:
        snapshot = type(
            "Snapshot",
            (),
            {
                "id": new_ulid(),
                "to_dict": lambda self: {"snapshot": {"id": self.id}},
            },
        )()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ", {"BOURNE_DB": str(Path(directory) / "bourne.sqlite3")}, clear=False
        ), patch("bourneprov.cli.SiteService.discover", return_value=snapshot) as discover, patch(
            "bourneprov.cli.format_inventory", return_value="remote inventory"
        ):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = main(["discover", "--site", "imperial"])
        self.assertEqual(code, 0)
        self.assertEqual(output.getvalue().strip(), "remote inventory")
        discover.assert_called_once()
        self.assertEqual(discover.call_args.args[0], "imperial")

    def test_site_only_planning_flags_are_never_silently_ignored(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            main(["plan", "--candidate", "sha256:candidate", "python"])
        self.assertEqual(raised.exception.code, 2)

        with self.assertRaises(SystemExit) as raised:
            main(
                [
                    "execute", "--plan", new_ulid(), "--site", "cluster",
                ]
            )
        self.assertEqual(raised.exception.code, 2)

    def test_duplicate_and_remote_only_local_site_configuration_fail_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ", {"BOURNE_DB": str(Path(directory) / "bourne.sqlite3")},
            clear=False,
        ):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(["site", "add", "local"]), 0)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(["site", "add", "local"]), 2)
                self.assertEqual(
                    main(["site", "add", "other", "--remote-root", "/work"]),
                    2,
                )

    def test_cli_selection_generates_real_shapes_and_materializes_variant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            site_service = SiteService(database)
            site = site_service.add_site(
                "real-shapes", scheduler_hint="slurm",
                local_project_root=str(root),
            )
            snapshot = inventory_snapshot(
                root, scheduler_families=("slurm",),
                executable=Path(sys.executable).name,
            )
            target = replace(
                snapshot.execution_targets[0],
                authorization="observed-authorized",
            )
            snapshot = replace(
                snapshot, site_label=site.name,
                targets=[snapshot.current_target, target],
            )
            InventoryStore(database).save(snapshot)
            site_service.sites.link_inventory(site.id, snapshot.id)
            case = root / "case.json"
            original = b'{"decomposition":{"x":1,"y":1},"science":42}\n'
            case.write_bytes(original)
            provider = provider_document()
            provider["environment_requirements"] = []
            provider["launcher_requirements"] = []
            provider_path = root / "provider.json"
            provider_path.write_text(json.dumps(provider), encoding="utf-8")
            base_arguments = [
                "plan", "--site", site.id, "--snapshot", snapshot.id,
                "--provider", str(provider_path), "--input", "case.json",
                "--backend", "slurm", "--json",
                sys.executable, "case.json",
            ]
            environment = {"BOURNE_DB": str(database)}
            output = io.StringIO()
            with patch.dict("os.environ", environment, clear=False), patch(
                "bourneprov.cli.Path.cwd", return_value=root
            ), contextlib.redirect_stdout(output):
                explored_code = main(base_arguments)
            explored = json.loads(output.getvalue())
            candidate = next(
                item for item in explored["exploration"]["candidates"]
                if item["state"] == "viable"
                and item["parameters"] == {"px": 2, "py": 2}
            )
            output = io.StringIO()
            selected_arguments = [
                *base_arguments[:1],
                "--candidate", candidate["id"],
                "--trust-provider-classifications",
                *base_arguments[1:],
            ]
            with patch.dict("os.environ", environment, clear=False), patch(
                "bourneprov.cli.Path.cwd", return_value=root
            ), contextlib.redirect_stdout(output):
                selected_code = main(selected_arguments)
            plan = json.loads(output.getvalue())
            original_after = case.read_bytes()

        self.assertEqual(explored_code, 2)
        self.assertEqual(candidate["resource_shape"]["mpi_ranks"], 4)
        self.assertEqual(selected_code, 0)
        self.assertIsNotNone(plan["workload_variant_id"])
        self.assertNotEqual(plan["arguments"][-1], "case.json")
        self.assertEqual(original_after, original)


if __name__ == "__main__":
    unittest.main()
