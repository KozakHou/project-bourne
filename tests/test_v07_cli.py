from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.cli import main
from bourneprov.ids import new_ulid
from bourneprov.site_models import Site
from bourneprov.workload import utc_now


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


if __name__ == "__main__":
    unittest.main()
