from __future__ import annotations

import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.discovery_providers import (
    BoundedCommandResult,
    CondaProvider,
    ContainerProvider,
    IdentityProvider,
    ModuleProvider,
    PathExecutableProvider,
    StorageProvider,
    VirtualenvProvider,
    classify_executable,
    run_bounded_command,
)
from tests.inventory_fixtures import request, state


def result(
    stdout: str = "", stderr: str = "", returncode: int = 0,
    *, timed_out: bool = False, truncated: bool = False,
) -> BoundedCommandResult:
    return BoundedCommandResult(
        argv=("fixture",), returncode=returncode, stdout=stdout, stderr=stderr,
        timed_out=timed_out, truncated=truncated,
    )


def executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class PathDiscoveryTests(unittest.TestCase):
    def test_unknown_executable_is_discovered_without_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "executed"
            binary = root / "bourne_unknown_solver"
            executable(binary, f"#!/bin/sh\ntouch '{marker}'\n")
            output = PathExecutableProvider().discover(
                request(root, {"PATH": str(root), "HOME": str(root)}), state()
            )
            was_executed = marker.exists()

        self.assertEqual([item.name for item in output.capabilities], ["bourne_unknown_solver"])
        self.assertEqual(output.capabilities[0].classifications, [])
        self.assertFalse(was_executed, "discovery executed an unknown scientific binary")
        self.assertFalse(output.metadata["executed_discovered_programs"])

    def test_permission_duplicates_precedence_symlink_and_unusual_name(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first"
            second = root / "second"
            first.mkdir()
            second.mkdir()
            executable(first / "solver")
            executable(second / "solver")
            executable(second / "unusual name Ω")
            (second / "not-executable").write_text("x", encoding="utf-8")
            (second / "solver-link").symlink_to(second / "solver")
            output = PathExecutableProvider().discover(
                request(root, {"PATH": f"{first}{os.pathsep}{second}"}), state()
            )

        solvers = [item for item in output.capabilities if item.name == "solver"]
        self.assertEqual([item.metadata["path_precedence"] for item in solvers], [0, 1])
        names = {item.name for item in output.capabilities}
        self.assertIn("solver-link", names)
        self.assertIn("unusual name Ω", names)
        self.assertNotIn("not-executable", names)

    def test_nonexistent_and_inaccessible_entries_do_not_abort_other_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            good = root / "good"
            good.mkdir()
            executable(good / "works")
            blocked = root / "blocked"
            real_scandir = os.scandir

            def guarded(path):  # type: ignore[no-untyped-def]
                if Path(path) == blocked:
                    raise PermissionError("denied")
                return real_scandir(path)

            with patch("bourneprov.discovery_providers.os.scandir", side_effect=guarded):
                output = PathExecutableProvider().discover(
                    request(
                        root,
                        {"PATH": os.pathsep.join([str(root / "missing"), str(blocked), str(good)])},
                    ),
                    state(),
                )

        self.assertEqual([item.name for item in output.capabilities], ["works"])
        self.assertEqual(output.status, "partial")
        self.assertIn("denied", output.diagnostic or "")

    def test_no_recursive_scan_and_truncation_is_partial(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nested = root / "nested"
            nested.mkdir()
            executable(nested / "hidden")
            executable(root / "visible-a")
            executable(root / "visible-b")
            output = PathExecutableProvider().discover(
                request(root, {"PATH": str(root)}, max_directory_entries=1), state()
            )

        self.assertEqual(output.status, "partial")
        self.assertTrue(output.truncated)
        self.assertNotIn("hidden", {item.name for item in output.capabilities})

    def test_classifier_is_annotation_not_universe(self) -> None:
        self.assertEqual(classify_executable("gcc"), ["compiler"])
        self.assertEqual(classify_executable("python3.13"), ["interpreter"])
        self.assertEqual(classify_executable("new_research_solver"), [])


class StorageAndIdentityTests(unittest.TestCase):
    def test_storage_uses_only_allowlisted_nonrecursive_paths_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch = root / "scratch"
            scratch.mkdir()
            sibling_user = root / "other-readable-user"
            sibling_user.mkdir()
            (sibling_user / "private-result.dat").write_text("not in scope", encoding="utf-8")
            environment = {
                "HOME": str(root), "PROJECT": str(root), "SCRATCH": str(scratch),
                "TMPDIR": str(scratch), "TOKEN": "must-not-persist",
            }
            with patch(
                "bourneprov.discovery_providers.os.scandir",
                side_effect=AssertionError("storage must not enumerate directories"),
            ):
                output = StorageProvider().discover(request(root, environment), state())

        by_path = {item.path: item for item in output.storage}
        self.assertEqual(len(by_path), 2)
        self.assertNotIn(str(sibling_user), by_path)
        self.assertNotIn("private-result.dat", json.dumps([item.metadata for item in output.storage]))
        self.assertEqual(by_path[str(root)].role_hints, ["cwd", "home", "project"])
        self.assertEqual(by_path[str(scratch)].role_hints, ["scratch", "temporary"])
        self.assertEqual(by_path[str(root)].metadata["policy"], "unknown")
        self.assertNotIn("must-not-persist", json.dumps([item.metadata for item in output.storage]))
        self.assertFalse(output.metadata["recursive_scan"])

    def test_unavailable_storage_path_is_truthful(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = root / "missing-scratch"
            output = StorageProvider().discover(
                request(root, {"HOME": str(root), "SCRATCH": str(missing)}), state()
            )

        resource = next(item for item in output.storage if item.path == str(missing))
        self.assertFalse(resource.exists)
        self.assertIsNone(resource.readable)
        self.assertEqual(resource.metadata["policy"], "unknown")

    def test_storage_inspection_error_is_partial_without_permission_probe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            blocked = root / "blocked"
            blocked.mkdir()
            original_stat = Path.stat

            def guarded(path: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
                if path == blocked:
                    raise PermissionError("inspection denied")
                return original_stat(path, *args, **kwargs)

            with patch.object(Path, "stat", guarded):
                output = StorageProvider().discover(
                    request(root, {"HOME": str(root), "SCRATCH": str(blocked)}), state()
                )

        resource = next(item for item in output.storage if item.path == str(blocked))
        self.assertEqual(output.status, "partial")
        self.assertIsNone(resource.exists)
        self.assertIn("inspection denied", output.diagnostic or "")

    def test_identity_reads_current_groups_without_enumerating_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("grp.getgrall", side_effect=AssertionError("must not enumerate groups")):
                output = IdentityProvider().discover(request(root), state())

        self.assertIsNotNone(output.identity)
        self.assertEqual(output.identity.home, str(root))  # type: ignore[union-attr]


class EnvironmentProviderTests(unittest.TestCase):
    def test_conda_unavailable_malformed_timeout_and_valid_active_environments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value=None):
                unavailable = CondaProvider().discover(request(root), state())
            with patch("bourneprov.discovery_providers.shutil.which", return_value="/bin/conda"):
                malformed = CondaProvider().discover(
                    request(root, {"PATH": ""}, runner=lambda *a, **k: result("not-json")), state()
                )
                timeout = CondaProvider().discover(
                    request(root, {"PATH": ""}, runner=lambda *a, **k: result(timed_out=True)), state()
                )
                first = root / "same" / "env"
                second = root / "other" / "env"
                for prefix in (first, second):
                    (prefix / "bin").mkdir(parents=True)
                    (prefix / "pyvenv.cfg").write_text("", encoding="utf-8")
                    executable(prefix / "bin" / "solver")
                payload = json.dumps({"envs": [str(first), str(second)]})
                valid = CondaProvider().discover(
                    request(
                        root,
                        {"PATH": "", "CONDA_PREFIX": str(second), "CONDA_DEFAULT_ENV": "active"},
                        runner=lambda *a, **k: result(payload),
                    ),
                    state(),
                )

        self.assertEqual(unavailable.status, "unavailable")
        self.assertEqual(malformed.status, "error")
        self.assertEqual(timeout.status, "timeout")
        self.assertEqual(len(valid.contexts), 2)
        self.assertEqual([item.name for item in valid.contexts], ["env", "active"])
        self.assertEqual(valid.contexts[1].state, "active")
        self.assertEqual(len(valid.capabilities), 2)

    def test_virtualenv_active_local_malformed_and_no_home_crawl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "active"
            local = root / ".venv"
            (local / "bin").mkdir(parents=True)
            (local / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
            executable(local / "bin" / "python")
            output = VirtualenvProvider().discover(
                request(root, {"HOME": str(root), "VIRTUAL_ENV": str(active)}), state()
            )

        by_locator = {item.locator: item for item in output.contexts}
        self.assertIn(str(active), by_locator)
        self.assertIn(str(local), by_locator)
        self.assertEqual(output.status, "partial")
        self.assertFalse(output.metadata["home_crawl"])
        self.assertIn("python", {item.name for item in output.capabilities})

    def test_container_listing_is_metadata_only_and_never_captures_environment(self) -> None:
        docker_payload = json.dumps(
            {"ID": "abc", "Names": "running-one", "Image": "safe/image", "State": "running"}
        ) + "\n" + json.dumps(
            {"ID": "def", "Names": "stopped-one", "Image": "safe/other", "State": "exited",
             "Environment": "TOKEN=must-not-persist"}
        )
        calls: list[list[str]] = []

        def runner(argv, **kwargs):  # type: ignore[no-untyped-def]
            calls.append(list(argv))
            return result(docker_payload)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value="/bin/docker"):
                output = ContainerProvider("docker").discover(
                    request(root, {"PATH": ""}, runner=runner), state()
                )

        self.assertEqual([item.state for item in output.contexts], ["running", "stopped"])
        self.assertEqual(calls[0][1:3], ["ps", "--all"])
        self.assertNotIn("exec", calls[0])
        self.assertNotIn("must-not-persist", json.dumps([item.metadata for item in output.contexts]))
        self.assertFalse(output.metadata["container_exec"])
        self.assertFalse(output.metadata["environment_inspected"])

    def test_container_unavailable_permission_error_timeout_and_podman(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch("bourneprov.discovery_providers.shutil.which", return_value=None):
                unavailable = ContainerProvider("docker").discover(request(root), state())
            with patch("bourneprov.discovery_providers.shutil.which", return_value="/bin/docker"):
                denied = ContainerProvider("docker").discover(
                    request(
                        root,
                        runner=lambda *a, **k: result(
                            stderr="permission denied TOKEN=diagnostic-secret", returncode=1
                        ),
                    ),
                    state(),
                )
                timeout = ContainerProvider("docker").discover(
                    request(root, runner=lambda *a, **k: result(timed_out=True)), state()
                )
            with patch("bourneprov.discovery_providers.shutil.which", return_value="/bin/podman"):
                podman = ContainerProvider("podman").discover(
                    request(root, runner=lambda *a, **k: result('[{"Id":"p1","Names":"p","State":"running"}]')),
                    state(),
                )

        self.assertEqual(unavailable.status, "unavailable")
        self.assertEqual(denied.status, "error")
        self.assertIn("<redacted>", denied.diagnostic or "")
        self.assertNotIn("diagnostic-secret", denied.diagnostic or "")
        self.assertEqual(timeout.status, "timeout")
        self.assertEqual(podman.contexts[0].metadata["runtime"], "podman")

    def test_modules_are_current_metadata_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            absent = ModuleProvider().discover(request(root), state())
            present = ModuleProvider().discover(
                request(root, {"LOADEDMODULES": "gcc/13:mpi/4", "MODULEPATH": "/modules"}),
                state(),
            )

        self.assertEqual(absent.status, "complete")
        self.assertEqual(present.status, "partial")
        self.assertEqual([item.name for item in present.capabilities], ["gcc/13", "mpi/4"])
        self.assertFalse(present.metadata["module_commands_executed"])


class BoundedCommandTests(unittest.TestCase):
    def test_output_is_bounded_and_shell_is_never_used(self) -> None:
        completed = run_bounded_command(
            ["/bin/sh", "-c", "printf 1234567890"], max_output_bytes=4
        )

        self.assertEqual(completed.stdout, "1234")
        self.assertTrue(completed.truncated)

    @unittest.skipUnless(os.name == "posix", "bounded process-group timeout requires POSIX")
    def test_timeout_terminates_the_probe_process_group(self) -> None:
        started = time.monotonic()
        completed = run_bounded_command(
            ["/bin/sh", "-c", "sleep 5"], timeout=0.05, max_output_bytes=32
        )

        self.assertTrue(completed.timed_out)
        self.assertLess(time.monotonic() - started, 2)


if __name__ == "__main__":
    unittest.main()
