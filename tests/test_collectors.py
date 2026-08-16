from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.collectors.git import collect_git
from bourneprov.collectors.system import collect_cpu, collect_system


class GitCollectorTests(unittest.TestCase):
    def test_execution_outside_git_is_supported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            provenance = collect_git(Path(directory))

        self.assertFalse(provenance.available)
        self.assertEqual(provenance.error, "not a Git repository")

    @unittest.skipUnless(shutil.which("git"), "git is required for this collector test")
    def test_clean_and_dirty_repository_states(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)
            subprocess.run(["git", "init", "-q", str(repository)], check=True)
            subprocess.run(
                ["git", "-C", str(repository), "config", "user.email", "test@example.invalid"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(repository), "config", "user.name", "Bourne Test"],
                check=True,
            )
            tracked = repository / "input.txt"
            tracked.write_text("first\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repository), "add", "input.txt"], check=True)
            subprocess.run(
                ["git", "-C", str(repository), "commit", "-qm", "fixture"], check=True
            )

            clean = collect_git(repository)
            tracked.write_text("changed\n", encoding="utf-8")
            dirty = collect_git(repository)

        self.assertTrue(clean.available)
        self.assertIsNotNone(clean.commit_sha)
        self.assertFalse(clean.dirty)
        self.assertTrue(dirty.dirty)
        self.assertEqual(clean.commit_sha, dirty.commit_sha)


class SystemCollectorTests(unittest.TestCase):
    def test_missing_nvidia_tooling_degrades_gracefully(self) -> None:
        with patch("bourneprov.collectors.system.shutil.which", return_value=None):
            provenance = collect_system()

        self.assertTrue(provenance.operating_system)
        self.assertTrue(provenance.architecture)
        self.assertFalse(provenance.gpu_available)
        self.assertEqual(provenance.gpus, [])
        self.assertEqual(provenance.gpu_error, "nvidia-smi executable not found")

    def test_nvidia_runtime_output_is_structured(self) -> None:
        query = "0, NVIDIA Test GPU, GPU-123, 555.42, 24576\n"
        summary = "NVIDIA-SMI 555.42  Driver Version: 555.42  CUDA Version: 12.5\n"
        with (
            patch("bourneprov.collectors.system.shutil.which", return_value="nvidia-smi"),
            patch(
                "bourneprov.collectors.system._run_nvidia_smi",
                side_effect=[(query, None), (summary, None)],
            ),
        ):
            provenance = collect_system()

        self.assertTrue(provenance.gpu_available)
        self.assertEqual(provenance.gpus[0]["name"], "NVIDIA Test GPU")
        self.assertEqual(provenance.nvidia_driver_version, "555.42")
        self.assertEqual(provenance.cuda_version, "12.5")
        self.assertIn("active driver", provenance.cuda_version_source or "")

    def test_failed_nvidia_diagnostic_on_stdout_is_preserved(self) -> None:
        failed = BoundedCommandResult(
            ("nvidia-smi",),
            9,
            stdout="NVIDIA-SMI could not communicate with the active driver\n",
            stderr="",
        )
        with (
            patch("bourneprov.collectors.system.shutil.which", return_value="nvidia-smi"),
            patch("bourneprov.collectors.system.run_bounded_command", return_value=failed),
        ):
            provenance = collect_system()

        self.assertFalse(provenance.gpu_available)
        self.assertIn("active driver", provenance.gpu_error or "")

    def test_lscpu_supplies_arm_model_names_when_proc_does_not(self) -> None:
        lscpu_payload = (
            '{"lscpu": ['
            '{"field": "Model name:", "data": "Cortex-X925"},'
            '{"field": "Model name:", "data": "Cortex-A725"}'
            "]}"
        )
        completed = BoundedCommandResult(
            ("lscpu", "--json"), 0, stdout=lscpu_payload, stderr=""
        )
        with (
            patch("bourneprov.collectors.system.platform.processor", return_value="aarch64"),
            patch("bourneprov.collectors.system.platform.machine", return_value="aarch64"),
            patch("bourneprov.collectors.system.Path.read_text", return_value="processor: 0\n"),
            patch("bourneprov.collectors.system.shutil.which", return_value="lscpu"),
            patch("bourneprov.collectors.system.run_bounded_command", return_value=completed),
        ):
            cpu = collect_cpu()

        self.assertEqual(cpu, "Cortex-X925 / Cortex-A725")


if __name__ == "__main__":
    unittest.main()
