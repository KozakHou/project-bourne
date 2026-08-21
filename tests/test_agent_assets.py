from __future__ import annotations

import json
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AgentAssetTests(unittest.TestCase):
    def test_python_core_stays_dependency_free_and_mcp_is_optional(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
            "project"
        ]
        self.assertEqual(project["version"], "0.6.0.dev0")
        self.assertEqual(project["dependencies"], [])
        self.assertEqual(project["optional-dependencies"]["mcp"], ["mcp>=2.0.0,<3"])

    def test_npm_launcher_has_no_runtime_dependencies_and_exact_version_coupling(self) -> None:
        package = json.loads(
            (ROOT / "packages" / "mcp" / "package.json").read_text(encoding="utf-8")
        )
        runtime = (ROOT / "packages" / "mcp" / "src" / "runtime.ts").read_text(
            encoding="utf-8"
        )
        self.assertEqual(package["name"], "@project-bourne/mcp")
        self.assertEqual(package["version"], "0.6.0-dev.0")
        self.assertNotIn("dependencies", package)
        self.assertEqual(package["engines"]["node"], ">=24")
        self.assertIn('PYTHON_VERSION = "0.6.0.dev0"', runtime)
        self.assertIn("shell: false", runtime)
        self.assertNotIn("sbatch", runtime)
        self.assertNotIn("qsub", runtime)

    def test_npm_package_carries_the_repository_apache_license_and_notice(self) -> None:
        package_root = ROOT / "packages" / "mcp"
        self.assertEqual(
            (package_root / "LICENSE").read_bytes(), (ROOT / "LICENSE").read_bytes()
        )
        self.assertEqual(
            (package_root / "NOTICE").read_bytes(), (ROOT / "NOTICE").read_bytes()
        )

    def test_portable_skill_is_vendor_neutral_and_grants_no_shell_permission(self) -> None:
        skill = (ROOT / "skills" / "project-bourne" / "SKILL.md").read_text(
            encoding="utf-8"
        )
        frontmatter = skill.split("---", 2)[1]
        self.assertIn("name: project-bourne", frontmatter)
        self.assertIn("description:", frontmatter)
        self.assertNotIn("allowed-tools", frontmatter)
        self.assertNotIn('allowed-tools: "*"', skill)
        self.assertNotIn("Claude", skill)
        self.assertNotIn("Codex", skill)
        self.assertIn("bourne_plan", skill)
        self.assertIn("MCP tool annotations", skill)


if __name__ == "__main__":
    unittest.main()
