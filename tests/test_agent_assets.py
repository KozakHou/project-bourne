from __future__ import annotations

import json
import unittest
from importlib.metadata import metadata, requires, version
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def skill_frontmatter() -> dict[str, str]:
    skill = (ROOT / "skills" / "project-bourne" / "SKILL.md").read_text(
        encoding="utf-8"
    )
    block = skill.split("---", 2)[1]
    return {
        key.strip(): value.strip()
        for line in block.splitlines()
        if ":" in line
        for key, value in [line.split(":", 1)]
    }


class AgentAssetTests(unittest.TestCase):
    def test_python_core_stays_dependency_free_and_mcp_is_optional(self) -> None:
        declared = requires("bourneprov") or []
        self.assertEqual(version("bourneprov"), "0.7.0.dev0")
        self.assertEqual(len(declared), 1)
        self.assertTrue(declared[0].startswith("mcp<3,>=2.0.0;"))
        self.assertIn('extra == "mcp"', declared[0])

    def test_uv_is_locked_development_tooling_not_runtime(self) -> None:
        project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        lock = (ROOT / "uv.lock").read_text(encoding="utf-8")
        ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        release = (ROOT / ".github" / "workflows" / "release.yml").read_text(
            encoding="utf-8"
        )
        runtime_sources = "\n".join(
            (ROOT / path).read_text(encoding="utf-8")
            for path in (
                "src/bourneprov/remote_worker.py",
                "src/bourneprov/compute_worker.py",
                "packages/mcp/src/runtime.ts",
            )
        )

        self.assertIn("dependencies = []", project)
        self.assertIn('build-backend = "setuptools.build_meta"', project)
        self.assertIn('requires-python = ">=3.10"', lock)
        self.assertIn('"mcp>=2.0.0,<3"', project)
        self.assertIn("uv sync --locked", ci)
        self.assertIn("uv run --frozen --no-sync", ci)
        self.assertIn("uv build --no-sources", ci)
        self.assertIn("uv build --no-sources", release)
        self.assertNotIn(" uv ", f" {runtime_sources.lower()} ")

    def test_npm_launcher_has_no_runtime_dependencies_and_exact_version_coupling(self) -> None:
        package = load_json(ROOT / "packages" / "mcp" / "package.json")
        runtime = (ROOT / "packages" / "mcp" / "src" / "runtime.ts").read_text(
            encoding="utf-8"
        )
        self.assertEqual(package["name"], "@project-bourne/mcp")
        self.assertEqual(package["version"], "0.7.0-dev.0")
        self.assertEqual(package["publishConfig"], {"access": "public"})
        self.assertNotIn("dependencies", package)
        self.assertEqual(package["engines"]["node"], ">=22")
        self.assertIn('PYTHON_VERSION = "0.7.0.dev0"', runtime)
        self.assertIn("shell: false", runtime)
        self.assertNotIn("sbatch", runtime)
        self.assertNotIn("qsub", runtime)

    def test_registry_and_npm_identity_version_and_transport_are_coupled(self) -> None:
        package = load_json(ROOT / "packages" / "mcp" / "package.json")
        server = load_json(ROOT / "server.json")
        registry_package = server["packages"][0]
        self.assertEqual(
            server["$schema"],
            "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json",
        )
        self.assertEqual(server["name"], "io.github.KozakHou/project-bourne")
        self.assertEqual(package["mcpName"], server["name"])
        self.assertEqual(package["version"], server["version"])
        self.assertEqual(package["version"], registry_package["version"])
        self.assertEqual(package["name"], registry_package["identifier"])
        self.assertEqual(registry_package["registryType"], "npm")
        self.assertEqual(registry_package["transport"], {"type": "stdio"})
        self.assertEqual(
            server["repository"]["url"],
            "https://github.com/KozakHou/project-bourne",
        )
        self.assertEqual(server["repository"]["source"], "github")
        self.assertIn("github.com/KozakHou/project-bourne", package["repository"]["url"])
        self.assertNotIn("remotes", server)

    def test_discovery_descriptions_are_semantic_bounded_and_truthful(self) -> None:
        package = load_json(ROOT / "packages" / "mcp" / "package.json")
        server = load_json(ROOT / "server.json")
        frontmatter = skill_frontmatter()
        registry_description = server["description"].lower()
        npm_description = package["description"].lower()
        skill_description = frontmatter["description"]

        for term in ("scientific", "run", "provenance"):
            self.assertIn(term, registry_description)
        self.assertNotEqual(
            npm_description,
            "launcher for the canonical project bourne mcp server",
        )
        for term in ("scientific", "slurm", "pbs", "provenance", "verification"):
            self.assertIn(term, npm_description)
        self.assertLessEqual(len(skill_description), 1024)
        self.assertIn("Use Project Bourne to plan", skill_description)
        self.assertIn("Trigger for simulations", skill_description)
        for term in ("numerical solvers", "ML", "HPC", "Slurm", "PBS", "artifact lineage"):
            self.assertIn(term, skill_description)

        descriptions = " ".join(
            (registry_description, npm_description, skill_description.lower())
        )
        for unsupported_claim in (
            "built-in llm",
            "built in llm",
            "natural-language interpretation",
            "natural language interpretation",
        ):
            self.assertNotIn(unsupported_claim, descriptions)

    def test_discovery_terms_cover_generic_problem_searches_without_brand_name(self) -> None:
        package = load_json(ROOT / "packages" / "mcp" / "package.json")
        server = load_json(ROOT / "server.json")
        combined = " ".join(
            (
                str(package["description"]),
                str(server["description"]),
                skill_frontmatter()["description"],
            )
        ).lower()
        concepts = {
            "scientific simulation": ("scientific", "simulation"),
            "scientific provenance": ("scientific", "provenance"),
            "reproducible experiment": ("reproducible", "experiment"),
            "HPC Slurm": ("hpc", "slurm"),
            "artifact lineage": ("artifact", "lineage"),
        }
        for terms in concepts.values():
            for term in terms:
                self.assertIn(term, combined)

    def test_npm_search_metadata_and_node_ci_are_focused(self) -> None:
        package = load_json(ROOT / "packages" / "mcp" / "package.json")
        self.assertEqual(
            set(package["keywords"]),
            {
                "mcp",
                "model-context-protocol",
                "scientific-computing",
                "scientific-workflows",
                "reproducibility",
                "provenance",
                "experiment-tracking",
                "hpc",
                "slurm",
                "pbs",
                "gpu",
                "simulation",
                "numerical-computing",
                "ai-agents",
            },
        )
        workflow = (ROOT / ".github" / "workflows" / "node-ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('node-version: ["22", "24"]', workflow)

    def test_npm_package_carries_the_repository_apache_license_and_notice(self) -> None:
        package_root = ROOT / "packages" / "mcp"
        self.assertEqual(
            (package_root / "LICENSE").read_bytes(), (ROOT / "LICENSE").read_bytes()
        )
        self.assertEqual(
            (package_root / "NOTICE").read_bytes(), (ROOT / "NOTICE").read_bytes()
        )
        package = load_json(package_root / "package.json")
        self.assertEqual(metadata("bourneprov")["License-Expression"], "Apache-2.0")
        self.assertEqual(package["license"], "Apache-2.0")
        self.assertEqual(skill_frontmatter()["license"], "Apache-2.0")

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
