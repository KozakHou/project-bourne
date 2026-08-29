from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

try:
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters, stdio_client
except ImportError:  # Core-only installations intentionally omit the optional SDK.
    Client = None  # type: ignore[assignment]

from bourneprov.agent_interface import BourneAgentService
from bourneprov.backends import LSFBackend, PBSBackend, SlurmBackend
from bourneprov.bounded_subprocess import BoundedCommandResult
from bourneprov.inventory_storage import InventoryStore
from bourneprov.execution_request import execution_request_schema
from bourneprov.workload_storage import ExecutionStore
from tests.test_v07_planning import provider_document
from tests.v04_fixtures import inventory_snapshot

if Client is not None:
    from bourneprov.mcp_server import create_mcp_server


TOOLS = [
    "bourne_request_schema",
    "bourne_validate_request",
    "bourne_discover",
    "bourne_inventory",
    "bourne_site_list",
    "bourne_site_inspect",
    "bourne_site_discover",
    "bourne_site_policy_claim",
    "bourne_site_candidates",
    "bourne_site_select",
    "bourne_plan",
    "bourne_execute_plan",
    "bourne_execution_get",
    "bourne_execution_reconcile",
    "bourne_execution_wait",
    "bourne_execution_cancel",
    "bourne_trace_artifact",
]


def execution_request(command: list[str], **values: object) -> dict[str, object]:
    return {
        "kind": "bourne.execution-request",
        "version": 2,
        "command": command,
        **values,
    }


@unittest.skipIf(Client is None, "install bourneprov[mcp] to run MCP schema tests")
class MCPServerSchemaDocumentationTests(unittest.TestCase):
    def test_site_tool_schema_documents_parameters_lifecycle_and_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = create_mcp_server(
                BourneAgentService(root / "bourne.sqlite3", cwd=root)
            )
            listed = asyncio.run(server.list_tools())

        by_name = {tool.name: tool for tool in listed}
        documented_parameters = {
            "bourne_site_discover": {"reference"},
            "bourne_site_policy_claim": {"reference", "claim"},
            "bourne_site_candidates": {
                "reference", "request", "provider", "inventory_reference",
            },
            "bourne_site_select": {
                "request_id", "candidate_id", "selection_source", "rationale",
                "variant_approvals", "explicit_user_declarations",
                "trusted_provider_contract", "container",
            },
        }
        for tool_name, parameter_names in documented_parameters.items():
            properties = by_name[tool_name].input_schema["properties"]
            for parameter_name in parameter_names:
                self.assertTrue(
                    properties[parameter_name].get("description"),
                    f"{tool_name}.{parameter_name} requires an MCP description",
                )

        description_boundaries = {
            "bourne_site_discover": (
                "bourne_discover", "bourne_site_inspect", "never executes",
                "compact summary", "bourne_inventory",
            ),
            "bourne_site_policy_claim": (
                "bourne_site_discover", "fetches no URL", "previous claims",
            ),
            "bourne_site_candidates": (
                "bourne_site_select", "bourne_plan", "ephemeral candidate session",
            ),
            "bourne_site_select": (
                "bourne_site_candidates", "bourne_execute_plan", "server restart",
            ),
        }
        for tool_name, fragments in description_boundaries.items():
            description = by_name[tool_name].description
            for fragment in fragments:
                self.assertIn(fragment, description)

        policy_schema = by_name["bourne_site_policy_claim"].input_schema
        claim_schema = policy_schema["properties"]["claim"]
        if "$ref" in claim_schema:
            claim_schema = policy_schema["$defs"]["SitePolicyClaimDocument"]
        for name, value in claim_schema["properties"].items():
            self.assertTrue(
                value.get("description"),
                f"SitePolicyClaimDocument.{name} requires an MCP description",
            )
        applicability_schema = policy_schema["$defs"]["PolicyApplicabilityDocument"]
        for name, value in applicability_schema["properties"].items():
            self.assertTrue(
                value.get("description"),
                f"PolicyApplicabilityDocument.{name} requires an MCP description",
            )

        selection_schema = by_name["bourne_site_select"].input_schema
        for model_name in ("ContainerExecutionDocument", "ContainerMountDocument"):
            model_schema = selection_schema["$defs"][model_name]
            for name, value in model_schema["properties"].items():
                self.assertTrue(
                    value.get("description"),
                    f"{model_name}.{name} requires an MCP description",
                )
        self.assertNotIn("command", claim_schema["properties"])
        self.assertNotIn("content", claim_schema["properties"])
        self.assertNotIn("shell", " ".join(by_name))


@unittest.skipIf(Client is None, "install bourneprov[mcp] to run MCP integration tests")
class MCPServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_tool_surface_schemas_outputs_and_annotations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = create_mcp_server(
                BourneAgentService(root / "bourne.sqlite3", cwd=root)
            )
            async with Client(server) as client:
                listed = await client.list_tools()
                response = await client.call_tool("bourne_request_schema", {})

        self.assertEqual([tool.name for tool in listed.tools], TOOLS)
        by_name = {tool.name: tool for tool in listed.tools}
        request_schema = by_name["bourne_plan"].input_schema["properties"]["request"]
        self.assertFalse(request_schema["additionalProperties"])
        self.assertEqual(request_schema["properties"]["kind"]["const"], "bourne.execution-request")
        self.assertEqual(request_schema["properties"]["version"]["const"], 2)
        self.assertEqual(request_schema["properties"]["command"]["maxItems"], 4096)
        for tool in listed.tools:
            self.assertTrue(tool.description)
            self.assertIsNotNone(tool.output_schema)
            self.assertFalse(tool.output_schema["additionalProperties"])
        self.assertTrue(by_name["bourne_request_schema"].annotations.read_only_hint)
        self.assertFalse(by_name["bourne_plan"].annotations.destructive_hint)
        self.assertTrue(by_name["bourne_execute_plan"].annotations.destructive_hint)
        self.assertTrue(by_name["bourne_execution_cancel"].annotations.destructive_hint)
        self.assertEqual(
            response.structured_content["data"]["schema"],
            execution_request_schema(),
        )

    async def test_every_tool_uses_structured_results_and_two_phase_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            marker = root / "mcp marker"
            result_path = root / "result.txt"
            document = execution_request(
                [
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path; "
                        f"Path({str(marker)!r}).touch(); "
                        f"Path({str(result_path)!r}).write_text('result'); "
                        "print('child-stdout'); "
                        "import sys; print('child-stderr', file=sys.stderr)"
                    ),
                ],
                artifacts={"outputs": [str(result_path)]},
                execution={"backend": "direct"},
                verification={
                    "checks": [{"type": "output_exists", "path": str(result_path)}]
                },
            )
            server = create_mcp_server(BourneAgentService(database, cwd=root))
            async with Client(server) as client:
                schema = await client.call_tool("bourne_request_schema", {})
                validated = await client.call_tool(
                    "bourne_validate_request", {"request": document}
                )
                discovered = await client.call_tool("bourne_discover", {})
                inventory = await client.call_tool(
                    "bourne_inventory", {"reference": "@2"}
                )
                planned = await client.call_tool(
                    "bourne_plan",
                    {"request": document, "inventory_reference": "@2"},
                )
                self.assertFalse(marker.exists())
                plan_id = planned.structured_content["data"]["resolution"]["selected"]["id"]
                executed = await client.call_tool(
                    "bourne_execute_plan", {"plan_id": plan_id}
                )
                execution_id = executed.structured_content["data"]["result"]["execution_id"]
                fetched = await client.call_tool(
                    "bourne_execution_get", {"reference": execution_id}
                )
                waited = await client.call_tool(
                    "bourne_execution_wait", {"reference": execution_id}
                )
                cancelled = await client.call_tool(
                    "bourne_execution_cancel", {"reference": execution_id}
                )
                traced = await client.call_tool(
                    "bourne_trace_artifact", {"path": str(result_path)}
                )
                marker_exists_after_execution = marker.exists()

        for response in (
            schema, validated, discovered, inventory, planned, executed, fetched,
            waited, cancelled, traced,
        ):
            self.assertIsNotNone(response.structured_content)
        self.assertTrue(schema.structured_content["ok"])
        self.assertTrue(validated.structured_content["ok"])
        self.assertTrue(discovered.structured_content["ok"])
        self.assertTrue(marker_exists_after_execution)
        self.assertEqual(
            fetched.structured_content["data"]["verification"]["aggregate_state"],
            "passed",
        )
        self.assertEqual(waited.structured_content["error"]["code"], "execution_not_allowed")
        self.assertEqual(cancelled.structured_content["error"]["code"], "execution_not_allowed")
        self.assertEqual(
            traced.structured_content["data"]["producer"]["id"],
            fetched.structured_content["data"]["experiment_id"],
        )

    async def test_invalid_unknown_and_concurrent_calls_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            server = create_mcp_server(BourneAgentService(database, cwd=root))
            valid = execution_request([sys.executable, "-c", "pass"])
            invalid = {**valid, "unknown_semantic_field": True}
            async with Client(server) as client:
                rejected = await client.call_tool(
                    "bourne_validate_request", {"request": invalid}
                )
                unknown_plan = await client.call_tool(
                    "bourne_execute_plan",
                    {"plan_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV"},
                )
                reads = await asyncio.gather(
                    *(
                        client.call_tool(
                            "bourne_inventory", {"reference": "latest"}
                        )
                        for _ in range(4)
                    )
                )
                mutations = await asyncio.gather(
                    *(
                        client.call_tool("bourne_plan", {"request": valid})
                        for _ in range(2)
                    )
                )

        self.assertEqual(rejected.structured_content["error"]["code"], "invalid_request")
        self.assertEqual(unknown_plan.structured_content["error"]["code"], "unknown_plan")
        self.assertTrue(all(item.structured_content["ok"] for item in reads))
        self.assertTrue(all(item.structured_content["ok"] for item in mutations))

    async def test_site_candidate_selection_tools_use_structured_core_services(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            agent = BourneAgentService(database, cwd=root)
            site = agent.site_service.add_site(
                "fixture-hpc", scheduler_hint="slurm",
                local_project_root=str(root),
            )
            snapshot = inventory_snapshot(
                root, scheduler_families=("slurm",),
                executable=Path(sys.executable).name,
            )
            target = replace(
                snapshot.execution_targets[0],
                authorization="unknown",
            )
            snapshot = replace(
                snapshot,
                site_label=site.name,
                targets=[snapshot.current_target, target],
            )
            InventoryStore(database).save(snapshot)
            agent.site_service.sites.link_inventory(site.id, snapshot.id)
            case = root / "case.json"
            original = b'{"decomposition":{"x":1,"y":1},"science":42}\n'
            case.write_bytes(original)
            image = root / "science.sif"
            image.write_bytes(b"existing-image")
            document = execution_request(
                [sys.executable, "case.json"],
                artifacts={"inputs": ["case.json"]},
                execution={"backend": "slurm"},
            )
            provider = provider_document()
            provider["environment_requirements"] = []
            provider["launcher_requirements"] = []
            server = create_mcp_server(agent)
            async with Client(server) as client:
                listed = await client.call_tool("bourne_site_list", {})
                inspected = await client.call_tool(
                    "bourne_site_inspect", {"reference": site.id}
                )
                policy = await client.call_tool(
                    "bourne_site_policy_claim",
                    {
                        "reference": site.id,
                        "claim": {
                            "subject": "reviewed-user-account",
                            "property": "authorization",
                            "value": True,
                            "evidence_kind": "user_declared",
                            "interpretation_status": "advisory",
                            "source_identity": "mcp-test-user",
                            "applicability": {"scope": "global"},
                        },
                    },
                )
                explored = await client.call_tool(
                    "bourne_site_candidates",
                    {
                        "reference": site.id, "request": document,
                        "provider": provider,
                    },
                )
                request_id = explored.structured_content["data"]["request"]["id"]
                candidate = next(
                    item
                    for item in explored.structured_content["data"]["exploration"]["candidates"]
                    if item["state"] == "viable"
                    and item["parameters"] == {"px": 2, "py": 2}
                )
                unreviewed = await client.call_tool(
                    "bourne_site_select",
                    {
                        "request_id": request_id,
                        "candidate_id": candidate["id"],
                        "selection_source": "test-agent",
                    },
                )
                selected = await client.call_tool(
                    "bourne_site_select",
                    {
                        "request_id": request_id,
                        "candidate_id": candidate["id"],
                        "selection_source": "test-human",
                        "rationale": "focused MCP contract test",
                        "trusted_provider_contract": True,
                        "container": {
                            "runtime": "apptainer",
                            "image": str(image),
                            "mounts": [
                                {
                                    "source": str(root),
                                    "destination": "/project",
                                    "read_only": True,
                                }
                            ],
                        },
                    },
                )
                unknown = await client.call_tool(
                    "bourne_execution_reconcile",
                    {"reference": "01ARZ3NDEKTSV4RRFFQ69G5FAV"},
                )
            original_after = case.read_bytes()

        self.assertEqual(listed.structured_content["data"]["sites"][0]["id"], site.id)
        self.assertEqual(inspected.structured_content["data"]["site"]["name"], "fixture-hpc")
        self.assertTrue(policy.structured_content["ok"])
        self.assertEqual(
            policy.structured_content["data"]["policy_claim"]["evidence_kind"],
            "user_declared",
        )
        self.assertEqual(candidate["state"], "viable")
        shape = candidate["resource_shape"]
        self.assertEqual(shape["total_cpus"], 4)
        self.assertEqual(shape["mpi_ranks"], 4)
        self.assertIn(shape["nodes"], (1, 2, 4))
        self.assertEqual(shape["nodes"] * shape["cpus_per_node"], 4)
        self.assertEqual(shape["nodes"] * shape["ranks_per_node"], 4)
        self.assertEqual(
            unreviewed.structured_content["error"]["code"],
            "candidate_selection_failed",
        )
        self.assertEqual(selected.structured_content["data"]["plan"]["site_id"], site.id)
        self.assertEqual(
            selected.structured_content["data"]["plan"]["container"]["runtime"],
            "apptainer",
        )
        self.assertIsNotNone(
            selected.structured_content["data"]["plan"]["workload_variant_id"]
        )
        self.assertEqual(original_after, original)
        self.assertEqual(unknown.structured_content["error"]["code"], "unknown_execution")

    async def test_real_stdio_negotiates_current_protocol_and_keeps_stdout_pure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            document = execution_request(
                [sys.executable, "-c", "print('not-an-mcp-frame')"],
                execution={"backend": "direct"},
            )
            environment = dict(
                os.environ,
                BOURNE_DB=str(database),
                PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"),
            )
            parameters = StdioServerParameters(
                command=sys.executable,
                args=["-m", "bourneprov", "mcp"],
                env=environment,
                cwd=root,
            )
            with tempfile.TemporaryFile("w+") as diagnostics:
                async with Client(
                    stdio_client(parameters, errlog=diagnostics),
                    read_timeout_seconds=15,
                ) as client:
                    protocol = client.session.protocol_version
                    identity = client.session.server_info
                    tools = await client.list_tools()
                    planned = await client.call_tool(
                        "bourne_plan", {"request": document}
                    )
                    plan_id = planned.structured_content["data"]["resolution"]["selected"]["id"]
                    executed = await client.call_tool(
                        "bourne_execute_plan", {"plan_id": plan_id}
                    )
                diagnostics.seek(0)
                stderr = diagnostics.read()

        self.assertEqual(protocol, "2026-07-28")
        self.assertEqual(identity.name, "project-bourne")
        self.assertEqual(identity.title, "Project Bourne")
        self.assertEqual([tool.name for tool in tools.tools], TOOLS)
        self.assertTrue(executed.structured_content["ok"])
        self.assertIn("not-an-mcp-frame", stderr)

    async def test_slurm_pbs_and_lsf_lifecycle_stays_scoped_to_bourne_executions(self) -> None:
        for family, backend_type, job_ids, allocation in (
            ("slurm", SlurmBackend, ("321", "322"), {"SLURM_CPUS_ON_NODE": "8"}),
            ("pbs", PBSBackend, ("88.server", "89.server"), {"PBS_NP": "8"}),
            (
                "lsf", LSFBackend, ("4182", "4183"),
                {"LSB_DJOB_NUMPROC": "8", "LSB_MCPU_HOSTS": "n01 8"},
            ),
        ):
            with self.subTest(family=family), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database = root / "bourne.sqlite3"
                snapshot = inventory_snapshot(root, scheduler_families=(family,))
                InventoryStore(database).save(snapshot)
                calls: list[tuple[str, ...]] = []
                submitted_jobs = iter(job_ids)

                def runner(argv, **_kwargs):
                    calls.append(tuple(argv))
                    if Path(argv[0]).name in {"sbatch", "qsub", "bsub"}:
                        job_id = next(submitted_jobs)
                        return BoundedCommandResult(
                            tuple(argv), 0,
                            (
                                f"Job <{job_id}> is submitted to queue <normal>.\n"
                                if family == "lsf" else f"{job_id}\n"
                            ), ""
                        )
                    return BoundedCommandResult(tuple(argv), 0, "", "")

                store = ExecutionStore(database)
                backend = backend_type(store, root / "stage", runner=runner)
                agent = BourneAgentService(
                    database, cwd=root, backends={family: backend}
                )
                server = create_mcp_server(agent)
                document = execution_request(
                    [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; Path('scheduled.txt').write_text('ok')",
                    ],
                    artifacts={"outputs": ["scheduled.txt"]},
                    resources={"cpus": 2},
                    execution={"backend": family},
                    verification={
                        "checks": [
                            {"type": "output_exists", "path": "scheduled.txt"}
                        ]
                    },
                )
                with patch(
                    "bourneprov.backends.shutil.which",
                    side_effect=lambda command: f"/usr/bin/{command}",
                ):
                    async with Client(server) as client:
                        planned = await client.call_tool(
                            "bourne_plan", {"request": document}
                        )
                        plan_id = planned.structured_content["data"]["resolution"]["selected"]["id"]
                        submitted = await client.call_tool(
                            "bourne_execute_plan", {"plan_id": plan_id}
                        )
                        submission = submitted.structured_content["data"]["submission"]
                        execution_id = submission["execution_id"]
                        execution = store.get_execution(execution_id)
                        staging = Path(execution.staging_directory)
                        worker = subprocess.run(
                            [
                                sys.executable,
                                str(staging / "worker.pyz"),
                                str(staging / "plan.json"),
                                str(staging / "result.json"),
                                execution_id,
                            ],
                            env={**os.environ, **allocation},
                            capture_output=True,
                            text=True,
                            check=False,
                        )
                        waited = await client.call_tool(
                            "bourne_execution_wait",
                            {"reference": execution_id, "timeout_seconds": 5},
                        )

                        second_plan = await client.call_tool(
                            "bourne_plan", {"request": document}
                        )
                        second_id = second_plan.structured_content["data"]["resolution"]["selected"]["id"]
                        second_submission = await client.call_tool(
                            "bourne_execute_plan", {"plan_id": second_id}
                        )
                        cancellable_id = second_submission.structured_content["data"]["submission"]["execution_id"]
                        foreign = await client.call_tool(
                            "bourne_execution_cancel", {"reference": job_ids[0]}
                        )
                        cancelled = await client.call_tool(
                            "bourne_execution_cancel", {"reference": cancellable_id}
                        )

                self.assertEqual(worker.returncode, 0, worker.stderr)
                self.assertEqual(submission["scheduler_family"], family)
                self.assertEqual(submission["job_id"], job_ids[0])
                self.assertEqual(submission["state"], "submitted")
                self.assertEqual(
                    waited.structured_content["data"]["execution"]["verification"]["aggregate_state"],
                    "passed",
                )
                self.assertEqual(foreign.structured_content["error"]["code"], "unknown_execution")
                self.assertEqual(cancelled.structured_content["data"]["state"], "cancelled")
                cancel_command = {
                    "slurm": "scancel", "pbs": "qdel", "lsf": "bkill"
                }[family]
                cancellation_calls = [
                    call for call in calls if Path(call[0]).name == cancel_command
                ]
                self.assertEqual(len(cancellation_calls), 1)
                self.assertIn(job_ids[1], cancellation_calls[0])


if __name__ == "__main__":
    unittest.main()
