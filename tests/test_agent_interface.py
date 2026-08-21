from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from bourneprov.agent_interface import AgentInterfaceError, BourneAgentService
from bourneprov.inventory_storage import InventoryStore
from bourneprov.storage import ExperimentStore
from bourneprov.workload_storage import ExecutionStore
from tests.v04_fixtures import inventory_snapshot


def request(command: list[str], **values: object) -> dict[str, object]:
    return {
        "kind": "bourne.execution-request",
        "version": 1,
        "command": command,
        **values,
    }


class AgentInterfaceTests(unittest.TestCase):
    def test_validation_is_canonical_normalized_and_has_no_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            service = BourneAgentService(database, cwd=root)
            result = service.validate_request(
                request(["solver", "case.dat"], working_directory="run")
            )
            exists = database.exists()

        self.assertTrue(result["valid"])
        self.assertEqual(result["kind"], "bourne.execution-request")
        self.assertEqual(result["version"], 1)
        self.assertEqual(
            result["request"]["resolved_working_directory"],
            str((root / "run").resolve()),
        )
        self.assertEqual(result["request"]["source"]["metadata"]["interface"], "mcp")
        self.assertFalse(result["persisted"])
        self.assertFalse(exists)

    def test_invalid_request_and_missing_inventory_are_machine_readable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            service = BourneAgentService(Path(directory) / "bourne.sqlite3")
            with self.assertRaises(AgentInterfaceError) as invalid:
                service.validate_request({"kind": "wrong", "version": 1, "command": ["x"]})
            with self.assertRaises(AgentInterfaceError) as missing:
                service.plan(request(["x"]))

        self.assertEqual(invalid.exception.code, "invalid_request")
        self.assertEqual(missing.exception.code, "no_inventory")

    def test_inventory_and_plan_ambiguity_are_preserved_without_guessing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            store = InventoryStore(database)
            store.save(inventory_snapshot(root))
            ambiguous_snapshot = inventory_snapshot(
                root,
                scheduler_families=("slurm", "slurm"),
                target_names=("gpu-a", "gpu-b"),
            )
            store.save(ambiguous_snapshot)
            service = BourneAgentService(database, cwd=root)
            with self.assertRaises(AgentInterfaceError) as inventory_error:
                service.inventory("0")
            with self.assertRaises(AgentInterfaceError) as plan_error:
                service.plan(
                    request(
                        [sys.executable, "-c", "pass"],
                        execution={"backend": "slurm"},
                    )
                )

        self.assertEqual(inventory_error.exception.code, "ambiguous_inventory")
        self.assertEqual(plan_error.exception.code, "unresolved_plan")
        self.assertEqual(
            len(plan_error.exception.details["resolution"]["candidates"]), 2
        )
        self.assertIsNone(plan_error.exception.details["resolution"]["selected"])

    def test_unknown_software_remains_incompatible_and_executes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            service = BourneAgentService(database, cwd=root)
            marker = root / "unknown-ran"
            with self.assertRaises(AgentInterfaceError) as error:
                service.plan(
                    request(["./not-a-solver", str(marker)])
                )
            execution_count = ExecutionStore(database).count_executions()
            marker_exists = marker.exists()

        self.assertEqual(error.exception.code, "incompatible_request")
        self.assertEqual(execution_count, 0)
        self.assertFalse(marker_exists)

    def test_plan_is_nonexecuting_then_execute_preserves_provenance_and_argv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            service = BourneAgentService(database, cwd=root)
            marker = root / "not during planning"
            literal_arguments = [
                "space value", "quote'\"", ";", "|", "&", "$HOME",
                "${HOME}", "$(touch injected)", "`touch injected`", "科學",
            ]
            program = (
                "import json,sys; from pathlib import Path; "
                f"Path({str(marker)!r}).touch(); print(json.dumps(sys.argv[1:]))"
            )
            planned = service.plan(
                request(
                    [sys.executable, "-c", program, *literal_arguments],
                    execution={"backend": "direct"},
                )
            )
            self.assertFalse(marker.exists())
            self.assertEqual(ExecutionStore(database).count_executions(), 0)

            plan_id = planned["resolution"]["selected"]["id"]
            executed = service.execute_plan(plan_id)
            execution_id = executed["result"]["execution_id"]
            view = service.execution_get(execution_id)
            experiment = ExperimentStore(database).get(view["experiment_id"])
            marker_exists = marker.exists()
            injection_exists = (root / "injected").exists()

        self.assertTrue(marker_exists)
        self.assertFalse(injection_exists)
        self.assertEqual(json.loads(experiment.stdout.strip()), literal_arguments)
        self.assertEqual(view["execution"]["state"], "completed")
        self.assertEqual(view["verification"]["aggregate_state"], "not_requested")
        self.assertEqual(view["telemetry"]["state"], "complete")
        self.assertEqual(view["request"]["command"], [sys.executable, "-c", program, *literal_arguments])

    def test_failed_verification_remains_distinct_from_completed_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            service = BourneAgentService(database, cwd=root)
            planned = service.plan(
                request(
                    [sys.executable, "-c", "print('complete process')"],
                    artifacts={"outputs": ["missing.dat"]},
                    execution={"backend": "direct"},
                    verification={
                        "checks": [{"type": "output_exists", "path": "missing.dat"}]
                    },
                )
            )
            result = service.execute_plan(planned["resolution"]["selected"]["id"])
            view = result["execution"]

        self.assertEqual(view["execution"]["state"], "completed")
        self.assertEqual(view["verification"]["aggregate_state"], "failed")

    def test_unknown_plan_unknown_execution_and_direct_controls_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            service = BourneAgentService(database, cwd=root)
            with self.assertRaises(AgentInterfaceError) as unknown_plan:
                service.execute_plan("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            with self.assertRaises(AgentInterfaceError) as unknown_execution:
                service.execution_get("01ARZ3NDEKTSV4RRFFQ69G5FAV")
            planned = service.plan(
                request(
                    [sys.executable, "-c", "pass"],
                    execution={"backend": "direct"},
                )
            )
            result = service.execute_plan(planned["resolution"]["selected"]["id"])
            execution_id = result["result"]["execution_id"]
            with self.assertRaises(AgentInterfaceError) as wait:
                service.execution_wait(execution_id)
            with self.assertRaises(AgentInterfaceError) as cancel:
                service.execution_cancel(execution_id)

        self.assertEqual(unknown_plan.exception.code, "unknown_plan")
        self.assertEqual(unknown_execution.exception.code, "unknown_execution")
        self.assertEqual(wait.exception.code, "execution_not_allowed")
        self.assertEqual(cancel.exception.code, "execution_not_allowed")

    def test_artifact_trace_is_structured_and_ambiguity_does_not_guess(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            InventoryStore(database).save(inventory_snapshot(root))
            service = BourneAgentService(database, cwd=root)
            planned = service.plan(
                request(
                    [
                        sys.executable,
                        "-c",
                        "from pathlib import Path; Path('result.txt').write_text('data')",
                    ],
                    artifacts={"outputs": ["result.txt"]},
                    execution={"backend": "direct"},
                )
            )
            result = service.execute_plan(planned["resolution"]["selected"]["id"])
            traced = service.trace_artifact("result.txt")

        self.assertEqual(
            traced["producer"]["id"], result["result"]["experiment"]["id"]
        )
        self.assertEqual(traced["artifact"]["role"], "output")


if __name__ == "__main__":
    unittest.main()
