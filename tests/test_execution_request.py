from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from bourneprov.cli import main
from bourneprov.execution_request import (
    MAX_REQUEST_ARGV,
    MAX_REQUEST_BYTES,
    REQUEST_KIND,
    REQUEST_SCHEMA_VERSION,
    ExecutionRequestError,
    RequestSource,
    execution_request_from_cli,
    execution_request_schema,
    load_execution_request,
    parse_execution_request,
)
from bourneprov.execution_service import (
    ExecutionService,
    PlanningError,
    request_to_workload,
)
from bourneprov.ids import new_ulid
from bourneprov.inventory_storage import InventoryStore
from bourneprov.storage import ExperimentStore
from bourneprov.workload_models import ExecutionConstraints, ResourceRequirements
from bourneprov.workload_storage import ExecutionStore
from tests.fixtures import experiment
from tests.v04_fixtures import inventory_snapshot


def minimal(**updates):
    value = {
        "kind": REQUEST_KIND,
        "version": REQUEST_SCHEMA_VERSION,
        "command": ["solver", "case.dat"],
    }
    value.update(updates)
    return value


def parse(value, base: Path):
    return parse_execution_request(
        value, base_directory=base, source=RequestSource("file")
    )


class ExecutionRequestValidationTests(unittest.TestCase):
    def test_valid_minimal_request_has_explicit_normalized_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            request = parse(minimal(), Path(directory))
        self.assertEqual(request.argv, ["solver", "case.dat"])
        self.assertEqual(request.telemetry_mode, "summary")
        self.assertEqual(request.execution.backend, "auto")
        self.assertEqual(request.verification_checks, ())

    def test_valid_full_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            request = parse(
                minimal(
                    working_directory="cases/a",
                    artifacts={"inputs": ["in.dat"], "outputs": ["out.dat"]},
                    resources={
                        "cpus": 8, "gpus": 2, "nodes": 1, "mpi_ranks": 4,
                        "memory": "4GiB", "walltime": "2h",
                    },
                    execution={"backend": "slurm", "target": "gpu", "context": "env"},
                    provenance={"parent_experiment": "latest"},
                    telemetry={"mode": "off"},
                    verification={
                        "checks": [
                            {"type": "output_exists", "path": "out.dat"},
                            {"type": "output_min_bytes", "path": "out.dat", "min_bytes": 5},
                            {"type": "output_sha256", "path": "out.dat", "sha256": "A" * 64},
                        ]
                    },
                ),
                Path(directory),
            )
        self.assertEqual(request.resources.memory_bytes, 4 * 1024**3)
        self.assertEqual(request.resources.walltime_seconds, 7200)
        self.assertEqual(request.verification_checks[-1].sha256, "a" * 64)
        self.assertEqual(request.requested_parent_experiment, "latest")
        self.assertIsNone(request.resolved_parent_experiment_id)

    def test_unknown_top_level_and_nested_fields_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            with self.assertRaisesRegex(ExecutionRequestError, "unknown execution request"):
                parse(minimal(gpu=4), base)
            with self.assertRaisesRegex(ExecutionRequestError, "unknown resources"):
                parse(minimal(resources={"gpu": 4}), base)
            with self.assertRaisesRegex(ExecutionRequestError, "unknown execution"):
                parse(minimal(execution={"partition": "gpu"}), base)
            with self.assertRaisesRegex(ExecutionRequestError, "unknown artifacts"):
                parse(minimal(artifacts={"input": ["x"]}), base)

    def test_missing_empty_and_malformed_commands_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            for value in (
                {"kind": REQUEST_KIND, "version": 1},
                minimal(command=[]),
                minimal(command=[""]),
                minimal(command="solver"),
            ):
                with self.subTest(value=value):
                    with self.assertRaises(ExecutionRequestError):
                        parse(value, base)

    def test_malformed_negative_resources_and_invalid_backend_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            for resources in (
                {"cpus": 0}, {"gpus": -1}, {"memory": "lots"},
                {"memory": "4Ki"}, {"walltime": 0}, {"walltime": "01"},
                {"nodes": True},
            ):
                with self.subTest(resources=resources):
                    with self.assertRaises(ExecutionRequestError):
                        parse(minimal(resources=resources), base)
            with self.assertRaisesRegex(ValueError, "unsupported backend"):
                parse(minimal(execution={"backend": "ssh"}), base)

    def test_request_count_and_depth_bounds_are_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            with self.assertRaisesRegex(ExecutionRequestError, "count limit"):
                parse(minimal(command=["x"] * (MAX_REQUEST_ARGV + 1)), base)
            nested: object = "x"
            for _ in range(14):
                nested = [nested]
            with self.assertRaisesRegex(ExecutionRequestError, "nesting-depth"):
                parse(minimal(extra=nested), base)

    def test_oversized_file_and_invalid_json_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            oversized = root / "oversized.json"
            oversized.write_bytes(b"x" * (MAX_REQUEST_BYTES + 1))
            with self.assertRaisesRegex(ExecutionRequestError, "exceeds"):
                load_execution_request(oversized)
            malformed = root / "bad.json"
            malformed.write_text("{not-json", encoding="utf-8")
            with self.assertRaisesRegex(ExecutionRequestError, "valid UTF-8 JSON"):
                load_execution_request(malformed)

    def test_duplicate_fields_and_oversized_in_memory_documents_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            duplicate = root / "duplicate.json"
            duplicate.write_text(
                '{"kind":"bourne.execution-request","version":1,'
                '"command":["one"],"command":["two"]}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ExecutionRequestError, "duplicate"):
                load_execution_request(duplicate)
            with self.assertRaisesRegex(ExecutionRequestError, "document exceeds"):
                parse(minimal(command=["x" * 12000] * 100), root)

    def test_invalid_checks_and_undeclared_output_paths_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            with self.assertRaisesRegex(ExecutionRequestError, "unsupported verification"):
                parse(
                    minimal(
                        artifacts={"outputs": ["out"]},
                        verification={"checks": [{"type": "run_script", "path": "out"}]},
                    ),
                    base,
                )
            with self.assertRaisesRegex(ExecutionRequestError, "not a declared output"):
                parse(
                    minimal(
                        artifacts={"outputs": ["out"]},
                        verification={"checks": [{"type": "output_exists", "path": "other"}]},
                    ),
                    base,
                )
            with self.assertRaisesRegex(ExecutionRequestError, "64-digit"):
                parse(
                    minimal(
                        artifacts={"outputs": ["out"]},
                        verification={"checks": [{"type": "output_sha256", "path": "out", "sha256": "bad"}]},
                    ),
                    base,
                )

    def test_malformed_and_unsupported_versions_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            with self.assertRaisesRegex(ExecutionRequestError, "integer"):
                parse(minimal(version="1"), base)
            with self.assertRaisesRegex(ExecutionRequestError, "unsupported"):
                parse(minimal(version=3), base)

    def test_request_file_relative_and_absolute_working_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_file = root / "case" / "bourne.json"
            request_file.parent.mkdir()
            request_file.write_text(
                json.dumps(minimal(working_directory="run")), encoding="utf-8"
            )
            relative = load_execution_request(request_file)
            absolute = parse(minimal(working_directory=str(root / "absolute")), root / "ignored")
        self.assertEqual(relative.working_directory, "run")
        self.assertEqual(
            relative.resolved_working_directory,
            str((request_file.parent / "run").resolve()),
        )
        self.assertEqual(absolute.resolved_working_directory, str((root / "absolute").resolve()))

    def test_shell_and_environment_syntax_remains_literal_and_executes_nothing(self) -> None:
        dangerous = [
            "space value", "single'quote", 'double"quote', ";", "|", "&",
            "$HOME", "${HOME}", "$(touch injected)", "`touch injected`", "λ",
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "injected"
            value = minimal(command=["solver", *dangerous], working_directory="$HOME/$(id)")
            with patch("subprocess.Popen") as popen:
                request = parse(value, root)
                workload = request_to_workload(request)
        self.assertEqual(request.argv[1:], dangerous)
        self.assertIn("$HOME", request.resolved_working_directory)
        self.assertFalse(marker.exists())
        self.assertFalse(workload.metadata["commands_executed"])
        popen.assert_not_called()

    def test_cli_and_file_requests_compile_to_semantically_equivalent_workloads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cli_request = execution_request_from_cli(
                ["solver", "case"], cwd=root, outputs=["result.txt"],
                resources=ResourceRequirements(cpus=2),
                execution=ExecutionConstraints(backend="direct"),
            )
            file_request = parse(
                minimal(
                    command=["solver", "case"],
                    artifacts={"outputs": ["result.txt"]},
                    resources={"cpus": 2},
                    execution={"backend": "direct"},
                ),
                root,
            )
        self.assertEqual(cli_request.semantic_dict(), file_request.semantic_dict())
        cli_workload = request_to_workload(cli_request)
        file_workload = request_to_workload(file_request)
        self.assertEqual(cli_workload.argv, file_workload.argv)
        self.assertEqual(cli_workload.resources, file_workload.resources)
        self.assertEqual(cli_workload.outputs, file_workload.outputs)

    def test_local_request_compilation_still_observes_bounded_project_markers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pyproject.toml").touch()
            request = execution_request_from_cli(["solver"], cwd=root)
            workload = request_to_workload(request)

        self.assertEqual(workload.working_directory, str(root.resolve()))
        self.assertEqual(workload.project_markers, ["pyproject.toml"])
        self.assertEqual(
            workload.metadata["inspection_scope"],
            "argv_and_allowlisted_cwd_markers",
        )

    def test_packaged_json_schema_identifies_the_same_contract_and_bounds(self) -> None:
        schema = execution_request_schema()
        self.assertEqual(schema["properties"]["kind"]["const"], REQUEST_KIND)
        self.assertEqual(schema["properties"]["version"]["const"], REQUEST_SCHEMA_VERSION)
        self.assertEqual(schema["properties"]["command"]["maxItems"], MAX_REQUEST_ARGV)
        self.assertEqual(
            schema["properties"]["command"]["items"]["pattern"],
            r"^[^\u0000]*$",
        )
        self.assertFalse(schema["additionalProperties"])

    def test_future_agent_fixture_maps_plain_intent_without_scheduler_mechanics(self) -> None:
        fixture = Path(__file__).parent / "data" / "future-agent-execution-request-v1.json"
        request = load_execution_request(fixture)
        self.assertEqual(request.argv, ["python", "train.py"])
        self.assertEqual(request.resources.gpus, 4)
        self.assertEqual(request.execution.backend, "auto")
        self.assertEqual(request.artifacts.outputs, ("result.h5",))
        self.assertEqual(request.verification_checks[0].type, "output_exists")


class ParentReferenceIntentTests(unittest.TestCase):
    def _parents(self, database: Path) -> tuple[str, str]:
        older_id = new_ulid(1_700_000_000_000)
        latest_id = new_ulid(1_700_000_001_000)
        store = ExperimentStore(database)
        store.save(
            experiment(
                id=older_id,
                started_at="2026-01-01T00:00:00.000000Z",
                ended_at="2026-01-01T00:00:01.000000Z",
            )
        )
        store.save(
            experiment(
                id=latest_id,
                started_at="2026-01-02T00:00:00.000000Z",
                ended_at="2026-01-02T00:00:01.000000Z",
            )
        )
        return older_id, latest_id

    def _plan_file_reference(
        self,
        root: Path,
        reference: str,
    ):
        database = root / "bourne.sqlite3"
        request_path = root / "bourne.json"
        request_path.write_text(
            json.dumps(
                minimal(
                    execution={"backend": "direct"},
                    provenance={"parent_experiment": reference},
                )
            ),
            encoding="utf-8",
        )
        snapshot = inventory_snapshot(root, executable="solver")
        InventoryStore(database).save(snapshot)
        parsed = load_execution_request(request_path)
        self.assertEqual(parsed.requested_parent_experiment, reference)
        self.assertIsNone(parsed.resolved_parent_experiment_id)
        service = ExecutionService(
            ExecutionStore(database), InventoryStore(database)
        )
        resolution = service.plan_request(parsed, snapshot)
        self.assertIsNotNone(resolution.selected)
        store = ExecutionStore(database)
        persisted = store.get_request(parsed.id)
        workload = store.get_workload(resolution.selected.workload_id)  # type: ignore[union-attr]
        self.assertEqual(persisted.id, parsed.id)
        self.assertIsNone(parsed.resolved_parent_experiment_id)
        self.assertEqual(store.count_requests(), 1)
        return persisted, workload

    def test_file_latest_preserves_request_and_compiles_canonical_parent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _older_id, latest_id = self._parents(root / "bourne.sqlite3")
            persisted, workload = self._plan_file_reference(root, "latest")
        self.assertEqual(persisted.requested_parent_experiment, "latest")
        self.assertEqual(persisted.resolved_parent_experiment_id, latest_id)
        self.assertEqual(workload.parent_experiment_id, latest_id)
        self.assertEqual(
            persisted.to_document()["provenance"]["parent_experiment"],
            "latest",
        )

    def test_unique_prefix_is_retained_while_workload_uses_canonical_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _older_id, latest_id = self._parents(root / "bourne.sqlite3")
            prefix = latest_id[:12]
            persisted, workload = self._plan_file_reference(root, prefix)
        self.assertEqual(persisted.requested_parent_experiment, prefix)
        self.assertEqual(persisted.resolved_parent_experiment_id, latest_id)
        self.assertEqual(workload.parent_experiment_id, latest_id)

    def test_full_canonical_parent_is_preserved_and_resolved_identically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            older_id, _latest_id = self._parents(root / "bourne.sqlite3")
            persisted, workload = self._plan_file_reference(root, older_id)
        self.assertEqual(persisted.requested_parent_experiment, older_id)
        self.assertEqual(persisted.resolved_parent_experiment_id, older_id)
        self.assertEqual(workload.parent_experiment_id, older_id)

    def test_relative_parent_is_retained_while_workload_uses_canonical_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            older_id, _latest_id = self._parents(root / "bourne.sqlite3")
            persisted, workload = self._plan_file_reference(root, "@2")
        self.assertEqual(persisted.requested_parent_experiment, "@2")
        self.assertEqual(persisted.resolved_parent_experiment_id, older_id)
        self.assertEqual(workload.parent_experiment_id, older_id)

    def test_invalid_and_ambiguous_parent_create_no_partial_planning_state(self) -> None:
        for kind in ("missing", "ambiguous"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                database = root / "bourne.sqlite3"
                snapshot = inventory_snapshot(root)
                InventoryStore(database).save(snapshot)
                if kind == "missing":
                    reference = "no-such-parent"
                else:
                    first = new_ulid(1_700_000_002_000)
                    second = new_ulid(1_700_000_002_000)
                    ExperimentStore(database).save(experiment(id=first))
                    ExperimentStore(database).save(
                        experiment(
                            id=second,
                            started_at="2026-01-02T00:00:00.000000Z",
                            ended_at="2026-01-02T00:00:01.000000Z",
                        )
                    )
                    reference = first[:10]
                request = parse_execution_request(
                    minimal(
                        execution={"backend": "direct"},
                        provenance={"parent_experiment": reference},
                    ),
                    base_directory=root,
                    source=RequestSource("file"),
                )
                store = ExecutionStore(database)
                service = ExecutionService(store, InventoryStore(database))
                with self.assertRaises(PlanningError):
                    service.plan_request(request, snapshot)
                with closing(sqlite3.connect(database)) as connection:
                    counts = {
                        table: connection.execute(
                            f"SELECT count(*) FROM {table}"
                        ).fetchone()[0]
                        for table in (
                            "execution_requests",
                            "execution_request_workload_links",
                            "workload_specs",
                            "execution_plans",
                        )
                    }
                self.assertEqual(set(counts.values()), {0})

    def test_cli_derived_from_uses_the_same_intent_preserving_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            _older_id, latest_id = self._parents(database)
            InventoryStore(database).save(inventory_snapshot(root))
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.cli.Path.cwd", return_value=root),
                contextlib.redirect_stdout(output),
            ):
                self.assertEqual(
                    main(
                        [
                            "plan", "--backend", "direct",
                            "--derived-from", "latest", "--json", "--",
                            sys.executable, "-c", "pass",
                        ]
                    ),
                    0,
                )
            payload = json.loads(output.getvalue())
            store = ExecutionStore(database)
            persisted = store.get_request(payload["request"]["id"])
            workload = store.get_workload(payload["selected"]["workload_id"])
        self.assertEqual(persisted.requested_parent_experiment, "latest")
        self.assertEqual(persisted.resolved_parent_experiment_id, latest_id)
        self.assertEqual(workload.parent_experiment_id, latest_id)


class RequestCliTests(unittest.TestCase):
    def test_init_validate_show_and_schema_do_not_create_database_or_execute(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path = root / "bourne.json"
            database = root / "must-not-exist.sqlite3"
            marker = root / "must-not-run"
            command = [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"]
            output = io.StringIO()
            with (
                patch.dict(os.environ, {"BOURNE_DB": str(database)}),
                patch("bourneprov.cli.Path.cwd", return_value=root),
                contextlib.redirect_stdout(output),
            ):
                self.assertEqual(main(["request", "init", "--output", str(request_path), "--", *command]), 0)
                self.assertEqual(main(["request", "validate", str(request_path)]), 0)
                self.assertEqual(main(["request", "show", str(request_path)]), 0)
                self.assertEqual(main(["request", "schema"]), 0)
            self.assertFalse(marker.exists())
            self.assertFalse(database.exists())
            self.assertEqual(json.loads(request_path.read_text(encoding="utf-8"))["command"], command)
            self.assertIn("Valid ExecutionRequest", output.getvalue())

    def test_request_cannot_be_combined_with_conflicting_flags_or_command(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path = root / "bourne.json"
            request_path.write_text(json.dumps(minimal()), encoding="utf-8")
            for argv in (
                ["plan", "--request", str(request_path), "--cpus", "2"],
                ["plan", "--request", str(request_path), "--", "other"],
                ["execute", "--request", str(request_path), "--output", "x"],
            ):
                with (
                    self.subTest(argv=argv),
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    main(argv)

    def test_request_plan_is_nonexecuting_and_execute_preserves_exact_argv(self) -> None:
        dangerous = [
            "space value", "single'quote", 'double"quote', ";touch injected",
            "|", "&", "$HOME", "${HOME}", "$(touch injected)", "`id`", "λ",
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "bourne.sqlite3"
            marker = root / "must-not-run-during-plan"
            request_path = root / "bourne.json"
            code = (
                "from pathlib import Path; import json,sys; "
                f"Path({str(marker)!r}).touch(); "
                "print(json.dumps(sys.argv[1:], ensure_ascii=False))"
            )
            request_path.write_text(
                json.dumps(
                    minimal(
                        command=[sys.executable, "-c", code, *dangerous],
                        execution={"backend": "direct"},
                    ),
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            InventoryStore(database).save(inventory_snapshot(root))
            environment = {"BOURNE_DB": str(database)}
            planned_output = io.StringIO()
            with (
                patch.dict(os.environ, environment),
                contextlib.redirect_stdout(planned_output),
            ):
                self.assertEqual(
                    main(["plan", "--request", str(request_path), "--json"]), 0
                )
            self.assertFalse(marker.exists())
            self.assertEqual(ExecutionStore(database).list_executions(), [])
            self.assertEqual(
                json.loads(planned_output.getvalue())["request"]["source"]["kind"],
                "file",
            )
            output = io.StringIO()
            with (
                patch.dict(os.environ, environment),
                contextlib.redirect_stdout(output),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(main(["execute", "--request", str(request_path)]), 0)
            execution = ExecutionStore(database).list_executions()[0]
            experiment_id = ExecutionStore(database).experiment_id(execution.id)
            recorded = ExperimentStore(database).get(experiment_id)  # type: ignore[arg-type]
            stored_request = ExecutionStore(database).request_for_execution(execution.id)
            self.assertTrue(marker.exists())
            self.assertEqual(recorded.arguments[-len(dangerous):], dangerous)
            self.assertEqual(json.loads(recorded.stdout), dangerous)
            self.assertFalse((root / "injected").exists())
            self.assertEqual(stored_request.source.kind, "file")  # type: ignore[union-attr]
            self.assertEqual(dict(stored_request.source.metadata)["path"], str(request_path.resolve()))  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
