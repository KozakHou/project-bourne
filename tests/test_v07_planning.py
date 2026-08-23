from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from bourneprov.compute_worker import execute_plan
from bourneprov.constraint_providers import (
    DeclarativeConstraintProvider,
    ProviderContractError,
    TrustedProviderRegistry,
    automatic_change_allowed,
    ensure_remote_provider_available,
    provider_schema,
)
from bourneprov.ids import new_ulid
from bourneprov.planning_models import (
    EnvironmentActivation,
    ResolvedEnvironment,
    ResourceShape,
)
from bourneprov.site_models import SitePolicyClaim
from bourneprov.site_planning import explore_candidates, resource_shapes_from_inventory
from bourneprov.variants import materialize_json_variant
from bourneprov.workload import inspect_workload, utc_now
from bourneprov.workload_models import DecisionEvidence, ExecutionPlan
from tests.v04_fixtures import inventory_snapshot


def provider_document(*, classification: str = "execution_only") -> dict[str, object]:
    return {
        "kind": "bourne.constraint-provider",
        "schema_version": 1,
        "name": "generic-decomposition",
        "provider_version": "1",
        "parameters": [
            {
                "name": "px",
                "classification": classification,
                "classification_evidence": {"kind": "provider_contract", "source": "reference"},
                "allowed_values": [1, 2, 4],
                "binding": {"kind": "json_path", "input": "case.json", "path": ["decomposition", "x"]},
            },
            {
                "name": "py",
                "classification": "execution_only",
                "classification_evidence": {"kind": "provider_contract", "source": "reference"},
                "allowed_values": [1, 2, 4],
                "binding": {"kind": "json_path", "input": "case.json", "path": ["decomposition", "y"]},
            },
        ],
        "constraints": [
            {
                "id": "decomposition-ranks",
                "operator": "equal",
                "left": {"multiply": [{"parameter": "px"}, {"parameter": "py"}]},
                "right": {"resource": "mpi_ranks"},
                "hard": True,
                "message": "decomposition product must equal MPI ranks",
            },
            {
                "id": "ranks-divide-cpus",
                "operator": "divisible_by",
                "left": {"resource": "total_cpus"},
                "right": {"resource": "mpi_ranks"},
                "hard": True,
            },
        ],
        "environment_requirements": [
            {"kind": "executable", "name": "python3", "required": True}
        ],
        "launcher_requirements": [{"kind": "launcher", "name": "mpirun"}],
    }


class DeclarativeProviderTests(unittest.TestCase):
    def test_schema_contract_and_typed_ast_are_deterministic(self) -> None:
        provider = DeclarativeConstraintProvider.from_dict(provider_document())
        shape = ResourceShape(nodes=2, cpus_per_node=4, total_cpus=8, mpi_ranks=4, ranks_per_node=2)
        first = provider.evaluate({"px": 2, "py": 2}, shape)
        second = provider.evaluate({"py": 2, "px": 2}, shape)
        self.assertEqual(first, second)
        self.assertTrue(all(item.state == "satisfied" for item in first))
        self.assertEqual(provider_schema()["properties"]["schema_version"], {"const": 1})

    def test_documented_reference_provider_is_loadable(self) -> None:
        path = Path(__file__).resolve().parents[1] / "examples" / "providers" / "generic-mpi-decomposition.json"
        provider = DeclarativeConstraintProvider.load(path)
        self.assertEqual(provider.name, "generic-mpi-decomposition")
        self.assertEqual(len(provider.constraints), 2)

    def test_arbitrary_expression_and_unknown_fields_are_rejected(self) -> None:
        document = provider_document()
        document["constraints"][0]["left"] = {"python": "__import__('os').system('id')"}  # type: ignore[index]
        with self.assertRaisesRegex(ProviderContractError, "unsupported expression"):
            DeclarativeConstraintProvider.from_dict(document)
        document = provider_document()
        document["eval"] = "2 + 2"
        with self.assertRaisesRegex(ProviderContractError, "unsupported provider fields"):
            DeclarativeConstraintProvider.from_dict(document)

        document = provider_document()
        document["source_digest"] = "unverified"
        with self.assertRaisesRegex(ProviderContractError, "canonical sha256"):
            DeclarativeConstraintProvider.from_dict(document)

    def test_trusted_code_provider_requires_explicit_enablement(self) -> None:
        registry = TrustedProviderRegistry()
        with patch("bourneprov.constraint_providers.importlib.metadata.entry_points") as points:
            points.return_value.select.return_value = []
            with self.assertRaisesRegex(ProviderContractError, "not explicitly enabled"):
                registry.load("project-file")
            points.return_value.select.assert_not_called()

    def test_unavailable_remote_code_provider_fails_without_install_or_guess(self) -> None:
        with self.assertRaisesRegex(ProviderContractError, "did not install or guess"):
            ensure_remote_provider_available("third-party-solver", declarative=False)
        ensure_remote_provider_available("portable-json", declarative=True)


class ConstraintPlanningTests(unittest.TestCase):
    def _workload(self, root: Path):
        return inspect_workload([sys.executable, "case.json"], cwd=root)

    def test_scheduler_classes_become_conservative_partial_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), scheduler_families=("slurm", "pbs")
            )

        shapes = resource_shapes_from_inventory(snapshot)

        self.assertEqual(len(shapes), 2)
        slurm = next(item for item in shapes if item.scheduler_class == "slurm-target")
        pbs = next(item for item in shapes if item.scheduler_class == "pbs-target")
        self.assertEqual(slurm.cpus_per_node, 32)
        self.assertIsNone(slurm.nodes)
        self.assertIsNone(slurm.total_cpus)
        self.assertIsNone(pbs.total_cpus)
        self.assertTrue(
            all(item["authorization"] == "unknown" for item in slurm.evidence)
        )

    def test_shape_search_is_bounded_and_reports_truncation(self) -> None:
        document = provider_document()
        for parameter in document["parameters"]:  # type: ignore[assignment]
            parameter["allowed_values"] = list(range(1, 9))
        provider = DeclarativeConstraintProvider.from_dict(document)
        with tempfile.TemporaryDirectory() as directory:
            workload = self._workload(Path(directory))
            shape = ResourceShape(
                total_cpus=64, mpi_ranks=8,
                evidence=[{"authorization": "user-declared-authorized"}],
            )
            result = explore_candidates(
                workload, [shape], [], provider=provider, limit=64
            )
        self.assertEqual(result.generated_count, 64)
        self.assertEqual(result.theoretical_count, 64)
        self.assertFalse(result.truncated)
        two_shapes = explore_candidates(
            workload, [shape, replace(shape, architecture="other")], [],
            provider=provider, limit=64,
        )
        self.assertTrue(two_shapes.truncated)
        self.assertIn("of 128", two_shapes.coverage)

    def test_normative_policy_rejects_advisory_does_not(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workload = self._workload(Path(directory))
            shape = ResourceShape(
                nodes=8, total_cpus=64,
                evidence=[{"authorization": "user-declared-authorized"}],
            )
            hard = SitePolicyClaim(
                id=new_ulid(), site_id=new_ulid(), subject="user",
                property="max_nodes", value=4, evidence_kind="site_declared",
                interpretation_status="hard_constraint", source_identity="official-doc",
                created_at=utc_now(),
            )
            advisory = replace(
                hard, id=new_ulid(), interpretation_status="advisory", value=2
            )
            rejected = explore_candidates(workload, [shape], [], policy_claims=[hard, advisory])
            advisory_only = explore_candidates(workload, [shape], [], policy_claims=[advisory])
        self.assertEqual(rejected.candidates[0].state, "policy_incompatible")
        self.assertEqual(advisory_only.candidates[0].state, "viable")

    def test_conflicting_policy_is_preserved_without_overwriting_true_maximum(self) -> None:
        site_id = new_ulid()
        claims = [
            SitePolicyClaim(
                id=new_ulid(), site_id=site_id, subject="user", property="max_nodes",
                value=value, evidence_kind="site_declared",
                interpretation_status="hard_constraint", source_identity=f"official-{value}",
                created_at=utc_now(),
            )
            for value in (4, 8)
        ]
        with tempfile.TemporaryDirectory() as directory:
            workload = self._workload(Path(directory))
            result = explore_candidates(
                workload,
                [
                    ResourceShape(nodes=4, evidence=[{"authorization": "observed-authorized"}]),
                    ResourceShape(nodes=8, evidence=[{"authorization": "observed-authorized"}]),
                ], [],
                policy_claims=claims,
            )
        states = {item.resource_shape.nodes: item.state for item in result.candidates}
        self.assertEqual(states, {4: "viable", 8: "policy_incompatible"})
        four_node = next(
            item for item in result.candidates if item.resource_shape.nodes == 4
        )
        reason = next(
            item for item in four_node.reasons
            if item.code == "policy_conflict_preserved"
        )
        self.assertEqual(reason.code, "policy_conflict_preserved")
        self.assertIn("true max_nodes remains unknown/conflicted", reason.message)

    def test_unknown_shape_fact_never_becomes_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workload = inspect_workload(
                [sys.executable], cwd=Path(directory),
                resources=replace(self._workload(Path(directory)).resources, nodes=2),
            )
            result = explore_candidates(workload, [ResourceShape(total_cpus=8)], [])
        self.assertEqual(result.candidates[0].state, "unresolved")

    def test_visible_resource_shape_does_not_imply_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workload = self._workload(Path(directory))
            visible = ResourceShape(
                nodes=1, total_cpus=8,
                evidence=[{"visibility": True, "authorization": "unknown"}],
            )
            result = explore_candidates(workload, [visible], [])
        self.assertEqual(result.candidates[0].state, "unresolved")
        self.assertEqual(
            result.candidates[0].reasons[0].code, "authorization_unknown"
        )

    def test_known_authorization_denial_cannot_be_downgraded_by_unknown_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workload = replace(
                self._workload(Path(directory)),
                resources=replace(
                    self._workload(Path(directory)).resources, nodes=2
                ),
            )
            shape = ResourceShape(
                evidence=[{"authorization": "observed-unauthorized"}]
            )

            result = explore_candidates(workload, [shape], [])

        self.assertEqual(result.candidates[0].state, "hard_invalid")
        self.assertEqual(result.hard_invalid_count, 1)

    def test_unevaluable_hard_policy_never_leaves_candidate_viable(self) -> None:
        claim = SitePolicyClaim(
            id=new_ulid(), site_id=new_ulid(), subject="account",
            property="max_nodes", value="unknown", evidence_kind="site_declared",
            interpretation_status="hard_constraint", source_identity="site-document",
            created_at=utc_now(),
        )
        with tempfile.TemporaryDirectory() as directory:
            workload = self._workload(Path(directory))
            shape = ResourceShape(
                nodes=1, evidence=[{"authorization": "observed-authorized"}]
            )

            result = explore_candidates(
                workload, [shape], [], policy_claims=[claim]
            )

        self.assertEqual(result.candidates[0].state, "unresolved")
        self.assertIn(
            "hard policy max_nodes is not numeric",
            result.candidates[0].unresolved,
        )


class VariantAndEnvironmentTests(unittest.TestCase):
    def test_variants_are_independent_and_original_is_unchanged(self) -> None:
        provider = DeclarativeConstraintProvider.from_dict(provider_document())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "case.json"
            original_raw = b'{"decomposition":{"x":1,"y":1},"science":42}\n'
            original.write_bytes(original_raw)
            first = materialize_json_variant(
                "workload", original, root / "stage", {"px": 2, "py": 2},
                provider, proposer="agent",
            )
            second = materialize_json_variant(
                "workload", original, root / "stage", {"px": 4, "py": 1},
                provider, proposer="human",
            )
            first_value = json.loads(Path(first.derived_path).read_text())
            second_value = json.loads(Path(second.derived_path).read_text())
            self.assertEqual(original.read_bytes(), original_raw)
        self.assertNotEqual(first.id, second.id)
        self.assertNotEqual(first.derived_sha256, second.derived_sha256)
        self.assertEqual(first_value["decomposition"], {"x": 2, "y": 2})
        self.assertEqual(second_value["decomposition"], {"x": 4, "y": 1})

    def test_semantic_classification_rules_and_specific_approval(self) -> None:
        execution_only = DeclarativeConstraintProvider.from_dict(provider_document()).parameter("px")
        self.assertTrue(automatic_change_allowed(execution_only))
        agent_document = provider_document()
        agent_document["parameters"][0]["classification_evidence"] = {"kind": "agent_assertion"}  # type: ignore[index]
        inferred = DeclarativeConstraintProvider.from_dict(agent_document).parameter("px")
        self.assertFalse(automatic_change_allowed(inferred))
        unknown_document = provider_document(classification="unknown")
        unknown = DeclarativeConstraintProvider.from_dict(unknown_document).parameter("px")
        self.assertFalse(automatic_change_allowed(unknown))
        self.assertTrue(automatic_change_allowed(unknown, explicit_change_approval=True))
        self.assertEqual(unknown.classification, "unknown")
        performance = replace(execution_only, classification="performance_tunable")
        self.assertFalse(automatic_change_allowed(performance))
        self.assertTrue(automatic_change_allowed(replace(performance, scientific_equivalence=True)))

    def test_existing_environment_activation_is_scoped_and_failure_prevents_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = inventory_snapshot(root, executable=Path(sys.executable).name)
            workload = inspect_workload([sys.executable, "-c", "print('never')"], cwd=root)
            plan = ExecutionPlan(
                id=new_ulid(), workload_id=workload.id,
                inventory_snapshot_id=snapshot.id, backend="direct",
                access_target_id=snapshot.current_target.id,  # type: ignore[union-attr]
                execution_target_id=None, execution_context_id=snapshot.execution_contexts[0].id,
                scheduler_id=None, requested_resources=workload.resources,
                executable=workload.executable, arguments=workload.arguments,
                working_directory=workload.working_directory, inputs=[], outputs=[],
                compatibility_state="compatible", unresolved_conditions=[],
                decision_evidence=[DecisionEvidence("observed", "environment", "fixture")],
                created_at=utc_now(),
                environment=ResolvedEnvironment(
                    snapshot.execution_contexts[0].id, "missing", "virtualenv",
                    "compatible", EnvironmentActivation("virtualenv", prefix=str(root / "missing")),
                ),
            )
            before = dict(os.environ)
            with patch("bourneprov.compute_worker.run_experiment") as run:
                result = execute_plan(plan, workload, new_ulid())
            run.assert_not_called()
            self.assertEqual(os.environ, before)
        self.assertEqual(result.state, "preflight_failed")
        self.assertIn("planned environment prefix", result.error)

    def test_reproduced_environment_is_used_and_recorded_without_global_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prefix = root / "existing-environment"
            (prefix / "bin").mkdir(parents=True)
            snapshot = inventory_snapshot(root, executable=Path(sys.executable).name)
            workload = inspect_workload(
                [sys.executable, "-c", "import os; print(os.environ['VIRTUAL_ENV'])"],
                cwd=root,
            )
            plan = ExecutionPlan(
                id=new_ulid(), workload_id=workload.id,
                inventory_snapshot_id=snapshot.id, backend="direct",
                access_target_id=snapshot.current_target.id,  # type: ignore[union-attr]
                execution_target_id=None,
                execution_context_id=snapshot.execution_contexts[0].id,
                scheduler_id=None, requested_resources=workload.resources,
                executable=workload.executable, arguments=workload.arguments,
                working_directory=workload.working_directory, inputs=[], outputs=[],
                compatibility_state="compatible", unresolved_conditions=[],
                decision_evidence=[
                    DecisionEvidence("observed", "environment", "fixture")
                ],
                created_at=utc_now(),
                environment=ResolvedEnvironment(
                    snapshot.execution_contexts[0].id, "existing", "virtualenv",
                    "compatible",
                    EnvironmentActivation("virtualenv", prefix=str(prefix)),
                ),
            )
            before = dict(os.environ)
            result = execute_plan(
                plan, workload, new_ulid(),
                stdout_stream=io.StringIO(), stderr_stream=io.StringIO(),
            )

        self.assertEqual(result.state, "completed")
        self.assertEqual(os.environ, before)
        self.assertEqual(
            result.experiment.execution_context.environment_hints["virtual_environment"],
            str(prefix),
        )
        self.assertEqual(result.experiment.stdout.strip(), str(prefix))


if __name__ == "__main__":
    unittest.main()
