from __future__ import annotations

import io
import inspect
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
from bourneprov.site_models import PolicyApplicability, SitePolicyClaim
from bourneprov.site_service import SiteService
from bourneprov.site_planning import (
    explore_candidates,
    generate_resource_shapes,
    resource_shapes_from_inventory,
)
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


def fixed_rank_provider_document(mpi_ranks: int) -> dict[str, object]:
    return {
        "kind": "bourne.constraint-provider",
        "schema_version": 1,
        "name": "fixed-mpi-ranks",
        "provider_version": "1",
        "parameters": [
            {
                "name": "ranks",
                "classification": "execution_only",
                "classification_evidence": {
                    "kind": "provider_contract",
                    "source": "reference",
                },
                "allowed_values": [mpi_ranks],
            }
        ],
        "constraints": [
            {
                "id": "fixed-rank-count",
                "operator": "equal",
                "left": {"parameter": "ranks"},
                "right": {"resource": "mpi_ranks"},
                "hard": True,
            },
            {
                "id": "ranks-divide-cpus",
                "operator": "divisible_by",
                "left": {"resource": "total_cpus"},
                "right": {"resource": "mpi_ranks"},
                "hard": True,
            },
        ],
        "environment_requirements": [],
        "launcher_requirements": [],
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

    def _multi_node_case(self, root: Path, *, nodes: int | None = None):
        snapshot = inventory_snapshot(root, scheduler_families=("slurm",))
        scheduler_target = snapshot.execution_targets[0]
        scheduler_target = replace(
            scheduler_target,
            metadata={**scheduler_target.metadata, "cpus_per_node": "64"},
        )
        snapshot = replace(
            snapshot,
            targets=[snapshot.current_target, scheduler_target],
        )
        workload = self._workload(root)
        workload = replace(
            workload,
            resources=replace(workload.resources, cpus=512, nodes=nodes),
        )
        provider = DeclarativeConstraintProvider.from_dict(
            fixed_rank_provider_document(512)
        )
        return snapshot, workload, provider

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

    def test_real_inventory_generates_concrete_shapes_without_visible_node_authority(self) -> None:
        provider = DeclarativeConstraintProvider.from_dict(provider_document())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = inventory_snapshot(root, scheduler_families=("slurm",))
            workload = self._workload(root)
            shapes = generate_resource_shapes(snapshot, workload, provider=provider)

        self.assertTrue(shapes)
        self.assertTrue(any(item.nodes != 1 for item in shapes))
        self.assertEqual(
            {item.mpi_ranks for item in shapes}, {1, 2, 4, 8, 16}
        )
        self.assertTrue(all(item.total_cpus == item.mpi_ranks for item in shapes))
        self.assertTrue(
            all(
                item.total_cpus == item.nodes * item.cpus_per_node
                for item in shapes
            )
        )
        self.assertTrue(
            all(
                evidence.get("authorization") == "unknown"
                for shape in shapes
                for evidence in shape.evidence
                if "authorization" in evidence
            )
        )
        self.assertIn(16, {item.nodes for item in shapes})

    def test_provider_rank_requirement_derives_multi_node_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot, workload, provider = self._multi_node_case(Path(directory))
            shapes = generate_resource_shapes(snapshot, workload, provider=provider)

        by_nodes = {item.nodes: item for item in shapes}
        eight = by_nodes[8]
        sixteen = by_nodes[16]
        self.assertEqual(
            (eight.cpus_per_node, eight.mpi_ranks, eight.ranks_per_node),
            (64, 512, 64),
        )
        self.assertEqual(
            (sixteen.cpus_per_node, sixteen.mpi_ranks, sixteen.ranks_per_node),
            (32, 512, 32),
        )

    def test_applicable_hard_max_nodes_bounds_automatic_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot, workload, provider = self._multi_node_case(Path(directory))
            policy = SitePolicyClaim(
                id=new_ulid(),
                site_id=new_ulid(),
                subject="site",
                property="max_nodes",
                value=16,
                evidence_kind="site_declared",
                interpretation_status="hard_constraint",
                source_identity="official-policy",
                created_at=utc_now(),
                applicability=PolicyApplicability("global", None),
            )
            shapes = generate_resource_shapes(
                snapshot, workload, provider=provider, policy_claims=[policy]
            )

        self.assertEqual({item.nodes for item in shapes}, {8, 16})

    def test_unknown_authorization_does_not_suppress_generated_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot, workload, provider = self._multi_node_case(Path(directory))
            shapes = generate_resource_shapes(snapshot, workload, provider=provider)
            result = explore_candidates(workload, shapes, [], provider=provider)

        self.assertIn(8, {item.nodes for item in shapes})
        self.assertTrue(result.candidates)
        self.assertTrue(
            all(item.state == "unresolved" for item in result.candidates)
        )
        self.assertTrue(
            all(
                any(reason.code == "authorization_unknown" for reason in item.reasons)
                for item in result.candidates
            )
        )

    def test_explicit_node_request_disables_automatic_node_alternatives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot, workload, provider = self._multi_node_case(
                Path(directory), nodes=16
            )
            shapes = generate_resource_shapes(snapshot, workload, provider=provider)

        self.assertEqual({item.nodes for item in shapes}, {16})
        shape = shapes[0]
        self.assertEqual(shape.cpus_per_node, 32)
        self.assertEqual(shape.ranks_per_node, 32)

    def test_policy_applicability_is_shape_scoped_and_preserves_evidence_kind(self) -> None:
        site_id = new_ulid()
        gpu_policy = SitePolicyClaim(
            id=new_ulid(), site_id=site_id, subject="gpu-queue",
            property="max_nodes", value=2, evidence_kind="user_declared",
            interpretation_status="hard_constraint", source_identity="reviewed-user-input",
            created_at=utc_now(),
            applicability=PolicyApplicability("queue", "gpu"),
        )
        cpu_policy = replace(
            gpu_policy, id=new_ulid(), subject="cpu-queue", value=8,
            evidence_kind="site_declared",
            applicability=PolicyApplicability("partition", "cpu"),
        )
        with tempfile.TemporaryDirectory() as directory:
            workload = self._workload(Path(directory))
            cpu = ResourceShape(
                nodes=4, scheduler_class="cpu",
                evidence=[{"authorization": "observed-authorized"}],
            )
            gpu = ResourceShape(
                nodes=4, scheduler_class="gpu",
                evidence=[{"authorization": "observed-authorized"}],
            )
            result = explore_candidates(
                workload, [cpu, gpu], [], policy_claims=[gpu_policy, cpu_policy]
            )
            scoped_shape = ResourceShape(
                nodes=4, scheduler_class="cpu", node_class="standard",
                placement={"account": "science"},
                evidence=[{"authorization": "observed-authorized"}],
            )
            unrelated = [
                replace(
                    gpu_policy, id=new_ulid(), value=2,
                    applicability=PolicyApplicability("node_class", "gpu"),
                ),
                replace(
                    gpu_policy, id=new_ulid(), value=2,
                    applicability=PolicyApplicability("account", "other"),
                ),
                replace(
                    cpu_policy, id=new_ulid(), value=8,
                    applicability=PolicyApplicability("global"),
                ),
            ]
            unrelated_result = explore_candidates(
                workload, [scoped_shape], [], policy_claims=unrelated
            )
            matching_account_result = explore_candidates(
                workload, [scoped_shape], [],
                policy_claims=[
                    replace(
                        gpu_policy, id=new_ulid(), value=2,
                        applicability=PolicyApplicability("account", "science"),
                    )
                ],
            )

        states = {
            item.resource_shape.scheduler_class: item.state
            for item in result.candidates
        }
        self.assertEqual(states, {"cpu": "viable", "gpu": "policy_incompatible"})
        gpu_candidate = next(
            item for item in result.candidates
            if item.resource_shape.scheduler_class == "gpu"
        )
        self.assertEqual(gpu_candidate.reasons[0].evidence_kind, "user_declared")
        self.assertEqual(unrelated_result.candidates[0].state, "viable")
        self.assertEqual(
            matching_account_result.candidates[0].state, "policy_incompatible"
        )

    def test_core_policy_submission_is_typed_bounded_and_round_trips_scope(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            service = SiteService(Path(directory) / "bourne.sqlite3")
            site = service.add_site("policy-site")
            saved = service.submit_policy_claim(
                site.id, subject="cpu-account", property="max_nodes", value=8,
                evidence_kind="user_declared",
                interpretation_status="hard_constraint",
                source_identity="reviewed-user-input",
                applicability=PolicyApplicability("account", "science"),
                source_identifier="policy-v1",
            )
            reopened = service.sites.policy_claims(site.id)[0]

        parameters = inspect.signature(service.submit_policy_claim).parameters
        self.assertNotIn("command", parameters)
        self.assertNotIn("content", parameters)
        self.assertEqual(reopened, saved)
        self.assertEqual(reopened.applicability, PolicyApplicability("account", "science"))
        self.assertEqual(reopened.evidence_kind, "user_declared")

    def test_fair_search_reaches_viable_group_after_sixty_four_invalid_combinations(self) -> None:
        document = {
            "kind": "bourne.constraint-provider", "schema_version": 1,
            "name": "fair-search", "provider_version": "1",
            "parameters": [{
                "name": "choice", "classification": "unknown",
                "classification_evidence": {"kind": "unknown"},
                "allowed_values": list(range(64)),
            }],
            "constraints": [{
                "id": "minimum-nodes", "operator": "greater_or_equal",
                "left": {"resource": "nodes"}, "right": {"constant": 2},
                "hard": True, "message": "at least two nodes",
            }],
            "environment_requirements": [], "launcher_requirements": [],
        }
        provider = DeclarativeConstraintProvider.from_dict(document)
        invalid = ResourceShape(
            nodes=1, evidence=[{"authorization": "observed-authorized"}]
        )
        viable = ResourceShape(
            nodes=2, evidence=[{"authorization": "observed-authorized"}]
        )
        # Make the invalid shape the first deterministic group, as in the
        # starvation regression, without changing the resource assertion.
        if invalid.identity > viable.identity:
            for index in range(256):
                candidate = replace(invalid, architecture=f"invalid-{index}")
                if candidate.identity < viable.identity:
                    invalid = candidate
                    break
        self.assertLess(invalid.identity, viable.identity)
        with tempfile.TemporaryDirectory() as directory:
            result = explore_candidates(
                self._workload(Path(directory)), [invalid, viable], [],
                provider=provider, limit=64,
            )

        self.assertEqual(result.theoretical_count, 128)
        self.assertEqual(result.generated_count, 64)
        self.assertEqual(result.hard_pruned_count, 64)
        self.assertTrue(any(item.state == "viable" for item in result.candidates))
        self.assertFalse(result.truncated)
        self.assertIn("explored all 128", result.coverage)

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
                provider, proposer="agent", trusted_provider_contract=True,
            )
            second = materialize_json_variant(
                "workload", original, root / "stage", {"px": 4, "py": 1},
                provider, proposer="human", trusted_provider_contract=True,
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
        self.assertFalse(automatic_change_allowed(execution_only))
        self.assertTrue(
            automatic_change_allowed(execution_only, trusted_provider_contract=True)
        )
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
        self.assertFalse(
            automatic_change_allowed(replace(performance, scientific_equivalence=True))
        )
        self.assertTrue(
            automatic_change_allowed(
                replace(performance, scientific_equivalence=True),
                trusted_provider_contract=True,
            )
        )

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
