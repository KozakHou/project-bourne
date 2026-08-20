from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bourneprov.resolver import resolve_execution
from bourneprov.workload import inspect_workload
from bourneprov.workload_models import ExecutionConstraints, ResourceRequirements, WorkloadSpec
from tests.v04_fixtures import inventory_snapshot


class WorkloadInspectionTests(unittest.TestCase):
    def test_unknown_workload_is_valid_and_never_executed(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch("subprocess.Popen") as popen:
            root = Path(directory)
            spec = inspect_workload(["./new_solver", "case.dat"], cwd=root)

        self.assertEqual(spec.argv, ["./new_solver", "case.dat"])
        self.assertEqual(spec.evidence[0].state, "explicit")
        self.assertFalse(spec.metadata["commands_executed"])
        popen.assert_not_called()

    def test_inspection_is_nonrecursive_and_does_not_read_marker_contents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pyproject.toml").write_text("SECRET=do-not-read", encoding="utf-8")
            (root / ".env").write_text("TOKEN=secret", encoding="utf-8")
            nested = root / "nested"
            nested.mkdir()
            (nested / "Dockerfile").write_text("FROM private", encoding="utf-8")
            spec = inspect_workload(["solver"], cwd=root)

        self.assertEqual(spec.project_markers, ["pyproject.toml"])
        self.assertFalse(spec.metadata["recursive_scan"])
        self.assertFalse(spec.metadata["marker_contents_read"])
        self.assertNotIn(".env", str(spec.to_dict()))
        self.assertNotIn("do-not-read", str(spec.to_dict()))

    def test_explicit_resources_inputs_outputs_and_constraints_retain_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            spec = inspect_workload(
                ["solver", "case"], cwd=Path(directory), inputs=["in.dat"],
                outputs=["out.dat"],
                resources=ResourceRequirements(
                    cpus=8, gpus=2, nodes=2, mpi_ranks=16,
                    memory_bytes=4096, walltime_seconds=60,
                ),
                constraints=ExecutionConstraints(backend="slurm", target="gpu"),
            )
        states = {(item.subject, item.state) for item in spec.evidence}
        self.assertIn(("resources.gpus", "explicit"), states)
        self.assertIn(("constraints.backend", "explicit"), states)
        self.assertIn(("input", "explicit"), states)
        self.assertIn(("output", "explicit"), states)

    def test_explicit_mpi_launcher_preserves_argv_and_infers_ranks(self) -> None:
        spec = inspect_workload(["mpirun", "-np", "64", "./solver"])
        self.assertEqual(spec.argv, ["mpirun", "-np", "64", "./solver"])
        self.assertEqual(spec.resources.mpi_ranks, 64)
        self.assertEqual(spec.launcher_requirement.name, "mpirun")  # type: ignore[union-attr]
        self.assertEqual(spec.launcher_requirement.evidence_state, "inferred")  # type: ignore[union-attr]

    def test_rank_constraint_without_launcher_does_not_guess(self) -> None:
        spec = inspect_workload(
            ["./solver"], resources=ResourceRequirements(mpi_ranks=8)
        )
        self.assertIsNone(spec.launcher_requirement.name)  # type: ignore[union-attr]
        self.assertEqual(spec.argv, ["./solver"])

    def test_workload_json_round_trip_is_exact(self) -> None:
        original = inspect_workload(
            ["solver", "a b", "λ"], resources=ResourceRequirements(cpus=2)
        )
        self.assertEqual(WorkloadSpec.from_dict(original.to_dict()), original)


class ResolverTests(unittest.TestCase):
    def test_compatible_direct_is_selected_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(Path(directory), executable="solver")
            workload = inspect_workload(["solver"], cwd=Path(directory))
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.selected.backend, "direct")  # type: ignore[union-attr]
        self.assertEqual(result.candidates[0].compatibility_state, "compatible")

    def test_direct_gpu_shortfall_is_hard_incompatibility(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(Path(directory), gpu_count=1, executable="solver")
            workload = inspect_workload(
                ["solver"], cwd=Path(directory),
                resources=ResourceRequirements(gpus=4),
            )
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.candidates[0].compatibility_state, "incompatible")
        self.assertIsNone(result.selected)

    def test_scheduler_selected_when_direct_is_incompatible_and_one_target_remains(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), scheduler_families=("slurm",),
                gpu_count=0, executable="solver",
            )
            workload = inspect_workload(
                ["solver"], cwd=Path(directory),
                resources=ResourceRequirements(gpus=4),
            )
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.selected.backend, "slurm")  # type: ignore[union-attr]
        self.assertIn("scheduler authorization is unknown", result.selected.unresolved_conditions)  # type: ignore[union-attr]

    def test_multiple_scheduler_targets_are_not_guessed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), scheduler_families=("slurm", "slurm"),
                target_names=("gpu-a", "gpu-b"), executable="solver",
            )
            workload = inspect_workload(
                ["solver"], cwd=Path(directory),
                constraints=ExecutionConstraints(backend="slurm"),
            )
            result = resolve_execution(workload, snapshot)
        self.assertIsNone(result.selected)
        self.assertEqual(len(result.candidates), 2)
        self.assertIn("multiple", result.reason)

    def test_explicit_target_resolves_ambiguity_without_claiming_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), scheduler_families=("slurm", "slurm"),
                target_names=("gpu-a", "gpu-b"), executable="solver",
            )
            workload = inspect_workload(
                ["solver"], cwd=Path(directory),
                constraints=ExecutionConstraints(backend="slurm", target="gpu-b"),
            )
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.selected.backend, "slurm")  # type: ignore[union-attr]
        self.assertIn("authorization is unknown", " ".join(result.selected.unresolved_conditions))  # type: ignore[union-attr]

    def test_historical_executable_is_not_current_availability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), historical_executable="retired-solver"
            )
            workload = inspect_workload(
                ["retired-solver"], cwd=Path(directory),
                constraints=ExecutionConstraints(backend="direct"),
            )
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.candidates[0].compatibility_state, "partial")
        self.assertEqual(result.candidates[0].decision_evidence[1].state, "historical")

    def test_explicit_direct_may_select_partial_but_keeps_unresolved_conditions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(Path(directory), executable="solver")
            workload = inspect_workload(
                ["solver"], cwd=Path(directory), resources=ResourceRequirements(cpus=8),
                constraints=ExecutionConstraints(backend="direct"),
            )
            result = resolve_execution(workload, snapshot)
        self.assertIsNotNone(result.selected)
        self.assertEqual(result.selected.compatibility_state, "partial")  # type: ignore[union-attr]
        self.assertIn("CPU count", " ".join(result.selected.unresolved_conditions))  # type: ignore[union-attr]

    def test_dgx_like_direct_target_satisfies_explicit_gpu_requirement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), gpu_count=8, executable="solver"
            )
            workload = inspect_workload(
                ["solver"], cwd=Path(directory),
                resources=ResourceRequirements(gpus=8),
            )
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.selected.backend, "direct")  # type: ignore[union-attr]
        self.assertEqual(result.selected.compatibility_state, "compatible")  # type: ignore[union-attr]

    def test_slurm_and_pbs_are_ambiguous_without_explicit_policy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), scheduler_families=("slurm", "pbs"),
                gpu_count=0, executable="solver",
            )
            workload = inspect_workload(
                ["solver"], cwd=Path(directory),
                resources=ResourceRequirements(gpus=1),
            )
            result = resolve_execution(workload, snapshot)
        self.assertIsNone(result.selected)
        self.assertEqual({item.backend for item in result.candidates}, {"direct", "slurm", "pbs"})

    def test_memory_walltime_and_mpi_stay_distinct_in_scheduler_plan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = inventory_snapshot(
                Path(directory), scheduler_families=("slurm",), executable="solver"
            )
            resources = ResourceRequirements(
                cpus=16, nodes=2, mpi_ranks=8,
                memory_bytes=64 * 1024**3, walltime_seconds=7200,
            )
            workload = inspect_workload(
                ["solver"], cwd=Path(directory), resources=resources,
                constraints=ExecutionConstraints(backend="slurm"),
            )
            result = resolve_execution(workload, snapshot)
        self.assertEqual(result.selected.requested_resources, resources)  # type: ignore[union-attr]
        self.assertIn("MPI rank count has no explicit launcher", result.selected.unresolved_conditions)  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
