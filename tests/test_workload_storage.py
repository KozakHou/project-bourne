from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from bourneprov.inventory_storage import InventoryStore
from bourneprov.resolver import resolve_execution
from bourneprov.workload import inspect_workload, utc_now
from bourneprov.workload_models import ExecutionAttempt, ExecutionConstraints
from bourneprov.workload_storage import ExecutionStore
from tests.v04_fixtures import inventory_snapshot


class WorkloadStorageTests(unittest.TestCase):
    def _stored_plan(self, root: Path, *, backend: str = "direct"):
        database = root / "bourne.sqlite3"
        snapshot = inventory_snapshot(
            root, scheduler_families=(() if backend == "direct" else (backend,)),
            executable="solver",
        )
        InventoryStore(database).save(snapshot)
        workload = inspect_workload(
            ["solver", "case.dat"], cwd=root,
            constraints=ExecutionConstraints(backend=backend),
        )
        plan = resolve_execution(workload, snapshot).selected
        self.assertIsNotNone(plan)
        store = ExecutionStore(database)
        store.save_workload(workload)
        store.save_plan(plan)  # type: ignore[arg-type]
        return store, snapshot, workload, plan

    def test_workload_and_plan_round_trip_and_are_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store, _snapshot, workload, plan = self._stored_plan(Path(directory))
            self.assertEqual(store.get_workload(workload.id), workload)
            self.assertEqual(store.get_plan(plan.id), plan)  # type: ignore[union-attr]
            with self.assertRaises(sqlite3.IntegrityError):
                store.save_workload(workload)
            with self.assertRaises(sqlite3.IntegrityError):
                store.save_plan(plan)  # type: ignore[arg-type]

    def test_plan_rejects_target_from_another_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store, _snapshot, workload, plan = self._stored_plan(root)
            other = inventory_snapshot(root, executable="solver")
            InventoryStore(store.path).save(other)
            invalid = replace(plan, id=__import__("bourneprov.ids", fromlist=["new_ulid"]).new_ulid(), inventory_snapshot_id=other.id)  # type: ignore[arg-type]
            with self.assertRaisesRegex(ValueError, "belong to its snapshot"):
                store.save_plan(invalid)

    def test_requested_and_allocated_resources_remain_separate(self) -> None:
        from bourneprov.ids import new_ulid
        from bourneprov.workload_models import AllocationObservation

        with tempfile.TemporaryDirectory() as directory:
            store, _snapshot, _workload, plan = self._stored_plan(Path(directory))
            now = utc_now()
            execution = ExecutionAttempt(
                id=new_ulid(), plan_id=plan.id, backend="direct", state="planned",  # type: ignore[union-attr]
                created_at=now, updated_at=now, submitting_identity="fixture",
            )
            store.create_execution(execution)
            allocation = AllocationObservation(
                id=new_ulid(), execution_id=execution.id, observed_at=utc_now(),
                resources={"cpus": 64}, hosts=["node-a"],
            )
            store.save_allocation(allocation)
            view = store.view(execution.id)

        self.assertIsNone(view.plan.requested_resources.cpus)
        self.assertEqual(view.allocations[0].resources["cpus"], 64)

    def test_lifecycle_retains_append_only_events_and_current_projection(self) -> None:
        from bourneprov.ids import new_ulid

        with tempfile.TemporaryDirectory() as directory:
            store, _snapshot, _workload, plan = self._stored_plan(Path(directory))
            now = utc_now()
            execution = ExecutionAttempt(
                id=new_ulid(), plan_id=plan.id, backend="direct", state="planned",  # type: ignore[union-attr]
                created_at=now, updated_at=now, submitting_identity="fixture",
            )
            store.create_execution(execution)
            store.update_execution_state(execution.id, "preflight", utc_now())
            store.update_execution_state(execution.id, "running", utc_now())
            current = store.get_execution(execution.id)
            events = store.events(execution.id)

        self.assertEqual(current.state, "running")
        self.assertEqual([item.state for item in events], ["planned", "preflight", "running"])

    def test_execution_and_plan_references_support_prefix_and_relative(self) -> None:
        from bourneprov.ids import new_ulid
        from bourneprov.workload_references import resolve_execution_attempt, resolve_plan

        with tempfile.TemporaryDirectory() as directory:
            store, _snapshot, _workload, plan = self._stored_plan(Path(directory))
            now = utc_now()
            execution = ExecutionAttempt(
                id=new_ulid(), plan_id=plan.id, backend="direct", state="planned",  # type: ignore[union-attr]
                created_at=now, updated_at=now, submitting_identity="fixture",
            )
            store.create_execution(execution)
            latest_plan = resolve_plan(store, "latest").id
            prefix_plan = resolve_plan(store, plan.id[:12]).id  # type: ignore[union-attr]
            latest_execution = resolve_execution_attempt(store, "@1").id

        self.assertEqual(latest_plan, plan.id)  # type: ignore[union-attr]
        self.assertEqual(prefix_plan, plan.id)  # type: ignore[union-attr]
        self.assertEqual(latest_execution, execution.id)

    def test_failed_result_import_rolls_back_experiment_link_allocation_and_state(self) -> None:
        from bourneprov.ids import new_ulid
        from bourneprov.models import ExperimentLineage
        from bourneprov.workload_models import AllocationObservation
        from tests.fixtures import experiment

        with tempfile.TemporaryDirectory() as directory:
            store, _snapshot, _workload, plan = self._stored_plan(Path(directory))
            now = utc_now()
            execution = ExecutionAttempt(
                id=new_ulid(), plan_id=plan.id, backend="direct", state="planned",  # type: ignore[union-attr]
                created_at=now, updated_at=now, submitting_identity="fixture",
            )
            store.create_execution(execution)
            record = experiment(id=new_ulid())
            allocation = AllocationObservation(
                id=new_ulid(), execution_id=execution.id, observed_at=utc_now(),
                resources={"nodes": 1}, hosts=["fixture"],
            )
            lineage = ExperimentLineage(
                child_experiment_id=record.id, parent_experiment_id=new_ulid(),
                relationship="derived_from", created_at=record.ended_at,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                store.import_experiment_result(
                    execution.id, record, [], [lineage], allocation,
                    state="completed", occurred_at=record.ended_at,
                )
            with closing(sqlite3.connect(store.path)) as connection:
                experiment_count = connection.execute(
                    "SELECT count(*) FROM experiments WHERE id = ?", (record.id,)
                ).fetchone()[0]
                allocation_count = connection.execute(
                    "SELECT count(*) FROM allocations WHERE execution_id = ?", (execution.id,)
                ).fetchone()[0]
            current = store.get_execution(execution.id)
            link = store.experiment_id(execution.id)

        self.assertEqual(experiment_count, 0)
        self.assertEqual(allocation_count, 0)
        self.assertEqual(current.state, "planned")
        self.assertIsNone(link)


if __name__ == "__main__":
    unittest.main()
