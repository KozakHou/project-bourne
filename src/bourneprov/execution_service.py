"""Reusable planning and execution orchestration independent of the CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from .backends import (
    DEFAULT_SCHEDULER_POLL_SECONDS,
    DirectBackend,
    ExecutionBackend,
    PBSBackend,
    SchedulerBackend,
    SlurmBackend,
    Submission,
)
from .inventory_models import InventorySnapshot
from .inventory_storage import InventoryStore
from .ids import new_ulid
from .identity import current_process_identity
from .resolver import resolve_execution
from .worker_result import WorkerResult
from .workload import inspect_workload, utc_now
from .workload_models import (
    ExecutionAttempt,
    ExecutionConstraints,
    ResolutionResult,
    ResourceRequirements,
)
from .workload_storage import ExecutionStore


class PlanningError(RuntimeError):
    pass


class ExecutionService:
    def __init__(
        self,
        store: ExecutionStore,
        inventory_store: InventoryStore,
        *,
        staging_root: Path | None = None,
    ):
        self.store = store
        self.inventory_store = inventory_store
        self.staging_root = (
            staging_root
            if staging_root is not None
            else store.path.parent / "execution-staging"
        )

    def plan(
        self,
        argv: Sequence[str],
        inventory: InventorySnapshot,
        *,
        cwd: Path | None = None,
        inputs: Sequence[str] = (),
        outputs: Sequence[str] = (),
        resources: ResourceRequirements | None = None,
        constraints: ExecutionConstraints | None = None,
        parent_experiment_id: str | None = None,
    ) -> ResolutionResult:
        workload = inspect_workload(
            argv, cwd=cwd, inputs=inputs, outputs=outputs,
            resources=resources, constraints=constraints,
            parent_experiment_id=parent_experiment_id,
        )
        self.store.save_workload(workload)
        resolution = resolve_execution(workload, inventory)
        if resolution.selected is not None:
            self.store.save_plan(resolution.selected)
        return resolution

    create_plan = plan

    def execute_plan(
        self,
        plan_id: str,
        inventory: InventorySnapshot,
        *,
        backend: ExecutionBackend | None = None,
    ) -> Submission | WorkerResult:
        plan = self.store.get_plan(plan_id)
        if plan.inventory_snapshot_id != inventory.id:
            raise PlanningError("plan inventory does not match the supplied snapshot")
        workload = self.store.get_workload(plan.workload_id)
        now = utc_now()
        identity = current_process_identity()
        execution = ExecutionAttempt(
            id=new_ulid(),
            plan_id=plan.id, backend=plan.backend, state="planned",
            created_at=now, updated_at=now,
            submitting_identity=identity.username,
        )
        self.store.create_execution(execution)
        self.store.record_execution_event(
            execution.id, "identity_observed", now, identity.evidence()
        )
        selected_backend = backend or self.backend(plan.backend)
        try:
            return selected_backend.execute(execution, plan, workload, inventory)
        except Exception as exc:
            current = self.store.get_execution(execution.id)
            if current.state not in {
                "failed", "preflight_failed", "completed", "cancelled", "interrupted"
            }:
                self.store.update_execution_state(
                    execution.id, "failed", utc_now(),
                    {"phase": "backend", "error_type": type(exc).__name__},
                    error=str(exc)[:4096],
                )
            raise

    def backend(self, name: str) -> ExecutionBackend:
        if name == "direct":
            return DirectBackend(self.store)
        if name == "slurm":
            return SlurmBackend(self.store, self.staging_root)
        if name == "pbs":
            return PBSBackend(self.store, self.staging_root)
        raise PlanningError(f"unsupported execution backend: {name}")

    def get_execution(self, execution_id: str):
        return self.store.view(execution_id)

    def wait_execution(
        self,
        execution_id: str,
        *,
        poll_seconds: float = DEFAULT_SCHEDULER_POLL_SECONDS,
        timeout_seconds: float | None = None,
        backend: SchedulerBackend | None = None,
    ) -> WorkerResult:
        execution = self.store.get_execution(execution_id)
        selected = backend or self._scheduler_backend(execution.backend)
        return selected.wait(
            execution, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds
        )

    def cancel_execution(
        self,
        execution_id: str,
        *,
        backend: SchedulerBackend | None = None,
    ) -> None:
        execution = self.store.get_execution(execution_id)
        selected = backend or self._scheduler_backend(execution.backend)
        selected.cancel(execution)

    def collect_execution(
        self,
        execution_id: str,
        *,
        backend: SchedulerBackend | None = None,
    ) -> WorkerResult:
        execution = self.store.get_execution(execution_id)
        selected = backend or self._scheduler_backend(execution.backend)
        return selected.collect(execution)

    def _scheduler_backend(self, name: str) -> SchedulerBackend:
        backend = self.backend(name)
        if not isinstance(backend, SchedulerBackend):
            raise PlanningError("direct execution is synchronous")
        return backend
