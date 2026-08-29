"""Reusable planning and execution orchestration independent of the CLI."""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .backends import (
    DEFAULT_SCHEDULER_POLL_SECONDS,
    DirectBackend,
    ExecutionBackend,
    LSFBackend,
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
from .execution_request import (
    ExecutionRequest,
    execution_request_from_cli,
)
from .references import ExperimentReferenceError, resolve_experiment
from .storage import ExperimentStore
from .worker_result import WorkerResult
from .workload import compile_workload, inspect_workload, utc_now
from .workload_models import (
    ExecutionAttempt,
    ExecutionConstraints,
    ResolutionResult,
    ResourceRequirements,
    WorkloadSpec,
)
from .workload_storage import ExecutionStore
from .site_storage import SiteStore


class PlanningError(RuntimeError):
    pass


@dataclass(frozen=True)
class RequestExecutionResult:
    request: ExecutionRequest
    resolution: ResolutionResult
    result: Submission | WorkerResult | None


def request_to_workload(request: ExecutionRequest) -> WorkloadSpec:
    """Compile normalized local intent, retaining bounded local marker discovery."""

    if (
        request.requested_parent_experiment is not None
        and request.resolved_parent_experiment_id is None
    ):
        raise PlanningError(
            "parent experiment reference must be resolved before workload compilation"
        )
    return inspect_workload(
        request.argv,
        cwd=Path(request.resolved_working_directory),
        inputs=request.artifacts.inputs,
        outputs=request.artifacts.outputs,
        resources=request.resources,
        constraints=request.execution,
        parent_experiment_id=request.resolved_parent_experiment_id,
    )


def remote_request_to_workload(request: ExecutionRequest) -> WorkloadSpec:
    """Compile intent whose resolved cwd is authoritative in a remote context."""

    if (
        request.requested_parent_experiment is not None
        and request.resolved_parent_experiment_id is None
    ):
        raise PlanningError(
            "parent experiment reference must be resolved before workload compilation"
        )
    return compile_workload(
        request.argv,
        resolved_working_directory=request.resolved_working_directory,
        inputs=request.artifacts.inputs,
        outputs=request.artifacts.outputs,
        resources=request.resources,
        constraints=request.execution,
        parent_experiment_id=request.resolved_parent_experiment_id,
        inspection_scope="argv_and_authoritative_remote_working_directory",
    )


class ExecutionService:
    def __init__(
        self,
        store: ExecutionStore,
        inventory_store: InventoryStore,
        *,
        staging_root: Path | None = None,
        remote_clients: Mapping[str, Any] | None = None,
    ):
        self.store = store
        self.inventory_store = inventory_store
        self.staging_root = (
            staging_root
            if staging_root is not None
            else store.path.parent / "execution-staging"
        )
        self.site_store = SiteStore(store.path)
        self.remote_clients = {} if remote_clients is None else dict(remote_clients)

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
        request = execution_request_from_cli(
            argv,
            cwd=cwd or Path.cwd(),
            inputs=inputs,
            outputs=outputs,
            resources=resources,
            execution=constraints,
            parent_experiment_id=parent_experiment_id,
            source_kind="sdk",
        )
        return self.plan_request(request, inventory)

    def plan_request(
        self,
        request: ExecutionRequest,
        inventory: InventorySnapshot,
    ) -> ResolutionResult:
        normalized = self._resolve_parent(request)
        workload = request_to_workload(normalized)
        self.store.save_request_with_workload(normalized, workload)
        resolution = resolve_execution(workload, inventory)
        if resolution.selected is not None:
            self.store.save_plan(resolution.selected)
        return resolution

    create_plan = plan

    def execute_request(
        self,
        request: ExecutionRequest,
        inventory: InventorySnapshot,
        *,
        backend: ExecutionBackend | None = None,
    ) -> RequestExecutionResult:
        resolution = self.plan_request(request, inventory)
        if resolution.selected is None:
            return RequestExecutionResult(
                self.store.get_request(request.id), resolution, None
            )
        result = self.execute_plan(
            resolution.selected.id,
            inventory,
            backend=backend,
        )
        return RequestExecutionResult(
            self.store.get_request(request.id), resolution, result
        )

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
        previous = self.store.latest_execution_for_plan(plan.id)
        if previous is not None and previous.state == "submission_ambiguous":
            raise PlanningError(
                f"execution {previous.id} has ambiguous submission truth; reconcile it "
                "before creating another execution from this plan"
            )
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
        selected_backend = backend or self._backend_for_plan(plan)
        try:
            return selected_backend.execute(execution, plan, workload, inventory)
        except Exception as exc:
            current = self.store.get_execution(execution.id)
            if current.state not in {
                "failed", "preflight_failed", "completed", "cancelled", "interrupted",
                "submission_ambiguous",
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
        if name == "lsf":
            return LSFBackend(self.store, self.staging_root)
        raise PlanningError(f"unsupported execution backend: {name}")

    def get_execution(self, execution_id: str):
        return self.store.view(execution_id)

    def wait_execution(
        self,
        execution_id: str,
        *,
        poll_seconds: float = DEFAULT_SCHEDULER_POLL_SECONDS,
        timeout_seconds: float | None = None,
        backend: Any | None = None,
    ) -> WorkerResult:
        execution = self.store.get_execution(execution_id)
        plan = self.store.get_plan(execution.plan_id)
        selected = backend or self._backend_for_plan(plan)
        if not hasattr(selected, "wait"):
            raise PlanningError("direct execution is synchronous")
        return selected.wait(
            execution, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds
        )

    def cancel_execution(
        self,
        execution_id: str,
        *,
        backend: Any | None = None,
    ) -> None:
        execution = self.store.get_execution(execution_id)
        plan = self.store.get_plan(execution.plan_id)
        selected = backend or self._backend_for_plan(plan)
        if not hasattr(selected, "cancel"):
            raise PlanningError("direct execution is synchronous")
        selected.cancel(execution)

    def collect_execution(
        self,
        execution_id: str,
        *,
        backend: Any | None = None,
    ) -> WorkerResult:
        execution = self.store.get_execution(execution_id)
        plan = self.store.get_plan(execution.plan_id)
        selected = backend or self._backend_for_plan(plan)
        if not hasattr(selected, "collect"):
            raise PlanningError("direct execution is synchronous")
        return selected.collect(execution)

    def _backend_for_plan(self, plan):
        if plan.site_id is None:
            return self.backend(plan.backend)
        site = self.site_store.get(plan.site_id)
        if site.kind == "local":
            return self.backend(plan.backend)
        from .remote_backend import RemoteSchedulerBackend
        from .remote_transport import OpenSSHTransport, RemoteWorkerClient

        client = self.remote_clients.get(site.id)
        if client is None:
            client = RemoteWorkerClient(site, OpenSSHTransport())
        return RemoteSchedulerBackend(
            self.store, self.site_store, site, client, self.staging_root
        )

    def _scheduler_backend(self, name: str) -> SchedulerBackend:
        backend = self.backend(name)
        if not isinstance(backend, SchedulerBackend):
            raise PlanningError("direct execution is synchronous")
        return backend

    def _resolve_parent(self, request: ExecutionRequest) -> ExecutionRequest:
        if request.requested_parent_experiment is None:
            return request
        try:
            parent = resolve_experiment(
                ExperimentStore(self.store.path),
                request.requested_parent_experiment,
            )
        except ExperimentReferenceError as exc:
            raise PlanningError(str(exc)) from exc
        return request.with_resolved_parent_experiment(parent.id)
