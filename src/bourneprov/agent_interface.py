"""Structured, vendor-neutral agent operations built on Bourne core APIs."""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, NoReturn

from .backends import (
    BackendError,
    DirectBackend,
    ExecutionBackend,
    SchedulerBackend,
    Submission,
)
from .config import default_database_path
from .discovery import discover_site
from .execution_request import (
    REQUEST_KIND,
    REQUEST_SCHEMA_VERSION,
    ExecutionRequest,
    ExecutionRequestError,
    RequestSource,
    execution_request_schema,
    parse_execution_request,
)
from .execution_service import ExecutionService, PlanningError
from .inventory_references import InventoryReferenceError, resolve_inventory
from .inventory_storage import InventoryStore
from .storage import ExperimentStore
from .tracing import (
    AmbiguousArtifactReference,
    ArtifactTrace,
    MissingArtifactReference,
    trace_artifact,
)
from .worker_result import WorkerResult
from .workload_references import WorkloadReferenceError, resolve_execution_attempt
from .workload_storage import (
    ExecutionNotFound,
    ExecutionStore,
    PlanNotFound,
)

MAX_WAIT_SECONDS = 7 * 24 * 60 * 60


class AgentInterfaceError(RuntimeError):
    """A bounded product error suitable for a structured agent response."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = {} if details is None else details

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class BourneAgentService:
    """Expose bounded Bourne operations without depending on an agent protocol."""

    def __init__(
        self,
        database_path: Path | None = None,
        *,
        cwd: Path | None = None,
        backends: Mapping[str, ExecutionBackend] | None = None,
    ):
        self.database_path = (
            default_database_path() if database_path is None else database_path
        )
        self.cwd = (Path.cwd() if cwd is None else cwd).resolve(strict=False)
        self.backends = {} if backends is None else dict(backends)
        self.experiments = ExperimentStore(self.database_path)
        self.inventories = InventoryStore(self.database_path)
        self.executions = ExecutionStore(self.database_path)
        self.execution_service = ExecutionService(self.executions, self.inventories)

    def request_schema(self) -> dict[str, Any]:
        return {
            "kind": REQUEST_KIND,
            "version": REQUEST_SCHEMA_VERSION,
            "schema": execution_request_schema(),
        }

    def validate_request(self, request: object) -> dict[str, Any]:
        parsed = self._parse_request(request)
        return {
            "valid": True,
            "kind": parsed.kind,
            "version": parsed.request_schema_version,
            "request": parsed.to_dict(),
            "document": parsed.to_document(),
            "persisted": False,
        }

    def discover(self) -> dict[str, Any]:
        try:
            snapshot = discover_site(self.inventories, cwd=self.cwd)
        except (OSError, ValueError) as exc:
            self._raise("discovery_failed", "Compute-site discovery failed.", exc)
        return {
            "snapshot_id": snapshot.id,
            "captured_at": snapshot.captured_at,
            "inventory": snapshot.to_dict(),
            "summary": _inventory_summary(snapshot),
        }

    def inventory(self, reference: str = "latest") -> dict[str, Any]:
        snapshot = self._inventory(reference)
        return {
            "reference": reference,
            "inventory": snapshot.to_dict(),
            "summary": _inventory_summary(snapshot),
        }

    def plan(
        self,
        request: object,
        *,
        inventory_reference: str = "latest",
    ) -> dict[str, Any]:
        snapshot = self._inventory(inventory_reference)
        parsed = self._parse_request(request)
        try:
            resolution = self.execution_service.plan_request(parsed, snapshot)
            persisted = self.executions.get_request(parsed.id)
        except (ExecutionRequestError, PlanningError, ValueError) as exc:
            self._raise("planning_failed", "Execution planning failed.", exc)
        data = {
            "inventory_snapshot_id": snapshot.id,
            "request": persisted.to_dict(),
            "workload": (
                None
                if (workload := self.executions.workload_for_request(persisted.id))
                is None
                else workload.to_dict()
            ),
            "resolution": resolution.to_dict(),
        }
        if resolution.selected is None:
            error_code = (
                "incompatible_request"
                if resolution.candidates
                and all(
                    item.compatibility_state == "incompatible"
                    for item in resolution.candidates
                )
                else "unresolved_plan"
            )
            raise AgentInterfaceError(
                error_code,
                "No unambiguous compatible execution plan was selected.",
                details=data,
            )
        return data

    def execute_plan(self, plan_id: str) -> dict[str, Any]:
        if not isinstance(plan_id, str) or not plan_id:
            raise AgentInterfaceError("invalid_plan", "A canonical plan ID is required.")
        try:
            plan = self.executions.get_plan(plan_id)
        except PlanNotFound:
            raise AgentInterfaceError(
                "unknown_plan", f"No persisted plan has ID '{plan_id}'."
            ) from None
        try:
            snapshot = self.inventories.get(plan.inventory_snapshot_id)
            backend = self.backends.get(plan.backend)
            if backend is None and plan.backend == "direct":
                backend = DirectBackend(
                    self.executions,
                    stdout_stream=sys.stderr,
                    stderr_stream=sys.stderr,
                )
            result = self.execution_service.execute_plan(
                plan.id, snapshot, backend=backend
            )
        except (BackendError, PlanningError, OSError, ValueError) as exc:
            self._raise("execution_failed", "The persisted plan could not be executed.", exc)
        return self._execution_result(result)

    def execution_get(self, reference: str) -> dict[str, Any]:
        execution_id = self._execution_id(reference)
        try:
            view = self.execution_service.get_execution(execution_id)
        except ExecutionNotFound:
            raise AgentInterfaceError(
                "unknown_execution", f"No execution matches reference '{reference}'."
            ) from None
        return _execution_view(self.executions, view.to_dict(), execution_id)

    def execution_wait(
        self,
        reference: str,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        if timeout_seconds is not None and (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or timeout_seconds > MAX_WAIT_SECONDS
        ):
            raise AgentInterfaceError(
                "invalid_timeout",
                f"timeout_seconds must be greater than zero and at most {MAX_WAIT_SECONDS}.",
            )
        execution_id = self._execution_id(reference)
        execution = self.executions.get_execution(execution_id)
        if execution.backend == "direct":
            raise AgentInterfaceError(
                "execution_not_allowed",
                "Direct execution is synchronous and cannot be waited separately.",
                details={"execution_id": execution.id, "state": execution.state},
            )
        try:
            backend = self.backends.get(execution.backend)
            result = self.execution_service.wait_execution(
                execution_id,
                timeout_seconds=timeout_seconds,
                backend=backend if isinstance(backend, SchedulerBackend) else None,
            )
        except TimeoutError as exc:
            self._raise("wait_timeout", "Execution wait timed out.", exc)
        except (BackendError, OSError, ValueError) as exc:
            self._raise("scheduler_error", "The scheduler wait failed.", exc)
        return self._execution_result(result)

    def execution_cancel(self, reference: str) -> dict[str, Any]:
        execution_id = self._execution_id(reference)
        execution = self.executions.get_execution(execution_id)
        if execution.backend == "direct":
            raise AgentInterfaceError(
                "execution_not_allowed",
                "Direct execution is synchronous and cannot be cancelled separately.",
                details={"execution_id": execution.id, "state": execution.state},
            )
        try:
            backend = self.backends.get(execution.backend)
            self.execution_service.cancel_execution(
                execution_id,
                backend=backend if isinstance(backend, SchedulerBackend) else None,
            )
        except (BackendError, OSError, ValueError) as exc:
            self._raise("scheduler_error", "The scheduler cancellation failed.", exc)
        updated = self.executions.get_execution(execution_id)
        return {"execution_id": execution_id, "state": updated.state}

    def trace_artifact(self, path: str) -> dict[str, Any]:
        if not isinstance(path, str) or not path:
            raise AgentInterfaceError("invalid_artifact", "A non-empty artifact path is required.")
        try:
            traced = trace_artifact(self.experiments, path, cwd=self.cwd)
        except MissingArtifactReference as exc:
            self._raise("unknown_artifact", str(exc), exc, include_cause=False)
        except AmbiguousArtifactReference as exc:
            raise AgentInterfaceError(
                "ambiguous_artifact",
                str(exc),
                details={
                    "matches": [
                        {
                            "artifact_id": item.id,
                            "experiment_id": item.experiment_id,
                            "sha256": item.sha256,
                        }
                        for item in exc.matches
                    ]
                },
            ) from None
        return _artifact_trace(traced)

    def _parse_request(self, request: object) -> ExecutionRequest:
        try:
            return parse_execution_request(
                request,
                base_directory=self.cwd,
                source=RequestSource(
                    "sdk", (("interface", "mcp"), ("interface_version", "1"))
                ),
            )
        except ExecutionRequestError as exc:
            self._raise("invalid_request", "ExecutionRequest validation failed.", exc)

    def _inventory(self, reference: str):
        if not isinstance(reference, str) or not reference:
            raise AgentInterfaceError(
                "invalid_inventory_reference",
                "An inventory reference is required.",
            )
        if self.inventories.count() == 0:
            raise AgentInterfaceError(
                "no_inventory",
                "No inventory snapshots are recorded. Run discovery first.",
            )
        try:
            return resolve_inventory(self.inventories, reference)
        except InventoryReferenceError as exc:
            code = (
                "ambiguous_inventory"
                if str(exc).startswith("Ambiguous inventory reference")
                else "unknown_inventory"
            )
            self._raise(code, str(exc), exc, include_cause=False)

    def _execution_id(self, reference: str) -> str:
        if not isinstance(reference, str) or not reference:
            raise AgentInterfaceError(
                "invalid_execution_reference", "An execution reference is required."
            )
        try:
            return resolve_execution_attempt(self.executions, reference).id
        except WorkloadReferenceError as exc:
            code = (
                "ambiguous_execution"
                if str(exc).startswith("Ambiguous execution reference")
                else "unknown_execution"
            )
            self._raise(code, str(exc), exc, include_cause=False)

    def _execution_result(self, result: Submission | WorkerResult) -> dict[str, Any]:
        if isinstance(result, Submission):
            return {
                "submission": {
                    "execution_id": result.execution_id,
                    "scheduler_family": result.scheduler_family,
                    "job_id": result.job_id,
                    "state": "submitted",
                },
                "execution": self.execution_get(result.execution_id),
            }
        return {
            "result": result.to_dict(),
            "execution": self.execution_get(result.execution_id),
        }

    @staticmethod
    def _raise(
        code: str,
        message: str,
        cause: BaseException,
        *,
        include_cause: bool = True,
    ) -> NoReturn:
        details = (
            {"reason": str(cause)[:4096], "error_type": type(cause).__name__}
            if include_cause
            else {}
        )
        raise AgentInterfaceError(code, message, details=details) from None


def _inventory_summary(snapshot: Any) -> dict[str, Any]:
    provider_states: dict[str, int] = {}
    for provider in snapshot.providers:
        provider_states[provider.status] = provider_states.get(provider.status, 0) + 1
    return {
        "snapshot_id": snapshot.id,
        "identity_available": snapshot.identity is not None,
        "target_count": len(snapshot.targets),
        "storage_count": len(snapshot.storage),
        "scheduler_count": len(snapshot.schedulers),
        "execution_context_count": len(snapshot.execution_contexts),
        "capability_count": len(snapshot.capabilities),
        "provider_states": provider_states,
    }


def _execution_view(
    store: ExecutionStore, view: dict[str, Any], execution_id: str
) -> dict[str, Any]:
    request = store.request_for_execution(execution_id)
    return {
        **view,
        "request": None if request is None else request.to_dict(),
    }


def _artifact_trace(traced: ArtifactTrace) -> dict[str, Any]:
    return {
        "artifact": asdict(traced.artifact),
        "producer": traced.producer.to_dict(),
        "inputs": [asdict(item) for item in traced.inputs],
        "ancestry": [item.to_dict() for item in traced.ancestry],
    }
