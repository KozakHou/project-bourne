"""Schema-5 persistence for requests, plans, execution, and outcomes."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

from .execution_outcomes import ExecutionTelemetrySummary, VerificationRun
from .execution_request import ExecutionRequest
from .ids import new_ulid
from .models import Artifact, Experiment, ExperimentLineage
from .storage import ExperimentStore, _insert_experiment_record
from .workload_models import (
    AllocationObservation,
    ExecutionAttempt,
    ExecutionEvent,
    ExecutionPlan,
    ExecutionView,
    SchedulerJob,
    WorkloadSpec,
)


class WorkloadNotFound(LookupError):
    pass


class PlanNotFound(LookupError):
    pass


class ExecutionNotFound(LookupError):
    pass


class RequestNotFound(LookupError):
    pass


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class ExecutionStore:
    """A transactional repository for immutable plans and append-only events."""

    def __init__(self, path: Path):
        self.path = path

    def initialize(self) -> None:
        ExperimentStore(self.path).initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self.initialize()
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def save_request_with_workload(
        self, request: ExecutionRequest, workload: WorkloadSpec
    ) -> None:
        """Persist immutable user intent, its compiled workload, and link atomically."""

        if (
            request.requested_parent_experiment is not None
            and request.resolved_parent_experiment_id is None
        ):
            raise ValueError(
                "linked request parent reference must be resolved"
            )
        if (
            workload.argv != request.argv
            or workload.working_directory != request.resolved_working_directory
            or workload.inputs != list(request.artifacts.inputs)
            or workload.outputs != list(request.artifacts.outputs)
            or workload.resources != request.resources
            or workload.constraints != request.execution
            or (
                workload.parent_experiment_id
                != request.resolved_parent_experiment_id
            )
        ):
            raise ValueError("workload does not match its execution request")
        with self._connection() as connection:
            self._insert_request(connection, request)
            self._insert_workload(connection, workload)
            connection.execute(
                """
                INSERT INTO execution_request_workload_links
                    (request_id, workload_id, relationship)
                VALUES (?, ?, 'compiled_to')
                """,
                (request.id, workload.id),
            )

    def save_request(self, request: ExecutionRequest) -> None:
        with self._connection() as connection:
            self._insert_request(connection, request)

    def get_request(self, request_id: str) -> ExecutionRequest:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT request_json FROM execution_requests WHERE id = ?",
                (request_id,),
            ).fetchone()
        if row is None:
            raise RequestNotFound(request_id)
        return ExecutionRequest.from_dict(json.loads(row["request_json"]))

    def count_requests(self) -> int:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT count(*) AS count FROM execution_requests"
            ).fetchone()
        return int(row["count"])

    def request_for_workload(self, workload_id: str) -> ExecutionRequest | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT request_json FROM execution_requests AS request
                JOIN execution_request_workload_links AS link
                    ON link.request_id = request.id
                WHERE link.workload_id = ?
                """,
                (workload_id,),
            ).fetchone()
        return (
            None
            if row is None
            else ExecutionRequest.from_dict(json.loads(row["request_json"]))
        )

    def request_for_execution(self, execution_id: str) -> ExecutionRequest | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT request.request_json
                FROM execution_requests AS request
                JOIN execution_request_workload_links AS link
                    ON link.request_id = request.id
                JOIN execution_plans AS plan ON plan.workload_id = link.workload_id
                JOIN execution_attempts AS execution ON execution.plan_id = plan.id
                WHERE execution.id = ?
                """,
                (execution_id,),
            ).fetchone()
        return (
            None
            if row is None
            else ExecutionRequest.from_dict(json.loads(row["request_json"]))
        )

    def save_workload(self, workload: WorkloadSpec) -> None:
        with self._connection() as connection:
            self._insert_workload(connection, workload)

    def get_workload(self, workload_id: str) -> WorkloadSpec:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT spec_json FROM workload_specs WHERE id = ?", (workload_id,)
            ).fetchone()
        if row is None:
            raise WorkloadNotFound(workload_id)
        return WorkloadSpec.from_dict(json.loads(row["spec_json"]))

    def save_plan(self, plan: ExecutionPlan) -> None:
        with self._connection() as connection:
            workload = connection.execute(
                "SELECT id FROM workload_specs WHERE id = ?", (plan.workload_id,)
            ).fetchone()
            if workload is None:
                raise WorkloadNotFound(plan.workload_id)
            snapshot = connection.execute(
                "SELECT id FROM inventory_snapshots WHERE id = ?",
                (plan.inventory_snapshot_id,),
            ).fetchone()
            if snapshot is None:
                raise ValueError("execution plan inventory snapshot does not exist")
            target_ids = [plan.access_target_id]
            if plan.execution_target_id is not None:
                target_ids.append(plan.execution_target_id)
            for target_id in target_ids:
                target = connection.execute(
                    """
                    SELECT id FROM discovered_targets
                    WHERE id = ? AND snapshot_id = ?
                    """,
                    (target_id, plan.inventory_snapshot_id),
                ).fetchone()
                if target is None:
                    raise ValueError("execution plan targets must belong to its snapshot")
            if plan.execution_context_id is not None:
                context = connection.execute(
                    """
                    SELECT id FROM discovered_execution_contexts
                    WHERE id = ? AND snapshot_id = ?
                    """,
                    (plan.execution_context_id, plan.inventory_snapshot_id),
                ).fetchone()
                if context is None:
                    raise ValueError("execution plan context must belong to its snapshot")
            if plan.scheduler_id is not None:
                scheduler = connection.execute(
                    """
                    SELECT family FROM scheduler_resources
                    WHERE id = ? AND snapshot_id = ?
                    """,
                    (plan.scheduler_id, plan.inventory_snapshot_id),
                ).fetchone()
                if scheduler is None or scheduler["family"] != plan.backend:
                    raise ValueError("execution plan scheduler must match its backend")
            elif plan.backend != "direct":
                raise ValueError("scheduled execution plans require a scheduler")
            connection.execute(
                """
                INSERT INTO execution_plans (
                    id, workload_id, inventory_snapshot_id, backend,
                    access_target_id, execution_target_id, execution_context_id,
                    compatibility_state, created_at, plan_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan.id, plan.workload_id, plan.inventory_snapshot_id,
                    plan.backend, plan.access_target_id, plan.execution_target_id,
                    plan.execution_context_id, plan.compatibility_state,
                    plan.created_at, _json(plan.to_dict()),
                ),
            )

    def get_plan(self, plan_id: str) -> ExecutionPlan:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT plan_json FROM execution_plans WHERE id = ?", (plan_id,)
            ).fetchone()
        if row is None:
            raise PlanNotFound(plan_id)
        return ExecutionPlan.from_dict(json.loads(row["plan_json"]))

    def get_plan_by_recency(self, position: int) -> ExecutionPlan:
        if position < 1:
            raise ValueError("position must be at least 1")
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT plan_json FROM execution_plans
                ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?
                """,
                (position - 1,),
            ).fetchone()
        if row is None:
            raise PlanNotFound(f"@{position}")
        return ExecutionPlan.from_dict(json.loads(row["plan_json"]))

    def find_plan_ids_by_prefix(self, prefix: str, limit: int = 100) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id FROM execution_plans WHERE substr(id, 1, ?) = ?
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (len(prefix), prefix, limit),
            ).fetchall()
        return [row["id"] for row in rows]

    def count_plans(self) -> int:
        with self._connection() as connection:
            row = connection.execute("SELECT count(*) AS count FROM execution_plans").fetchone()
        return int(row["count"])

    def create_execution(self, execution: ExecutionAttempt) -> None:
        plan = self.get_plan(execution.plan_id)
        if plan.backend != execution.backend:
            raise ValueError("execution backend must match its immutable plan")
        event = ExecutionEvent(
            id=new_ulid(), execution_id=execution.id,
            occurred_at=execution.created_at, state=execution.state,
            details={"backend": execution.backend},
        )
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO execution_attempts (
                    id, plan_id, backend, state, created_at, updated_at,
                    submitting_identity, staging_directory, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution.id, execution.plan_id, execution.backend,
                    execution.state, execution.created_at, execution.updated_at,
                    execution.submitting_identity, execution.staging_directory,
                    execution.error,
                ),
            )
            self._insert_event(connection, event)

    def get_execution(self, execution_id: str) -> ExecutionAttempt:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM execution_attempts WHERE id = ?", (execution_id,)
            ).fetchone()
        if row is None:
            raise ExecutionNotFound(execution_id)
        return _execution_from_row(row)

    def get_execution_by_recency(self, position: int) -> ExecutionAttempt:
        if position < 1:
            raise ValueError("position must be at least 1")
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM execution_attempts
                ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?
                """,
                (position - 1,),
            ).fetchone()
        if row is None:
            raise ExecutionNotFound(f"@{position}")
        return _execution_from_row(row)

    def find_execution_ids_by_prefix(self, prefix: str, limit: int = 100) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id FROM execution_attempts WHERE substr(id, 1, ?) = ?
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (len(prefix), prefix, limit),
            ).fetchall()
        return [row["id"] for row in rows]

    def count_executions(self) -> int:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT count(*) AS count FROM execution_attempts"
            ).fetchone()
        return int(row["count"])

    def list_executions(self, limit: int = 20) -> list[ExecutionAttempt]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM execution_attempts
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [_execution_from_row(row) for row in rows]

    def set_staging_directory(self, execution_id: str, path: str, observed_at: str) -> None:
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE execution_attempts
                SET staging_directory = ?, updated_at = ? WHERE id = ?
                """,
                (path, observed_at, execution_id),
            )
            if cursor.rowcount != 1:
                raise ExecutionNotFound(execution_id)

    def update_execution_state(
        self,
        execution_id: str,
        state: str,
        occurred_at: str,
        details: dict[str, object] | None = None,
        *,
        error: str | None = None,
    ) -> None:
        event = ExecutionEvent(
            id=new_ulid(), execution_id=execution_id, occurred_at=occurred_at,
            state=state, details={} if details is None else details,
        )
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE execution_attempts
                SET state = ?, updated_at = ?, error = ? WHERE id = ?
                """,
                (state, occurred_at, error, execution_id),
            )
            if cursor.rowcount != 1:
                raise ExecutionNotFound(execution_id)
            self._insert_event(connection, event)

    def record_execution_event(
        self,
        execution_id: str,
        state: str,
        occurred_at: str,
        details: dict[str, object] | None = None,
    ) -> None:
        """Append lifecycle evidence without changing the current-state projection."""

        event = ExecutionEvent(
            id=new_ulid(), execution_id=execution_id, occurred_at=occurred_at,
            state=state, details={} if details is None else details,
        )
        with self._connection() as connection:
            exists = connection.execute(
                "SELECT id FROM execution_attempts WHERE id = ?", (execution_id,)
            ).fetchone()
            if exists is None:
                raise ExecutionNotFound(execution_id)
            self._insert_event(connection, event)

    def save_scheduler_job(self, job: SchedulerJob) -> None:
        execution = self.get_execution(job.execution_id)
        if execution.backend != job.family:
            raise ValueError("scheduler job family must match the execution backend")
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO scheduler_jobs (
                    execution_id, family, job_id, submitting_identity,
                    submitted_at, state, last_observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.execution_id, job.family, job.job_id,
                    job.submitting_identity, job.submitted_at, job.state,
                    job.last_observed_at,
                ),
            )

    def get_scheduler_job(self, execution_id: str) -> SchedulerJob | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM scheduler_jobs WHERE execution_id = ?", (execution_id,)
            ).fetchone()
        return None if row is None else _scheduler_job_from_row(row)

    def update_scheduler_job(self, execution_id: str, state: str, observed_at: str) -> None:
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE scheduler_jobs SET state = ?, last_observed_at = ?
                WHERE execution_id = ?
                """,
                (state, observed_at, execution_id),
            )
            if cursor.rowcount != 1:
                raise ExecutionNotFound(execution_id)

    def save_allocation(self, allocation: AllocationObservation) -> None:
        with self._connection() as connection:
            self._insert_allocation(connection, allocation)

    def events(self, execution_id: str) -> list[ExecutionEvent]:
        self.get_execution(execution_id)
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM execution_events WHERE execution_id = ?
                ORDER BY occurred_at, id
                """,
                (execution_id,),
            ).fetchall()
        return [_event_from_row(row) for row in rows]

    def allocations(self, execution_id: str) -> list[AllocationObservation]:
        self.get_execution(execution_id)
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM allocations WHERE execution_id = ?
                ORDER BY observed_at, id
                """,
                (execution_id,),
            ).fetchall()
        return [_allocation_from_row(row) for row in rows]

    def experiment_id(self, execution_id: str) -> str | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT experiment_id FROM execution_experiment_links
                WHERE execution_id = ?
                """,
                (execution_id,),
            ).fetchone()
        return None if row is None else str(row["experiment_id"])

    def telemetry(self, execution_id: str) -> ExecutionTelemetrySummary | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT summary_json FROM telemetry_summaries WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
        return (
            None
            if row is None
            else ExecutionTelemetrySummary.from_dict(json.loads(row["summary_json"]))
        )

    def verification(self, execution_id: str) -> VerificationRun | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM verification_runs WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if row is None:
                return None
            checks = connection.execute(
                """
                SELECT * FROM verification_checks
                WHERE verification_run_id = ? ORDER BY ordinal
                """,
                (row["id"],),
            ).fetchall()
        return VerificationRun.from_dict(
            {
                "id": row["id"],
                "request_id": row["request_id"],
                "execution_id": row["execution_id"],
                "experiment_id": row["experiment_id"],
                "aggregate_state": row["aggregate_state"],
                "evaluated_at": row["evaluated_at"],
                "source": row["source"],
                "checks": [
                    {
                        "id": item["id"],
                        "verification_run_id": item["verification_run_id"],
                        "ordinal": item["ordinal"],
                        "check_type": item["check_type"],
                        "output_path": item["output_path"],
                        "state": item["state"],
                        "evidence": json.loads(item["evidence_json"]),
                    }
                    for item in checks
                ],
            }
        )

    def view(self, execution_id: str) -> ExecutionView:
        execution = self.get_execution(execution_id)
        plan = self.get_plan(execution.plan_id)
        return ExecutionView(
            execution=execution, plan=plan,
            workload=self.get_workload(plan.workload_id),
            events=self.events(execution_id),
            scheduler_job=self.get_scheduler_job(execution_id),
            allocations=self.allocations(execution_id),
            experiment_id=self.experiment_id(execution_id),
            request_id=(
                None
                if (request := self.request_for_workload(plan.workload_id)) is None
                else request.id
            ),
            telemetry=(
                None
                if (summary := self.telemetry(execution_id)) is None
                else summary.to_dict()
            ),
            verification=(
                None
                if (verification := self.verification(execution_id)) is None
                else verification.to_dict()
            ),
        )

    def import_experiment_result(
        self,
        execution_id: str,
        experiment: Experiment,
        artifacts: Sequence[Artifact],
        lineage: Sequence[ExperimentLineage],
        allocation: AllocationObservation | None,
        *,
        state: str,
        occurred_at: str,
        details: dict[str, object] | None = None,
        telemetry: ExecutionTelemetrySummary | None = None,
        verification: VerificationRun | None = None,
    ) -> None:
        """Import execution-plane provenance and link it atomically."""

        event = ExecutionEvent(
            id=new_ulid(), execution_id=execution_id, occurred_at=occurred_at,
            state=state, details={} if details is None else details,
        )
        with self._connection() as connection:
            execution = connection.execute(
                "SELECT id FROM execution_attempts WHERE id = ?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise ExecutionNotFound(execution_id)
            _insert_experiment_record(connection, experiment, artifacts, lineage)
            connection.execute(
                """
                INSERT INTO execution_experiment_links
                    (execution_id, experiment_id, relationship)
                VALUES (?, ?, 'actual_experiment')
                """,
                (execution_id, experiment.id),
            )
            if allocation is not None:
                if allocation.execution_id != execution_id:
                    raise ValueError("allocation execution ID must match the import")
                self._insert_allocation(connection, allocation)
            self._insert_outcomes(
                connection,
                execution_id,
                experiment.id,
                telemetry,
                verification,
            )
            connection.execute(
                """
                UPDATE execution_attempts
                SET state = ?, updated_at = ?, error = NULL WHERE id = ?
                """,
                (state, occurred_at, execution_id),
            )
            self._insert_event(connection, event)

    def import_worker_failure(
        self,
        execution_id: str,
        allocation: AllocationObservation | None,
        *,
        state: str,
        occurred_at: str,
        details: dict[str, object],
        error: str | None,
    ) -> None:
        """Atomically retain a worker-observed failure with no experiment."""

        event = ExecutionEvent(
            id=new_ulid(), execution_id=execution_id, occurred_at=occurred_at,
            state=state, details=details,
        )
        with self._connection() as connection:
            execution = connection.execute(
                "SELECT id FROM execution_attempts WHERE id = ?", (execution_id,)
            ).fetchone()
            if execution is None:
                raise ExecutionNotFound(execution_id)
            if allocation is not None:
                if allocation.execution_id != execution_id:
                    raise ValueError("allocation execution ID must match the import")
                self._insert_allocation(connection, allocation)
            connection.execute(
                """
                UPDATE execution_attempts
                SET state = ?, updated_at = ?, error = ? WHERE id = ?
                """,
                (state, occurred_at, error, execution_id),
            )
            self._insert_event(connection, event)

    @staticmethod
    def _insert_request(
        connection: sqlite3.Connection, request: ExecutionRequest
    ) -> None:
        connection.execute(
            """
            INSERT INTO execution_requests (
                id, request_schema_version, created_at, base_directory,
                working_directory, resolved_working_directory,
                source_kind, request_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.id,
                request.request_schema_version,
                request.created_at,
                request.base_directory,
                request.working_directory,
                request.resolved_working_directory,
                request.source.kind,
                _json(request.to_dict()),
            ),
        )

    @staticmethod
    def _insert_workload(
        connection: sqlite3.Connection, workload: WorkloadSpec
    ) -> None:
        connection.execute(
            """
            INSERT INTO workload_specs
                (id, created_at, working_directory, executable, spec_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                workload.id,
                workload.created_at,
                workload.working_directory,
                workload.executable,
                _json(workload.to_dict()),
            ),
        )

    @staticmethod
    def _insert_outcomes(
        connection: sqlite3.Connection,
        execution_id: str,
        experiment_id: str,
        telemetry: ExecutionTelemetrySummary | None,
        verification: VerificationRun | None,
    ) -> None:
        if telemetry is None and verification is None:
            return
        row = connection.execute(
            """
            SELECT link.request_id
            FROM execution_attempts AS execution
            JOIN execution_plans AS plan ON plan.id = execution.plan_id
            JOIN execution_request_workload_links AS link
                ON link.workload_id = plan.workload_id
            WHERE execution.id = ?
            """,
            (execution_id,),
        ).fetchone()
        if row is None:
            raise ValueError("execution outcomes require a linked execution request")
        request_id = str(row["request_id"])
        if telemetry is not None:
            if (
                telemetry.request_id != request_id
                or telemetry.execution_id != execution_id
                or telemetry.experiment_id != experiment_id
            ):
                raise ValueError("telemetry relationships do not match the imported result")
            connection.execute(
                """
                INSERT INTO telemetry_summaries (
                    id, request_id, execution_id, experiment_id, created_at,
                    state, sources_json, coverage_json, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telemetry.id,
                    telemetry.request_id,
                    telemetry.execution_id,
                    telemetry.experiment_id,
                    telemetry.created_at,
                    telemetry.state,
                    _json(telemetry.sources),
                    _json(telemetry.coverage),
                    _json(telemetry.to_dict()),
                ),
            )
        if verification is not None:
            if (
                verification.request_id != request_id
                or verification.execution_id != execution_id
                or verification.experiment_id != experiment_id
            ):
                raise ValueError(
                    "verification relationships do not match the imported result"
                )
            connection.execute(
                """
                INSERT INTO verification_runs (
                    id, request_id, execution_id, experiment_id,
                    aggregate_state, evaluated_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    verification.id,
                    verification.request_id,
                    verification.execution_id,
                    verification.experiment_id,
                    verification.aggregate_state,
                    verification.evaluated_at,
                    verification.source,
                ),
            )
            for check in verification.checks:
                connection.execute(
                    """
                    INSERT INTO verification_checks (
                        id, verification_run_id, ordinal, check_type,
                        output_path, state, evidence_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        check.id,
                        check.verification_run_id,
                        check.ordinal,
                        check.check_type,
                        check.output_path,
                        check.state,
                        _json(check.evidence),
                    ),
                )

    @staticmethod
    def _insert_event(connection: sqlite3.Connection, event: ExecutionEvent) -> None:
        connection.execute(
            """
            INSERT INTO execution_events
                (id, execution_id, occurred_at, state, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                event.id, event.execution_id, event.occurred_at,
                event.state, _json(event.details),
            ),
        )

    @staticmethod
    def _insert_allocation(
        connection: sqlite3.Connection, allocation: AllocationObservation
    ) -> None:
        connection.execute(
            """
            INSERT INTO allocations (
                id, execution_id, observed_at, resources_json,
                hosts_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                allocation.id, allocation.execution_id, allocation.observed_at,
                _json(allocation.resources), _json(allocation.hosts),
                _json(allocation.evidence),
            ),
        )


def _execution_from_row(row: sqlite3.Row) -> ExecutionAttempt:
    return ExecutionAttempt(
        id=row["id"], plan_id=row["plan_id"], backend=row["backend"],
        state=row["state"], created_at=row["created_at"],
        updated_at=row["updated_at"],
        submitting_identity=row["submitting_identity"],
        staging_directory=row["staging_directory"], error=row["error"],
    )


def _scheduler_job_from_row(row: sqlite3.Row) -> SchedulerJob:
    return SchedulerJob(
        execution_id=row["execution_id"], family=row["family"],
        job_id=row["job_id"], submitting_identity=row["submitting_identity"],
        submitted_at=row["submitted_at"], state=row["state"],
        last_observed_at=row["last_observed_at"],
    )


def _event_from_row(row: sqlite3.Row) -> ExecutionEvent:
    return ExecutionEvent(
        id=row["id"], execution_id=row["execution_id"],
        occurred_at=row["occurred_at"], state=row["state"],
        details=json.loads(row["details_json"]),
    )


def _allocation_from_row(row: sqlite3.Row) -> AllocationObservation:
    return AllocationObservation(
        id=row["id"], execution_id=row["execution_id"],
        observed_at=row["observed_at"],
        resources=json.loads(row["resources_json"]),
        hosts=json.loads(row["hosts_json"]),
        evidence=json.loads(row["evidence_json"]),
    )
