"""Remote SSH scheduler backend reusing Bourne's existing compute worker."""

from __future__ import annotations

import hashlib
import time
from pathlib import Path, PurePosixPath

from .backends import (
    BackendError,
    Submission,
    _import_result,
    _target_name,
    _validate_result_against_plan,
    render_batch_script,
)
from .inventory_models import InventorySnapshot
from .planning_models import ResourceShape
from .remote_transport import RemoteTransportError, RemoteWorkerClient
from .site_models import Site
from .site_storage import SiteStore
from .worker_bundle import build_worker_zipapp, write_staged_plan
from .worker_result import WorkerResult, WorkerResultError, parse_worker_result
from .workload import utc_now
from .workload_models import ExecutionAttempt, ExecutionPlan, SchedulerJob, WorkloadSpec
from .workload_storage import ExecutionStore


class AmbiguousSubmissionError(BackendError):
    """The scheduler may own a job, so automatic resubmission is forbidden."""


class RemoteSchedulerBackend:
    """Submit/reconcile one immutable plan through typed remote operations."""

    def __init__(
        self,
        store: ExecutionStore,
        site_store: SiteStore,
        site: Site,
        client: RemoteWorkerClient,
        staging_root: Path,
    ):
        if site.kind != "remote_ssh":
            raise ValueError("remote scheduler backend requires a remote SSH site")
        self.store = store
        self.site_store = site_store
        self.site = site
        self.client = client
        self.staging_root = staging_root.resolve(strict=False)

    def execute(
        self,
        execution: ExecutionAttempt,
        plan: ExecutionPlan,
        workload: WorkloadSpec,
        inventory: InventorySnapshot,
    ) -> Submission:
        if plan.site_id != self.site.id:
            raise BackendError("remote plan site does not match the selected transport")
        if plan.backend not in {"slurm", "pbs", "lsf"}:
            raise BackendError("remote execution requires Slurm, PBS, or LSF")
        if self.site.remote_project_root is None:
            raise BackendError("remote site has no configured project/staging root")
        request = self.store.request_for_workload(workload.id)
        validation = self.client.call(
            "validate_plan",
            {
                "plan": plan.to_dict(), "workload": workload.to_dict(),
                "request": None if request is None else request.to_dict(),
            },
        )
        if validation.status != "ok" or not validation.data.get("valid"):
            problems = validation.data.get("problems", [])
            raise BackendError("remote plan validation failed: " + "; ".join(problems))

        local_staging = self.staging_root / execution.id
        local_staging.mkdir(parents=True, exist_ok=False)
        worker = build_worker_zipapp(local_staging / "worker.pyz")
        staged_plan = write_staged_plan(
            local_staging / "plan.json", execution.id, plan, workload, request
        )
        remote_staging = str(
            PurePosixPath(self.site.remote_project_root)
            / ".bourne" / "executions" / execution.id
        )
        script = render_batch_script(
            plan.backend, plan,
            Path(remote_staging) / "worker.pyz",
            Path(remote_staging) / "plan.json",
            Path(remote_staging) / "result.json",
            execution.id,
            target_name=_target_name(plan, inventory),
        )
        job_script = local_staging / "job.sh"
        job_script.write_text(script, encoding="utf-8")
        files = {"worker.pyz": worker, "plan.json": staged_plan, "job.sh": job_script}
        digests = {
            name: hashlib.sha256(path.read_bytes()).hexdigest()
            for name, path in files.items()
        }
        variant_payload = None
        variant = None
        if plan.workload_variant_id is not None:
            variant = self.site_store.get_variant(plan.workload_variant_id)
            remote_variant_path = next(
                (
                    value for value in [plan.executable, *plan.arguments, *plan.inputs]
                    if value.endswith("/" + Path(variant.derived_path).name)
                    and f"/.bourne/variants/{variant.id}/" in value
                ),
                None,
            )
            if remote_variant_path is None:
                raise BackendError("immutable plan does not reference its remote variant")
            variant_payload = {
                "remote_path": remote_variant_path,
                "sha256": variant.derived_sha256.removeprefix("sha256:"),
            }
        prepared = self.client.call(
            "prepare",
            {
                "execution_id": execution.id,
                "staging_root": self.site.remote_project_root,
                "scheduler_family": plan.backend,
                "expected_files": digests,
                "variant": variant_payload,
            },
        )
        if prepared.status != "ok":
            raise BackendError("remote execution staging could not be prepared")
        observed_staging = prepared.data.get("staging_directory")
        if observed_staging != remote_staging:
            raise BackendError("remote worker returned an unexpected staging directory")
        self.store.set_staging_directory(execution.id, str(local_staging), utc_now())
        self.store.update_execution_state(
            execution.id, "prepared", utc_now(),
            {
                "site_id": self.site.id,
                "remote_staging_directory": remote_staging,
                "compute_visibility": "unknown",
            },
        )
        self.site_store.save_remote_state(
            execution.id, self.site.id, "prepared", utc_now(),
            remote_staging_directory=remote_staging,
            scheduler_family=plan.backend,
            evidence={"remote_worker": "typed_one_shot"},
        )
        for name, path in files.items():
            self.client.upload(path, f"{remote_staging}/{name}")
        if variant is not None and variant_payload is not None:
            self.client.upload(
                Path(variant.derived_path), str(variant_payload["remote_path"])
            )

        try:
            response = self.client.call(
                "submit",
                {"execution_id": execution.id, "staging_directory": remote_staging},
            )
        except RemoteTransportError as exc:
            # The scheduler may already own the job. Query the exact execution
            # identity once; never issue another submit from this path.
            try:
                response = self.client.call(
                    "reconcile",
                    {"execution_id": execution.id, "staging_directory": remote_staging},
                )
            except RemoteTransportError:
                self._ambiguous(execution, remote_staging, str(exc))
                raise AmbiguousSubmissionError(
                    "submission truth is ambiguous; Bourne did not resubmit"
                ) from exc
        if response.status in {"ambiguous", "unknown"}:
            self._ambiguous(
                execution, remote_staging,
                str(response.data.get("diagnostic", "remote submission truth is incomplete")),
            )
            raise AmbiguousSubmissionError(
                "submission truth is ambiguous; Bourne did not resubmit"
            )
        if response.status != "ok" or not isinstance(response.data.get("job_id"), str):
            diagnostic = str(response.data.get("diagnostic", "remote scheduler rejected submission"))
            self.store.update_execution_state(
                execution.id, "failed", utc_now(),
                {"phase": "remote_submission", "diagnostic": diagnostic},
                error=diagnostic,
            )
            raise BackendError(diagnostic)
        return self._record_submission(execution, response.data, remote_staging)

    def collect(self, execution: ExecutionAttempt) -> WorkerResult:
        remote = self._remote_record(execution.id)
        response = self.client.call(
            "reconcile",
            {
                "execution_id": execution.id,
                "staging_directory": remote["remote_staging_directory"],
            },
        )
        self._record_reconciliation(execution, response.status, response.data)
        raw_result = response.data.get("result")
        if not isinstance(raw_result, dict):
            raise BackendError("remote worker result is not currently available")
        try:
            result = parse_worker_result(raw_result, execution.id)
        except WorkerResultError as exc:
            raise BackendError(str(exc)) from exc
        plan = self.store.get_plan(execution.plan_id)
        _validate_result_against_plan(result, plan)
        existing = self.store.experiment_id(execution.id)
        if existing is not None:
            if result.experiment is None or result.experiment.id != existing:
                raise BackendError("remote result conflicts with imported experiment")
            return result
        _import_result(self.store, result)
        return result

    def wait(
        self,
        execution: ExecutionAttempt,
        *,
        poll_seconds: float = 15.0,
        timeout_seconds: float | None = None,
    ) -> WorkerResult:
        started = time.monotonic()
        delay = max(0.05, min(60.0, poll_seconds))
        while True:
            current = self.store.get_execution(execution.id)
            try:
                return self.collect(current)
            except BackendError as exc:
                remote = self.site_store.remote_state(execution.id)
                evidence = {} if remote is None else remote.get("evidence", {})
                scheduler = evidence.get("scheduler", {})
                state = scheduler.get("state") if isinstance(scheduler, dict) else None
                if state in {
                    "completed", "failed", "cancelled", "finished", "timeout",
                    "node_fail", "out_of_memory", "preempted", "unknown_terminal",
                }:
                    result_evidence = (
                        "partial"
                        if evidence.get("result_state") == "invalid"
                        else "missing"
                    )
                    outcome = {
                        "cancelled": "scheduler_cancelled",
                        "timeout": "scheduler_timeout",
                        "node_fail": "node_failure",
                        "out_of_memory": "out_of_memory",
                    }.get(
                        state,
                        "result_bundle_partial"
                        if result_evidence == "partial"
                        else "result_bundle_missing",
                    )
                    self.store.update_execution_state(
                        execution.id, "collection_failed", utc_now(),
                        {
                            "scientific_completion_established": False,
                            "scheduler_state": state,
                            "result_bundle": (
                                "invalid" if result_evidence == "partial" else "absent"
                            ),
                            "termination_phase": "scheduler",
                            "termination_outcome": outcome,
                            "result_evidence": result_evidence,
                            "telemetry_evidence": "unavailable",
                        },
                        error=str(exc),
                    )
                    raise BackendError(
                        "scientific completion was not established: " + str(exc)
                    ) from exc
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                raise TimeoutError(f"timed out waiting for execution {execution.id}")
            time.sleep(delay)
            delay = min(60.0, delay * 1.5)

    def cancel(self, execution: ExecutionAttempt) -> None:
        remote = self._remote_record(execution.id)
        response = self.client.call(
            "cancel",
            {
                "execution_id": execution.id,
                "staging_directory": remote["remote_staging_directory"],
            },
        )
        if response.status != "ok":
            raise BackendError("remote scheduler cancellation was not established")
        now = utc_now()
        self.store.update_scheduler_job(execution.id, "cancelled", now)
        self.store.update_execution_state(execution.id, "cancelled", now, response.data)
        self.site_store.save_remote_state(
            execution.id, self.site.id, "cancelled", now,
            remote_staging_directory=remote["remote_staging_directory"],
            scheduler_family=execution.backend,
            scheduler_job_id=response.data.get("job_id"),
            evidence=response.data,
        )

    def _record_submission(
        self, execution: ExecutionAttempt, data: dict[str, object], remote_staging: str
    ) -> Submission:
        job_id = str(data["job_id"])
        identity = str(data.get("submitting_identity") or "unknown")
        now = utc_now()
        self.store.save_scheduler_job(
            SchedulerJob(
                execution_id=execution.id, family=execution.backend,
                job_id=job_id, submitting_identity=identity,
                submitted_at=str(data.get("submitted_at") or now),
                state="submitted", last_observed_at=now,
            )
        )
        self.store.update_execution_state(
            execution.id, "submitted", now,
            {
                "scheduler_family": execution.backend, "job_id": job_id,
                "site_id": self.site.id, "scheduler_owns_lifetime": True,
                "local_keepalive": False,
            },
        )
        self.site_store.save_remote_state(
            execution.id, self.site.id, "submitted", now,
            remote_staging_directory=remote_staging,
            scheduler_family=execution.backend, scheduler_job_id=job_id,
            evidence=data,
        )
        return Submission(execution.backend, job_id, execution.id)

    def _ambiguous(
        self, execution: ExecutionAttempt, remote_staging: str, diagnostic: str
    ) -> None:
        now = utc_now()
        self.store.update_execution_state(
            execution.id, "submission_ambiguous", now,
            {
                "site_id": self.site.id,
                "scientific_submission_established": False,
                "automatic_resubmission": False,
                "diagnostic": diagnostic[:4096],
            },
            error=diagnostic[:4096],
        )
        self.site_store.save_remote_state(
            execution.id, self.site.id, "ambiguous", now,
            remote_staging_directory=remote_staging,
            scheduler_family=execution.backend,
            evidence={"diagnostic": diagnostic[:4096], "blind_retry": False},
        )

    def _remote_record(self, execution_id: str) -> dict[str, object]:
        value = self.site_store.remote_state(execution_id)
        if value is None or not value.get("remote_staging_directory"):
            raise BackendError("execution has no remote reconciliation state")
        return value

    def _record_reconciliation(
        self, execution: ExecutionAttempt, status: str, data: dict[str, object]
    ) -> None:
        now = utc_now()
        scheduler = data.get("scheduler")
        job_id = data.get("job_id")
        if isinstance(scheduler, dict) and isinstance(job_id, str):
            scheduler_state = str(scheduler.get("state", "unknown"))
            known_job = self.store.get_scheduler_job(execution.id)
            if known_job is None:
                self.store.save_scheduler_job(
                    SchedulerJob(
                        execution_id=execution.id, family=execution.backend,
                        job_id=job_id,
                        submitting_identity=str(
                            data.get("submitting_identity") or "unknown"
                        ),
                        submitted_at=str(data.get("submitted_at") or now),
                        state=scheduler_state, last_observed_at=now,
                    )
                )
                self.store.update_execution_state(
                    execution.id,
                    (
                        "scheduler_unobservable"
                        if scheduler_state in {"unobservable", "unknown"}
                        else "queued"
                        if scheduler_state in {"pending", "queued", "configuring"}
                        else "running"
                        if scheduler_state in {"running", "completing"}
                        else "scheduler_terminal"
                    ),
                    now,
                    {
                        "submission_recovered_by_reconciliation": True,
                        "scheduler_family": execution.backend,
                        "job_id": job_id,
                    },
                )
            else:
                self.store.update_scheduler_job(execution.id, scheduler_state, now)
            self.store.record_execution_event(
                execution.id, "remote_reconciled", now,
                {
                    "site_id": self.site.id, "remote_status": status,
                    "scheduler": scheduler,
                    "result_state": data.get("result_state", "unknown"),
                },
            )
        remote = self._remote_record(execution.id)
        self.site_store.save_remote_state(
            execution.id, self.site.id, str(data.get("state", status)), now,
            remote_staging_directory=str(remote["remote_staging_directory"]),
            scheduler_family=execution.backend,
            scheduler_job_id=None if job_id is None else str(job_id),
            evidence=data,
        )
