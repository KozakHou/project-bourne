"""Framework-independent direct, Slurm, and PBS execution backends."""

from __future__ import annotations

import re
import shlex
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from .bounded_subprocess import BoundedCommandResult, run_bounded_command
from .compute_worker import execute_plan
from .execution_outcomes import (
    ExecutionTelemetrySummary,
    VerificationRun,
    add_scheduler_wait,
    build_telemetry_summary,
    evaluate_verification,
)
from .execution_request import ExecutionRequest
from .identity import current_process_identity
from .inventory_models import InventorySnapshot
from .worker_bundle import build_worker_zipapp, write_staged_plan
from .worker_result import WorkerResult, WorkerResultError, load_worker_result
from .workload import utc_now
from .workload_models import ExecutionAttempt, ExecutionPlan, SchedulerJob, WorkloadSpec
from .workload_storage import ExecutionStore

SCHEDULER_TIMEOUT_SECONDS = 10.0
MAX_SCHEDULER_OUTPUT_BYTES = 1024 * 1024
DEFAULT_SCHEDULER_POLL_SECONDS = 15.0
MAX_SCHEDULER_POLL_SECONDS = 60.0
_SAFE_SCHEDULER_NAME = re.compile(r"[A-Za-z0-9_.+-]+\Z")
_SAFE_SLURM_JOB = re.compile(r"[0-9]+(?:_[0-9]+)?\Z")
_SAFE_PBS_JOB = re.compile(r"[A-Za-z0-9_.-]+\Z")

CommandRunner = Callable[..., BoundedCommandResult]


class BackendError(RuntimeError):
    pass


@dataclass(frozen=True)
class Submission:
    scheduler_family: str
    job_id: str
    execution_id: str


@dataclass(frozen=True)
class SchedulerObservation:
    """One exact-job scheduler observation and the provenance of that fact."""

    state: str
    source: str
    observable: bool
    terminal: bool
    diagnostic: str | None = None

    def details(self, family: str, job_id: str) -> dict[str, object]:
        value: dict[str, object] = {
            "scheduler_family": family,
            "scheduler_state": self.state,
            "job_id": job_id,
            "observation_source": self.source,
            "job_observable": self.observable,
            "scheduler_terminal": self.terminal,
        }
        if self.diagnostic is not None:
            value["diagnostic"] = self.diagnostic
        return value


class ExecutionBackend(Protocol):
    name: str

    def execute(
        self,
        execution: ExecutionAttempt,
        plan: ExecutionPlan,
        workload: WorkloadSpec,
        inventory: InventorySnapshot,
    ) -> Submission | WorkerResult:
        ...


class DirectBackend:
    name = "direct"

    def __init__(self, store: ExecutionStore):
        self.store = store

    def execute(
        self,
        execution: ExecutionAttempt,
        plan: ExecutionPlan,
        workload: WorkloadSpec,
        inventory: InventorySnapshot,
    ) -> WorkerResult:
        del inventory
        self.store.update_execution_state(
            execution.id, "preflight", utc_now(), {"backend": self.name}
        )
        request = self.store.request_for_workload(workload.id)
        result = execute_plan(
            plan, workload, execution.id, request=request
        )
        _import_result(self.store, result)
        return result


class SchedulerBackend:
    name = ""
    submit_command = ""
    status_command = ""
    cancel_command = ""

    def __init__(
        self,
        store: ExecutionStore,
        staging_root: Path,
        *,
        runner: CommandRunner = run_bounded_command,
    ):
        self.store = store
        self.staging_root = staging_root.resolve(strict=False)
        self.runner = runner

    def execute(
        self,
        execution: ExecutionAttempt,
        plan: ExecutionPlan,
        workload: WorkloadSpec,
        inventory: InventorySnapshot,
    ) -> Submission:
        staging = self.staging_root / execution.id
        staging.mkdir(parents=True, exist_ok=False)
        worker_path = build_worker_zipapp(staging / "worker.pyz")
        plan_path = write_staged_plan(
            staging / "plan.json",
            execution.id,
            plan,
            workload,
            self.store.request_for_workload(workload.id),
        )
        result_path = staging / "result.json"
        target_name = _target_name(plan, inventory)
        script = render_batch_script(
            self.name, plan, worker_path, plan_path, result_path,
            execution.id, target_name=target_name,
        )
        script_path = staging / ("job.slurm" if self.name == "slurm" else "job.pbs")
        script_path.write_text(script, encoding="utf-8")
        self.store.set_staging_directory(execution.id, str(staging), utc_now())
        self.store.update_execution_state(
            execution.id, "prepared", utc_now(),
            {
                "staging_directory": str(staging),
                "compute_visibility": "unknown",
            },
        )
        executable = shutil.which(self.submit_command)
        if executable is None:
            raise BackendError(f"{self.submit_command} executable is unavailable")
        result = self._run([executable, *self._submit_arguments(script_path)])
        if result.returncode != 0 or result.timed_out:
            detail = _failure_detail(result)
            self.store.update_execution_state(
                execution.id, "failed", utc_now(),
                {"phase": "submission", "diagnostic": detail}, error=detail,
            )
            raise BackendError(f"{self.name} submission failed: {detail}")
        try:
            job_id = self._parse_submission(result.stdout)
        except BackendError as exc:
            self.store.update_execution_state(
                execution.id, "failed", utc_now(),
                {"phase": "submission", "diagnostic": str(exc)},
                error=str(exc),
            )
            raise
        identity = execution.submitting_identity or current_process_identity().username
        now = utc_now()
        self.store.save_scheduler_job(
            SchedulerJob(
                execution_id=execution.id, family=self.name, job_id=job_id,
                submitting_identity=identity, submitted_at=now,
                state="submitted", last_observed_at=now,
            )
        )
        self.store.update_execution_state(
            execution.id, "submitted", now,
            {"scheduler_family": self.name, "job_id": job_id},
        )
        return Submission(self.name, job_id, execution.id)

    def status(self, execution: ExecutionAttempt) -> str:
        job = self._known_job(execution)
        observation = self._observe_and_record(execution, job)
        return observation.state

    def _observe_and_record(
        self, execution: ExecutionAttempt, job: SchedulerJob
    ) -> SchedulerObservation:
        executable = shutil.which(self.status_command)
        if executable is None:
            error = BackendError(f"{self.status_command} executable is unavailable")
            self._record_query_error(execution, job, error)
            raise error
        try:
            observation = self._observe_job(job, executable)
        except BackendError as exc:
            self._record_query_error(execution, job, exc)
            raise
        now = utc_now()
        self.store.update_scheduler_job(execution.id, observation.state, now)
        self.store.update_execution_state(
            execution.id, _execution_state(observation), now,
            observation.details(self.name, job.job_id),
        )
        return observation

    def _record_query_error(
        self, execution: ExecutionAttempt, job: SchedulerJob, error: BackendError
    ) -> None:
        self.store.record_execution_event(
            execution.id, "scheduler_query_error", utc_now(),
            {
                "scheduler_family": self.name,
                "job_id": job.job_id,
                "observation_source": "active",
                "diagnostic": str(error)[:4096],
            },
        )

    def collect(self, execution: ExecutionAttempt) -> WorkerResult:
        staging = execution.staging_directory
        if staging is None:
            raise BackendError("execution has no staging directory")
        path = Path(staging) / "result.json"
        try:
            result = load_worker_result(path, execution.id)
        except WorkerResultError as exc:
            raise BackendError(str(exc)) from exc
        plan = self.store.get_plan(execution.plan_id)
        _validate_result_against_plan(result, plan)
        existing_experiment = self.store.experiment_id(execution.id)
        if existing_experiment is not None:
            if result.experiment is None or result.experiment.id != existing_experiment:
                raise BackendError("worker result conflicts with the imported experiment")
            return result
        if (
            result.experiment is None
            and execution.state == result.state
            and self.store.allocations(execution.id)
        ):
            return result
        _import_result(self.store, result)
        return result

    def wait(
        self,
        execution: ExecutionAttempt,
        *,
        poll_seconds: float = DEFAULT_SCHEDULER_POLL_SECONDS,
        timeout_seconds: float | None = None,
    ) -> WorkerResult:
        started = time.monotonic()
        delay = min(
            MAX_SCHEDULER_POLL_SECONDS, max(0.05, float(poll_seconds))
        )
        while True:
            current = self.store.get_execution(execution.id)
            try:
                return self.collect(current)
            except BackendError:
                pass
            job = self._known_job(current)
            observation = self._observe_and_record(current, job)
            if observation.terminal or not observation.observable:
                try:
                    return self.collect(self.store.get_execution(execution.id))
                except BackendError as exc:
                    now = utc_now()
                    details = observation.details(self.name, job.job_id)
                    details.update(
                        {
                            "result_bundle": "absent_or_invalid",
                            "scientific_completion_established": False,
                            "collection_diagnostic": str(exc)[:4096],
                        }
                    )
                    self.store.update_execution_state(
                        execution.id, "collection_failed", now,
                        details,
                        error=str(exc),
                    )
                    raise BackendError(
                        "scientific completion was not established: " + str(exc)
                    ) from exc
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                raise TimeoutError(f"timed out waiting for execution {execution.id}")
            time.sleep(delay)
            delay = min(MAX_SCHEDULER_POLL_SECONDS, delay * 1.5)

    def cancel(self, execution: ExecutionAttempt) -> None:
        requester = current_process_identity()
        self.store.record_execution_event(
            execution.id, "cancellation_requested", utc_now(),
            {
                "requested_by": requester.username,
                "effective_uid": requester.effective_uid,
                "identity_source": requester.source,
            },
        )
        try:
            job = self._known_job(execution)
        except BackendError as exc:
            self.store.record_execution_event(
                execution.id, "cancellation_rejected", utc_now(),
                {"diagnostic": str(exc)},
            )
            raise
        executable = shutil.which(self.cancel_command)
        if executable is None:
            self.store.record_execution_event(
                execution.id, "cancellation_failed", utc_now(),
                {"diagnostic": f"{self.cancel_command} executable is unavailable"},
            )
            raise BackendError(f"{self.cancel_command} executable is unavailable")
        result = self._run([executable, *self._cancel_arguments(job.job_id, job.submitting_identity)])
        if result.returncode != 0 or result.timed_out:
            detail = _failure_detail(result)
            self.store.record_execution_event(
                execution.id, "cancellation_failed", utc_now(),
                {"diagnostic": detail},
            )
            raise BackendError(f"scheduler cancellation failed: {detail}")
        now = utc_now()
        self.store.update_scheduler_job(execution.id, "cancelled", now)
        self.store.update_execution_state(
            execution.id, "cancelled", now,
            {"scheduler_family": self.name, "job_id": job.job_id},
        )

    @property
    def terminal_states(self) -> set[str]:
        raise NotImplementedError

    def _run(self, argv: list[str]) -> BoundedCommandResult:
        try:
            return self.runner(
                argv, timeout=SCHEDULER_TIMEOUT_SECONDS,
                max_output_bytes=MAX_SCHEDULER_OUTPUT_BYTES,
            )
        except OSError as exc:
            raise BackendError(f"could not run scheduler command: {exc}") from exc

    def _known_job(self, execution: ExecutionAttempt) -> SchedulerJob:
        if execution.backend != self.name:
            raise BackendError("execution backend does not match scheduler")
        job = self.store.get_scheduler_job(execution.id)
        if job is None:
            raise BackendError("execution has no Bourne-managed scheduler job")
        if job.submitting_identity != current_process_identity().username:
            raise BackendError("current identity did not submit this Bourne job")
        return job

    def _submit_arguments(self, script_path: Path) -> list[str]:
        raise NotImplementedError

    def _parse_submission(self, stdout: str) -> str:
        raise NotImplementedError

    def _status_arguments(self, job_id: str, identity: str) -> list[str]:
        raise NotImplementedError

    def _parse_status(self, result: BoundedCommandResult) -> str:
        raise NotImplementedError

    def _observe_job(
        self, job: SchedulerJob, status_executable: str
    ) -> SchedulerObservation:
        raise NotImplementedError

    def _cancel_arguments(self, job_id: str, identity: str) -> list[str]:
        raise NotImplementedError


class SlurmBackend(SchedulerBackend):
    name = "slurm"
    submit_command = "sbatch"
    status_command = "squeue"
    cancel_command = "scancel"

    @property
    def terminal_states(self) -> set[str]:
        return {
            "completed", "failed", "cancelled", "timeout", "node_fail",
            "out_of_memory", "preempted", "boot_fail", "deadline",
        }

    def _submit_arguments(self, script_path: Path) -> list[str]:
        return ["--parsable", str(script_path)]

    def _parse_submission(self, stdout: str) -> str:
        job_id = stdout.strip().split(";", 1)[0]
        if not _SAFE_SLURM_JOB.fullmatch(job_id):
            raise BackendError("Slurm returned an invalid job ID")
        return job_id

    def _status_arguments(self, job_id: str, identity: str) -> list[str]:
        return ["--noheader", "--jobs", job_id, "--user", identity, "--format=%T"]

    def _parse_status(self, result: BoundedCommandResult) -> str:
        if result.returncode != 0 or result.timed_out:
            raise BackendError(f"Slurm status failed: {_failure_detail(result)}")
        value = result.stdout.strip().splitlines()
        return "unobservable" if not value else _normalize_slurm_state(value[0])

    def _observe_job(
        self, job: SchedulerJob, status_executable: str
    ) -> SchedulerObservation:
        active_result = self._run(
            [
                status_executable,
                *self._status_arguments(job.job_id, job.submitting_identity),
            ]
        )
        active_state = self._parse_status(active_result)
        if active_state != "unobservable":
            return SchedulerObservation(
                state=active_state,
                source="active",
                observable=True,
                terminal=active_state in self.terminal_states,
            )

        accounting_executable = shutil.which("sacct")
        if accounting_executable is None:
            return SchedulerObservation(
                state="unobservable",
                source="accounting_unavailable",
                observable=False,
                terminal=False,
                diagnostic=(
                    "known job was absent from squeue and Slurm accounting "
                    "was unavailable"
                ),
            )
        accounting_result = self._run(
            [
                accounting_executable,
                "--noheader",
                "--parsable2",
                "--jobs", job.job_id,
                "--user", job.submitting_identity,
                "--format=JobIDRaw,State",
            ]
        )
        if accounting_result.returncode != 0 or accounting_result.timed_out:
            return SchedulerObservation(
                state="unobservable",
                source="accounting_error",
                observable=False,
                terminal=False,
                diagnostic=(
                    "known job was absent from squeue and Slurm accounting "
                    f"failed: {_failure_detail(accounting_result)}"
                )[:4096],
            )
        accounting_state = _parse_slurm_accounting_state(
            accounting_result.stdout, job.job_id
        )
        if accounting_state is None:
            return SchedulerObservation(
                state="unobservable",
                source="terminal_accounting",
                observable=False,
                terminal=False,
                diagnostic=(
                    "known job was absent from squeue and had no exact sacct record"
                ),
            )
        return SchedulerObservation(
            state=accounting_state,
            source="terminal_accounting",
            observable=True,
            terminal=accounting_state in self.terminal_states,
        )

    def _cancel_arguments(self, job_id: str, identity: str) -> list[str]:
        return ["--user", identity, job_id]


class PBSBackend(SchedulerBackend):
    name = "pbs"
    submit_command = "qsub"
    status_command = "qstat"
    cancel_command = "qdel"

    @property
    def terminal_states(self) -> set[str]:
        return {"completed", "failed", "cancelled", "finished", "expired"}

    def _submit_arguments(self, script_path: Path) -> list[str]:
        return [str(script_path)]

    def _parse_submission(self, stdout: str) -> str:
        job_id = stdout.strip().split()[0] if stdout.strip() else ""
        if not _SAFE_PBS_JOB.fullmatch(job_id):
            raise BackendError("PBS returned an invalid job ID")
        return job_id

    def _status_arguments(self, job_id: str, identity: str) -> list[str]:
        del identity
        return ["-f", job_id]

    def _parse_status(self, result: BoundedCommandResult) -> str:
        if result.returncode != 0 or result.timed_out:
            raise BackendError(f"PBS status failed: {_failure_detail(result)}")
        match = re.search(r"^\s*job_state\s*=\s*([A-Za-z])\s*$", result.stdout, re.MULTILINE)
        if match is None:
            return "unknown"
        return {
            "Q": "queued", "H": "held", "R": "running", "E": "exiting",
            "F": "finished", "C": "completed", "S": "suspended",
        }.get(match.group(1).upper(), "unknown")

    def _observe_job(
        self, job: SchedulerJob, status_executable: str
    ) -> SchedulerObservation:
        result = self._run(
            [
                status_executable,
                *self._status_arguments(job.job_id, job.submitting_identity),
            ]
        )
        if result.timed_out:
            raise BackendError(f"PBS status failed: {_failure_detail(result)}")
        if result.returncode != 0:
            if _pbs_job_is_unobservable(result):
                return SchedulerObservation(
                    state="unobservable",
                    source="active",
                    observable=False,
                    terminal=False,
                    diagnostic=(
                        "known job was no longer observable through exact-job qstat"
                    ),
                )
            raise BackendError(f"PBS status failed: {_failure_detail(result)}")
        if not result.stdout.strip():
            return SchedulerObservation(
                state="unobservable",
                source="active",
                observable=False,
                terminal=False,
                diagnostic="exact-job qstat returned no observable job",
            )
        state = self._parse_status(result)
        if state == "unknown":
            raise BackendError("PBS status did not contain a recognized job_state")
        return SchedulerObservation(
            state=state,
            source="active",
            observable=True,
            terminal=state in self.terminal_states,
        )

    def _cancel_arguments(self, job_id: str, identity: str) -> list[str]:
        del identity
        return [job_id]


def render_batch_script(
    family: str,
    plan: ExecutionPlan,
    worker_path: Path,
    plan_path: Path,
    result_path: Path,
    execution_id: str,
    *,
    target_name: str | None,
) -> str:
    """Render fixed scheduler syntax; scientific argv is never shell text."""

    if family not in {"slurm", "pbs"}:
        raise ValueError("unsupported scheduler family")
    if target_name is not None and not _SAFE_SCHEDULER_NAME.fullmatch(target_name):
        raise ValueError("scheduler target name contains unsafe characters")
    directives = _slurm_directives(plan, target_name) if family == "slurm" else _pbs_directives(plan, target_name)
    command = " ".join(
        shlex.quote(str(item))
        for item in (worker_path, plan_path, result_path, execution_id)
    )
    return "\n".join(
        [
            "#!/bin/sh", *directives, "set -eu",
            "if command -v python3 >/dev/null 2>&1; then",
            f"  exec python3 {command}",
            "elif command -v python >/dev/null 2>&1; then",
            f"  exec python {command}",
            "else",
            "  echo 'bourne worker requires a Python 3 runtime' >&2",
            "  exit 70",
            "fi", "",
        ]
    )


def _slurm_directives(plan: ExecutionPlan, target_name: str | None) -> list[str]:
    resources = plan.requested_resources
    values = ["#SBATCH --job-name=bourne"]
    if target_name:
        values.append(f"#SBATCH --partition={target_name}")
    for name, option in (("nodes", "nodes"), ("cpus", "cpus-per-task"), ("gpus", "gpus"), ("mpi_ranks", "ntasks")):
        value = getattr(resources, name)
        if value is not None:
            values.append(f"#SBATCH --{option}={value}")
    if resources.memory_bytes is not None:
        values.append(f"#SBATCH --mem={_mebibytes(resources.memory_bytes)}M")
    if resources.walltime_seconds is not None:
        values.append(f"#SBATCH --time={_walltime(resources.walltime_seconds)}")
    return values


def _pbs_directives(plan: ExecutionPlan, target_name: str | None) -> list[str]:
    resources = plan.requested_resources
    values = ["#PBS -N bourne"]
    if target_name:
        values.append(f"#PBS -q {target_name}")
    chunks = [f"select={resources.nodes or 1}"]
    if resources.cpus is not None:
        chunks.append(f"ncpus={resources.cpus}")
    if resources.gpus is not None:
        chunks.append(f"ngpus={resources.gpus}")
    if resources.memory_bytes is not None:
        chunks.append(f"mem={_mebibytes(resources.memory_bytes)}mb")
    values.append("#PBS -l " + ":".join(chunks))
    if resources.walltime_seconds is not None:
        values.append(f"#PBS -l walltime={_walltime(resources.walltime_seconds)}")
    return values


def _target_name(plan: ExecutionPlan, inventory: InventorySnapshot) -> str | None:
    if plan.execution_target_id is None:
        return None
    target = next(
        (item for item in inventory.targets if item.id == plan.execution_target_id), None
    )
    if target is None:
        raise BackendError("execution target is absent from the plan inventory")
    return target.name


def _import_result(store: ExecutionStore, result: WorkerResult) -> None:
    details: dict[str, object] = {"preflight": result.preflight}
    expected_request = store.request_for_execution(result.execution_id)
    if result.protocol_version >= 2:
        if expected_request is None or result.request_id != expected_request.id:
            raise BackendError("worker result request does not match the immutable plan")
        _validate_request_outcomes(store, result, expected_request)
    elif result.request_id is not None or result.telemetry is not None or result.verification is not None:
        raise BackendError("released v0.4 worker results cannot carry v0.5 outcomes")
    telemetry = result.telemetry
    scheduler_job = store.get_scheduler_job(result.execution_id)
    if (
        telemetry is not None
        and scheduler_job is not None
        and result.experiment is not None
    ):
        telemetry = add_scheduler_wait(
            telemetry,
            submitted_at=scheduler_job.submitted_at,
            execution_started_at=result.experiment.started_at,
        )
    if result.experiment is None:
        store.import_worker_failure(
            result.execution_id, result.allocation, state=result.state,
            occurred_at=result.created_at, details=details, error=result.error,
        )
    else:
        store.import_experiment_result(
            result.execution_id, result.experiment, result.artifacts,
            result.lineage, result.allocation, state=result.state,
            occurred_at=result.created_at, details=details,
            telemetry=telemetry,
            verification=result.verification,
        )


def _validate_result_against_plan(
    result: WorkerResult, plan: ExecutionPlan
) -> None:
    experiment = result.experiment
    if experiment is None:
        return
    if (
        experiment.command != plan.executable
        or experiment.arguments != plan.arguments
        or experiment.working_directory != plan.working_directory
    ):
        raise BackendError("worker experiment does not match the immutable plan")


def _validate_request_outcomes(
    store: ExecutionStore,
    result: WorkerResult,
    request: ExecutionRequest,
) -> None:
    experiment = result.experiment
    if experiment is None:
        return
    execution = store.get_execution(result.execution_id)
    plan = store.get_plan(execution.plan_id)
    expected_verification = evaluate_verification(
        request, result.execution_id, experiment, result.artifacts
    )
    if result.verification is None or _verification_semantics(
        result.verification
    ) != _verification_semantics(expected_verification):
        raise BackendError(
            "worker verification does not match captured artifact evidence"
        )
    expected_telemetry = build_telemetry_summary(
        request,
        plan,
        result.execution_id,
        experiment,
        result.artifacts,
        result.allocation,
    )
    if _telemetry_semantics(result.telemetry) != _telemetry_semantics(
        expected_telemetry
    ):
        raise BackendError("worker telemetry does not match captured execution evidence")


def _verification_semantics(value: VerificationRun | None) -> object:
    if value is None:
        return None
    return {
        "aggregate_state": value.aggregate_state,
        "source": value.source,
        "checks": [
            {
                "ordinal": item.ordinal,
                "check_type": item.check_type,
                "output_path": item.output_path,
                "state": item.state,
                "evidence": item.evidence,
            }
            for item in value.checks
        ],
    }


def _telemetry_semantics(value: ExecutionTelemetrySummary | None) -> object:
    if value is None:
        return None
    payload = value.to_dict()
    payload.pop("id", None)
    return payload


def _execution_state(observation: SchedulerObservation) -> str:
    if not observation.observable:
        return "scheduler_unobservable"
    if observation.terminal:
        return "scheduler_terminal"
    return {
        "pending": "queued", "queued": "queued", "configuring": "queued",
        "running": "running", "completing": "running", "cancelled": "cancelled",
    }.get(observation.state, "submitted")


def _failure_detail(result: BoundedCommandResult) -> str:
    if result.timed_out:
        return "scheduler command timed out"
    return (result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}")[:4096]


def _normalize_slurm_state(value: str) -> str:
    return value.strip().split(None, 1)[0].rstrip("+").replace("-", "_").casefold()


def _parse_slurm_accounting_state(stdout: str, job_id: str) -> str | None:
    for line in stdout.splitlines():
        fields = line.split("|")
        if len(fields) >= 2 and fields[0].strip() == job_id:
            state = fields[1].strip()
            return None if not state else _normalize_slurm_state(state)
    return None


def _pbs_job_is_unobservable(result: BoundedCommandResult) -> bool:
    diagnostic = f"{result.stderr}\n{result.stdout}".casefold()
    return bool(
        re.search(
            r"unknown\s+job(?:\s+id)?|job(?:\s+id)?\s+(?:does\s+not\s+exist|not\s+found)",
            diagnostic,
        )
    )


def _walltime(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def _mebibytes(value: int) -> int:
    return max(1, (value + 1024 * 1024 - 1) // (1024 * 1024))
