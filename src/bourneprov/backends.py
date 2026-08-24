"""Framework-independent direct, Slurm, and PBS execution backends."""

from __future__ import annotations

import re
import shlex
import shutil
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Protocol, TextIO

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
from .planning_models import ResourceShape
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
_SAFE_PBS_JOB = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_SAFE_LSF_JOB = re.compile(r"[0-9]+\Z")

CommandRunner = Callable[..., BoundedCommandResult]


class BackendError(RuntimeError):
    pass


class AmbiguousSubmission(BackendError):
    """The scheduler may have accepted a job but did not return one exact ID."""


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

    def __init__(
        self,
        store: ExecutionStore,
        *,
        stdout_stream: TextIO | None = None,
        stderr_stream: TextIO | None = None,
    ):
        self.store = store
        self.stdout_stream = stdout_stream
        self.stderr_stream = stderr_stream

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
            plan,
            workload,
            execution.id,
            request=request,
            stdout_stream=self.stdout_stream,
            stderr_stream=self.stderr_stream,
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
        suffix = {"slurm": "job.slurm", "pbs": "job.pbs", "lsf": "job.lsf"}[self.name]
        script_path = staging / suffix
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
            detail = f"{self.submit_command} executable is unavailable"
            self.store.update_execution_state(
                execution.id, "failed", utc_now(),
                {"phase": "submission", "diagnostic": detail}, error=detail,
            )
            raise BackendError(detail)
        result = self._run(
            [executable, *self._submit_arguments(script_path)],
            input_bytes=self._submission_input(script_path),
        )
        if result.timed_out:
            detail = "scheduler submission timed out; acceptance is unknown"
            self.store.update_execution_state(
                execution.id, "submission_ambiguous", utc_now(),
                {"phase": "submission", "diagnostic": detail, "retry_safe": False},
                error=detail,
            )
            raise AmbiguousSubmission(detail)
        if result.returncode != 0:
            detail = _failure_detail(result)
            self.store.update_execution_state(
                execution.id, "failed", utc_now(),
                {"phase": "submission", "diagnostic": detail}, error=detail,
            )
            raise BackendError(f"{self.name} submission failed: {detail}")
        try:
            job_id = self._parse_submission(result.stdout)
        except AmbiguousSubmission as exc:
            self.store.update_execution_state(
                execution.id, "submission_ambiguous", utc_now(),
                {
                    "phase": "submission", "diagnostic": str(exc),
                    "retry_safe": False,
                },
                error=str(exc),
            )
            raise
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
                    result_path = (
                        None
                        if current.staging_directory is None
                        else Path(current.staging_directory) / "result.json"
                    )
                    result_evidence = (
                        "partial"
                        if result_path is not None and result_path.is_file()
                        else "missing"
                    )
                    details = observation.details(self.name, job.job_id)
                    details.update(
                        {
                            "result_bundle": (
                                "invalid" if result_evidence == "partial" else "absent"
                            ),
                            "scientific_completion_established": False,
                            "collection_diagnostic": str(exc)[:4096],
                            "termination_phase": "scheduler",
                            "termination_outcome": _scheduler_outcome(
                                observation.state, result_evidence=result_evidence
                            ),
                            "result_evidence": result_evidence,
                            "telemetry_evidence": "unavailable",
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
            {
                "scheduler_family": self.name, "job_id": job.job_id,
                "termination_phase": "scheduler",
                "termination_outcome": "scheduler_cancelled",
                "result_evidence": "unknown", "telemetry_evidence": "unknown",
            },
        )

    @property
    def terminal_states(self) -> set[str]:
        raise NotImplementedError

    def _run(
        self, argv: list[str], *, input_bytes: bytes | None = None
    ) -> BoundedCommandResult:
        try:
            keywords = {
                "timeout": SCHEDULER_TIMEOUT_SECONDS,
                "max_output_bytes": MAX_SCHEDULER_OUTPUT_BYTES,
            }
            if input_bytes is not None:
                keywords["input_bytes"] = input_bytes
            return self.runner(argv, **keywords)
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

    def _submission_input(self, script_path: Path) -> bytes | None:
        del script_path
        return None

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


class LSFBackend(SchedulerBackend):
    name = "lsf"
    submit_command = "bsub"
    status_command = "bjobs"
    cancel_command = "bkill"

    @property
    def terminal_states(self) -> set[str]:
        return {
            "completed", "failed", "cancelled", "timeout", "out_of_memory",
            "node_fail", "unknown_terminal",
        }

    def _submit_arguments(self, script_path: Path) -> list[str]:
        del script_path
        return []

    def _submission_input(self, script_path: Path) -> bytes | None:
        return script_path.read_bytes()

    def _parse_submission(self, stdout: str) -> str:
        matches = re.findall(r"\bJob\s+<([0-9]+)>", stdout)
        if len(matches) != 1 or not _SAFE_LSF_JOB.fullmatch(matches[0]):
            raise AmbiguousSubmission("LSF returned no unique exact job identity")
        return matches[0]

    def _status_arguments(self, job_id: str, identity: str) -> list[str]:
        return ["-noheader", "-u", identity, "-o", "jobid stat", job_id]

    def _parse_status(self, result: BoundedCommandResult) -> str:
        if result.returncode != 0 or result.timed_out:
            raise BackendError(f"LSF status failed: {_failure_detail(result)}")
        parsed = _parse_lsf_job_line(result.stdout)
        if parsed is None:
            return "unobservable"
        _job_id, raw_state = parsed
        return _normalize_lsf_state(raw_state)

    def _observe_job(
        self, job: SchedulerJob, status_executable: str
    ) -> SchedulerObservation:
        active = self._run(
            [status_executable, *self._status_arguments(job.job_id, job.submitting_identity)]
        )
        parsed = None if active.returncode != 0 or active.timed_out else _parse_lsf_job_line(
            active.stdout, expected_job_id=job.job_id
        )
        if parsed is not None:
            state = _normalize_lsf_state(parsed[1])
            return SchedulerObservation(
                state=state, source="active", observable=True,
                terminal=state in self.terminal_states,
            )
        if active.timed_out:
            raise BackendError(f"LSF active status timed out: {_failure_detail(active)}")
        if active.returncode != 0 and not _lsf_job_is_unobservable(active):
            raise BackendError(f"LSF active status failed: {_failure_detail(active)}")
        if active.returncode == 0 and active.stdout.strip():
            raise BackendError(
                "LSF active status did not contain one unique exact job record"
            )
        historical = self._run(
            [
                status_executable, "-a", "-noheader", "-u",
                job.submitting_identity, "-o", "jobid stat", job.job_id,
            ]
        )
        historical_parsed = (
            None
            if historical.returncode != 0 or historical.timed_out
            else _parse_lsf_job_line(historical.stdout, expected_job_id=job.job_id)
        )
        if historical_parsed is None:
            if historical.timed_out:
                diagnostic = "LSF historical/accounting query timed out"
            elif historical.returncode == 0 and historical.stdout.strip():
                diagnostic = (
                    "LSF historical/accounting output did not contain one "
                    "unique exact job record"
                )
            else:
                diagnostic = "known job was absent from active and historical LSF views"
            return SchedulerObservation(
                state="unobservable", source="historical_unavailable",
                observable=False, terminal=False, diagnostic=diagnostic,
            )
        state = _normalize_lsf_state(historical_parsed[1])
        return SchedulerObservation(
            state=state, source="historical_accounting", observable=True,
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

    if family not in {"slurm", "pbs", "lsf"}:
        raise ValueError("unsupported scheduler family")
    if target_name is not None and not _SAFE_SCHEDULER_NAME.fullmatch(target_name):
        raise ValueError("scheduler target name contains unsafe characters")
    directives = (
        _slurm_directives(plan, target_name)
        if family == "slurm"
        else _pbs_directives(plan, target_name)
        if family == "pbs"
        else _lsf_directives(plan, target_name)
    )
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
    values = ["#SBATCH --job-name=bourne"]
    if target_name:
        values.append(f"#SBATCH --partition={target_name}")
    if plan.resource_shape is not None:
        shape = plan.resource_shape
        if shape.nodes is not None:
            values.append(f"#SBATCH --nodes={shape.nodes}")
        if shape.mpi_ranks is not None:
            values.append(f"#SBATCH --ntasks={shape.mpi_ranks}")
        if shape.ranks_per_node is not None:
            values.append(f"#SBATCH --ntasks-per-node={shape.ranks_per_node}")
        threads = _shape_threads_per_rank(shape)
        if threads is not None:
            values.append(f"#SBATCH --cpus-per-task={threads}")
        elif shape.mpi_ranks is None:
            if shape.cpus_per_node is not None:
                values.append("#SBATCH --ntasks-per-node=1")
                values.append(f"#SBATCH --cpus-per-task={shape.cpus_per_node}")
            elif shape.total_cpus is not None and shape.nodes in {None, 1}:
                values.append(f"#SBATCH --cpus-per-task={shape.total_cpus}")
            elif shape.total_cpus is not None:
                raise ValueError(
                    "multi-node Slurm shape requires CPUs per node or MPI layout"
                )
        if shape.gpus_per_node is not None:
            values.append(f"#SBATCH --gpus-per-node={shape.gpus_per_node}")
        elif shape.gpus is not None:
            values.append(f"#SBATCH --gpus={shape.gpus}")
        memory_per_node = _shape_per_node(
            shape.memory_bytes, shape.memory_per_node_bytes, shape.nodes, "memory"
        )
        if memory_per_node is not None:
            values.append(f"#SBATCH --mem={_mebibytes(memory_per_node)}M")
        if shape.walltime_seconds is not None:
            values.append(f"#SBATCH --time={_walltime(shape.walltime_seconds)}")
        return values

    resources = plan.requested_resources
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
    values = ["#PBS -N bourne"]
    if target_name:
        values.append(f"#PBS -q {target_name}")
    if plan.resource_shape is not None:
        shape = plan.resource_shape
        nodes = shape.nodes or 1
        chunks = [f"select={nodes}"]
        cpus_per_node = _shape_per_node(
            shape.total_cpus, shape.cpus_per_node, shape.nodes, "CPUs"
        )
        if cpus_per_node is not None:
            chunks.append(f"ncpus={cpus_per_node}")
        gpus_per_node = _shape_per_node(
            shape.gpus, shape.gpus_per_node, shape.nodes, "GPUs"
        )
        if gpus_per_node is not None:
            chunks.append(f"ngpus={gpus_per_node}")
        memory_per_node = _shape_per_node(
            shape.memory_bytes, shape.memory_per_node_bytes, shape.nodes, "memory"
        )
        if memory_per_node is not None:
            chunks.append(f"mem={_mebibytes(memory_per_node)}mb")
        ranks_per_node = _shape_per_node(
            shape.mpi_ranks, shape.ranks_per_node, shape.nodes, "MPI ranks"
        )
        if ranks_per_node is not None:
            chunks.append(f"mpiprocs={ranks_per_node}")
        if shape.threads_per_rank is not None:
            chunks.append(f"ompthreads={shape.threads_per_rank}")
        values.append("#PBS -l " + ":".join(chunks))
        if shape.walltime_seconds is not None:
            values.append(f"#PBS -l walltime={_walltime(shape.walltime_seconds)}")
        return values

    resources = plan.requested_resources
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


def _lsf_directives(plan: ExecutionPlan, target_name: str | None) -> list[str]:
    values = ["#BSUB -J bourne"]
    if target_name:
        values.append(f"#BSUB -q {target_name}")
    shape = plan.resource_shape
    resources = plan.requested_resources
    nodes = shape.nodes if shape is not None else resources.nodes
    ranks = shape.mpi_ranks if shape is not None else resources.mpi_ranks
    cpus = shape.total_cpus if shape is not None else resources.cpus
    gpus = shape.gpus if shape is not None else resources.gpus
    memory = shape.memory_bytes if shape is not None else resources.memory_bytes
    walltime = shape.walltime_seconds if shape is not None else resources.walltime_seconds
    slots = ranks or cpus
    if slots is not None:
        values.append(f"#BSUB -n {slots}")
    per_host = None
    if shape is not None:
        per_host = (
            shape.ranks_per_node if ranks is not None else shape.cpus_per_node
        )
    if nodes is not None:
        if slots is None:
            raise ValueError(
                "LSF node count requires total CPUs or MPI ranks for a portable mapping"
            )
        if per_host is not None and slots == nodes * per_host:
            values.append(f'#BSUB -R "span[ptile={per_host}]"')
        elif nodes == 1:
            values.append('#BSUB -R "span[hosts=1]"')
        else:
            raise ValueError(
                "LSF multi-node shape requires a divisible per-host CPU or MPI layout"
            )
    elif per_host is not None:
        values.append(f'#BSUB -R "span[ptile={per_host}]"')
    if gpus not in {None, 0}:
        raise ValueError(
            "LSF GPU mapping is unresolved; Bourne will not invent site-specific -gpu semantics"
        )
    if memory is not None:
        raise ValueError(
            "LSF memory mapping is unresolved; Bourne will not invent site-specific -M units"
        )
    if walltime is not None:
        values.append(f"#BSUB -W {max(1, (walltime + 59) // 60)}")
    return values


def _shape_threads_per_rank(shape: ResourceShape) -> int | None:
    if shape.threads_per_rank is not None:
        return shape.threads_per_rank
    if shape.total_cpus is None or shape.mpi_ranks is None:
        return None
    if shape.total_cpus % shape.mpi_ranks:
        raise ValueError("resource-shape CPUs are not divisible across MPI ranks")
    return shape.total_cpus // shape.mpi_ranks


def _shape_per_node(
    total: int | None,
    per_node: int | None,
    nodes: int | None,
    label: str,
) -> int | None:
    if per_node is not None:
        return per_node
    if total is None:
        return None
    effective_nodes = nodes or 1
    if total % effective_nodes:
        raise ValueError(f"resource-shape {label} are not divisible across nodes")
    return total // effective_nodes


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
    runtime_evidence = result.runtime_evidence
    if runtime_evidence is not None and scheduler_job is not None:
        runtime_evidence = replace(
            runtime_evidence,
            allocation=replace(
                runtime_evidence.allocation,
                metrics={
                    **runtime_evidence.allocation.metrics,
                    "controller_scheduler_job": {
                        "family": scheduler_job.family,
                        "job_id": scheduler_job.job_id,
                        "submitting_identity": scheduler_job.submitting_identity,
                        "submitted_at": scheduler_job.submitted_at,
                        "last_observed_state": scheduler_job.state,
                    },
                },
            ),
        )
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
            runtime_evidence=runtime_evidence,
            termination=result.termination,
        )
    else:
        store.import_experiment_result(
            result.execution_id, result.experiment, result.artifacts,
            result.lineage, result.allocation, state=result.state,
            occurred_at=result.created_at, details=details,
            telemetry=telemetry,
            verification=result.verification,
            runtime_evidence=runtime_evidence,
            termination=result.termination,
        )


def _validate_result_against_plan(
    result: WorkerResult, plan: ExecutionPlan
) -> None:
    experiment = result.experiment
    if experiment is None:
        return
    expected_argv = _planned_execution_argv(plan)
    if (
        experiment.command != expected_argv[0]
        or experiment.arguments != expected_argv[1:]
        or experiment.working_directory != plan.working_directory
    ):
        raise BackendError("worker experiment does not match the immutable plan")


def _planned_execution_argv(plan: ExecutionPlan) -> list[str]:
    if plan.container is None:
        return plan.argv
    container = plan.container
    argv = [container.runtime, "exec"]
    if container.clean_environment:
        argv.append("--cleanenv")
    argv.extend(["--pwd", plan.working_directory])
    for mount in container.mounts:
        specification = f"{mount.source}:{mount.destination}"
        if mount.read_only:
            specification += ":ro"
        argv.extend(["--bind", specification])
    return [*argv, container.image, *plan.argv]


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


def _scheduler_outcome(state: str, *, result_evidence: str = "missing") -> str:
    scheduler = {
        "cancelled": "scheduler_cancelled",
        "timeout": "scheduler_timeout",
        "out_of_memory": "out_of_memory",
        "node_fail": "node_failure",
    }.get(state)
    if scheduler is not None:
        return scheduler
    return (
        "result_bundle_partial"
        if result_evidence == "partial"
        else "result_bundle_missing"
    )


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


def _parse_lsf_job_line(
    stdout: str, expected_job_id: str | None = None
) -> tuple[str, str] | None:
    rows: list[tuple[str, str]] = []
    for line in stdout.splitlines():
        fields = line.split()
        if len(fields) == 2 and _SAFE_LSF_JOB.fullmatch(fields[0]):
            if expected_job_id is None or fields[0] == expected_job_id:
                rows.append((fields[0], fields[1]))
    return rows[0] if len(rows) == 1 else None


def _normalize_lsf_state(value: str) -> str:
    return {
        "PEND": "pending",
        "WAIT": "pending",
        "PROV": "pending",
        "RUN": "running",
        "PSUSP": "suspended",
        "USUSP": "suspended",
        "SSUSP": "suspended",
        "DONE": "completed",
        "EXIT": "failed",
        "ZOMBI": "failed",
        "UNKWN": "unknown_terminal",
    }.get(value.strip().upper(), "unknown")


def _pbs_job_is_unobservable(result: BoundedCommandResult) -> bool:
    diagnostic = f"{result.stderr}\n{result.stdout}".casefold()
    return bool(
        re.search(
            r"unknown\s+job(?:\s+id)?|job(?:\s+id)?\s+(?:does\s+not\s+exist|not\s+found)",
            diagnostic,
        )
    )


def _lsf_job_is_unobservable(result: BoundedCommandResult) -> bool:
    diagnostic = f"{result.stderr}\n{result.stdout}".casefold()
    return bool(
        re.search(
            r"job(?:\s+<[^>]+>)?\s+(?:is\s+not\s+found|not\s+found)"
            r"|no\s+(?:unfinished\s+)?job\s+found"
            r"|not\s+found\s+in\s+job\s+list",
            diagnostic,
        )
    )


def _walltime(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def _mebibytes(value: int) -> int:
    return max(1, (value + 1024 * 1024 - 1) // (1024 * 1024))
