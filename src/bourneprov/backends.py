"""Framework-independent direct, Slurm, and PBS execution backends."""

from __future__ import annotations

import getpass
import re
import shlex
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from .bounded_subprocess import BoundedCommandResult, run_bounded_command
from .compute_worker import execute_plan
from .inventory_models import InventorySnapshot
from .worker_bundle import build_worker_zipapp, write_staged_plan
from .worker_result import WorkerResult, WorkerResultError, load_worker_result
from .workload import utc_now
from .workload_models import ExecutionAttempt, ExecutionPlan, SchedulerJob, WorkloadSpec
from .workload_storage import ExecutionStore

SCHEDULER_TIMEOUT_SECONDS = 10.0
MAX_SCHEDULER_OUTPUT_BYTES = 1024 * 1024
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
        result = execute_plan(plan, workload, execution.id)
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
            staging / "plan.json", execution.id, plan, workload
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
        identity = execution.submitting_identity or _current_identity()
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
        executable = shutil.which(self.status_command)
        if executable is None:
            raise BackendError(f"{self.status_command} executable is unavailable")
        result = self._run([executable, *self._status_arguments(job.job_id, job.submitting_identity)])
        state = self._parse_status(result)
        now = utc_now()
        self.store.update_scheduler_job(execution.id, state, now)
        self.store.update_execution_state(
            execution.id, _execution_state(state), now,
            {"scheduler_state": state, "job_id": job.job_id},
        )
        return state

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
        poll_seconds: float = 2.0,
        timeout_seconds: float | None = None,
    ) -> WorkerResult:
        started = time.monotonic()
        while True:
            current = self.store.get_execution(execution.id)
            try:
                return self.collect(current)
            except BackendError:
                pass
            state = self.status(current)
            if state in self.terminal_states:
                try:
                    return self.collect(self.store.get_execution(execution.id))
                except BackendError as exc:
                    now = utc_now()
                    self.store.update_execution_state(
                        execution.id, "collection_failed", now,
                        {"scheduler_state": state, "diagnostic": str(exc)},
                        error=str(exc),
                    )
                    raise
            if timeout_seconds is not None and time.monotonic() - started >= timeout_seconds:
                raise TimeoutError(f"timed out waiting for execution {execution.id}")
            time.sleep(max(0.05, poll_seconds))

    def cancel(self, execution: ExecutionAttempt) -> None:
        self.store.record_execution_event(
            execution.id, "cancellation_requested", utc_now(),
            {"requested_by": _current_identity()},
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
        if job.submitting_identity != _current_identity():
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

    def _cancel_arguments(self, job_id: str, identity: str) -> list[str]:
        raise NotImplementedError


class SlurmBackend(SchedulerBackend):
    name = "slurm"
    submit_command = "sbatch"
    status_command = "squeue"
    cancel_command = "scancel"

    @property
    def terminal_states(self) -> set[str]:
        return {"completed", "failed", "cancelled", "timeout", "node_fail"}

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
        return "unknown" if not value else value[0].strip().casefold()

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


def _execution_state(scheduler_state: str) -> str:
    return {
        "pending": "queued", "queued": "queued", "configuring": "queued",
        "running": "running", "completing": "running", "cancelled": "cancelled",
    }.get(scheduler_state, "submitted")


def _failure_detail(result: BoundedCommandResult) -> str:
    if result.timed_out:
        return "scheduler command timed out"
    return (result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}")[:4096]


def _current_identity() -> str:
    try:
        return getpass.getuser()
    except (OSError, KeyError):
        return "unknown"


def _walltime(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"


def _mebibytes(value: int) -> int:
    return max(1, (value + 1024 * 1024 - 1) // (1024 * 1024))
