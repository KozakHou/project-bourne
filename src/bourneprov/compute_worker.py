"""Portable execution-plane worker used inside scheduler allocations."""

from __future__ import annotations

import json
import os
import platform
import re
import sys
from pathlib import Path
from typing import Any

from .artifacts import capture_artifacts
from .collectors.system import collect_system
from .collectors.execution_context import resolve_executable
from .ids import new_ulid
from .execution_outcomes import build_telemetry_summary, evaluate_verification
from .execution_request import ExecutionRequest
from .lifecycle import run_experiment
from .models import ExperimentLineage
from .worker_result import WorkerResult, encode_worker_result
from .workload import utc_now
from .workload_models import AllocationObservation, ExecutionPlan, WorkloadSpec

MAX_PLAN_BYTES = 1024 * 1024
_ALLOCATION_ENVIRONMENT = {
    "SLURM_JOB_ID": "slurm_job_id",
    "SLURM_JOB_PARTITION": "slurm_partition",
    "SLURM_JOB_NUM_NODES": "nodes",
    "SLURM_CPUS_ON_NODE": "cpus_per_node",
    "SLURM_NTASKS": "mpi_ranks",
    "SLURM_GPUS": "slurm_gpus",
    "PBS_JOBID": "pbs_job_id",
    "PBS_QUEUE": "pbs_queue",
    "PBS_NP": "cpus",
    "PBS_NODEFILE": "pbs_nodefile",
    "CUDA_VISIBLE_DEVICES": "cuda_visible_devices",
}


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 3:
        print("usage: worker.pyz PLAN RESULT EXECUTION_ID", file=sys.stderr)
        return 2
    plan_path, result_path, execution_id = map(str, arguments)
    try:
        plan, workload, request = _load_plan(Path(plan_path), execution_id)
        result = execute_plan(plan, workload, execution_id, request=request)
        _write_immutable(Path(result_path), encode_worker_result(result))
    except Exception as exc:
        print(f"bourne worker failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 70
    return 0 if result.state == "completed" else result.experiment.exit_code if result.experiment else 75


def execute_plan(
    plan: ExecutionPlan,
    workload: WorkloadSpec,
    execution_id: str,
    *,
    request: ExecutionRequest | None = None,
) -> WorkerResult:
    allocation = _observe_allocation(execution_id, direct=plan.backend == "direct")
    problems, preflight = _preflight(plan, allocation)
    if problems:
        return WorkerResult(
            execution_id=execution_id, state="preflight_failed", created_at=utc_now(),
            experiment=None, artifacts=[], lineage=[], allocation=allocation,
            preflight={**preflight, "status": "failed", "problems": problems},
            error="; ".join(problems),
            request_id=None if request is None else request.id,
            protocol_version=1 if request is None else 2,
        )
    cwd = Path(plan.working_directory)
    experiment_id = new_ulid()
    inputs = capture_artifacts(experiment_id, "input", list(plan.inputs), cwd)
    experiment = run_experiment(
        plan.argv, cwd=cwd, stdout_stream=sys.stdout, stderr_stream=sys.stderr,
        experiment_id=experiment_id,
    )
    outputs = capture_artifacts(experiment_id, "output", list(plan.outputs), cwd)
    lineage = (
        [
            ExperimentLineage(
                child_experiment_id=experiment_id,
                parent_experiment_id=workload.parent_experiment_id,
                relationship="derived_from", created_at=experiment.ended_at,
            )
        ]
        if workload.parent_experiment_id is not None
        else []
    )
    telemetry = (
        None
        if request is None
        else build_telemetry_summary(
            request, plan, execution_id, experiment, [*inputs, *outputs], allocation
        )
    )
    verification = (
        None
        if request is None
        else evaluate_verification(
            request, execution_id, experiment, [*inputs, *outputs]
        )
    )
    return WorkerResult(
        execution_id=execution_id, state=experiment.status,
        created_at=experiment.ended_at, experiment=experiment,
        artifacts=[*inputs, *outputs], lineage=lineage, allocation=allocation,
        preflight={**preflight, "status": "passed", "problems": []},
        request_id=None if request is None else request.id,
        telemetry=telemetry,
        verification=verification,
        protocol_version=1 if request is None else 2,
    )


def _load_plan(
    path: Path, execution_id: str
) -> tuple[ExecutionPlan, WorkloadSpec, ExecutionRequest | None]:
    if path.stat().st_size > MAX_PLAN_BYTES:
        raise ValueError("staged plan exceeds the size limit")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") not in {1, 2}:
        raise ValueError("unsupported staged plan")
    if value.get("execution_id") != execution_id:
        raise ValueError("staged plan execution ID does not match")
    plan_value = value.get("plan")
    workload_value = value.get("workload")
    if not isinstance(plan_value, dict) or not isinstance(workload_value, dict):
        raise ValueError("staged plan is malformed")
    _validate_staged_models(plan_value, workload_value)
    plan = ExecutionPlan.from_dict(plan_value)
    workload = WorkloadSpec.from_dict(workload_value)
    request_value = value.get("request")
    if value["schema_version"] == 2:
        if not isinstance(request_value, dict):
            raise ValueError("version-2 staged plan requires an execution request")
        request = ExecutionRequest.from_dict(request_value)
    else:
        request = None
    if plan.workload_id != workload.id:
        raise ValueError("staged workload does not match plan")
    if (
        plan.executable != workload.executable
        or plan.arguments != workload.arguments
        or plan.working_directory != workload.working_directory
        or plan.inputs != workload.inputs
        or plan.outputs != workload.outputs
        or plan.requested_resources != workload.resources
    ):
        raise ValueError("staged plan content does not match its workload")
    if request is not None and (
        request.argv != workload.argv
        or request.resolved_working_directory != workload.working_directory
        or list(request.artifacts.inputs) != workload.inputs
        or list(request.artifacts.outputs) != workload.outputs
        or request.resources != workload.resources
        or request.execution != workload.constraints
        or request.parent_experiment_id != workload.parent_experiment_id
    ):
        raise ValueError("staged request does not match its workload")
    return plan, workload, request


def _validate_staged_models(
    plan: dict[str, Any], workload: dict[str, Any]
) -> None:
    if plan.get("backend") not in {"direct", "slurm", "pbs"}:
        raise ValueError("staged plan backend is invalid")
    for value, label in ((plan, "plan"), (workload, "workload")):
        if not isinstance(value.get("id"), str):
            raise ValueError(f"staged {label} ID is invalid")
        if not isinstance(value.get("executable"), str) or not value["executable"]:
            raise ValueError(f"staged {label} executable is invalid")
        for field in ("arguments", "inputs", "outputs"):
            items = value.get(field)
            if (
                not isinstance(items, list)
                or len(items) > 4096
                or not all(isinstance(item, str) for item in items)
            ):
                raise ValueError(f"staged {label} {field} are invalid")
        if not isinstance(value.get("working_directory"), str):
            raise ValueError(f"staged {label} working directory is invalid")


def _observe_allocation(
    execution_id: str, *, direct: bool = False
) -> AllocationObservation:
    raw = {
        field: value
        for variable, field in _ALLOCATION_ENVIRONMENT.items()
        if (value := os.environ.get(variable)) is not None
    }
    resources: dict[str, Any] = {}
    for key in ("nodes", "cpus_per_node", "mpi_ranks", "cpus"):
        if key in raw:
            parsed = _positive_int(raw[key])
            if parsed is not None:
                resources[key] = parsed
    if "nodes" in resources and "cpus_per_node" in resources:
        resources["cpus"] = resources["nodes"] * resources["cpus_per_node"]
    gpu_count = _allocated_gpu_count(raw)
    if gpu_count is not None:
        resources["gpus"] = gpu_count
    if direct:
        cpu_count = os.cpu_count()
        if cpu_count is not None:
            resources["cpus"] = cpu_count
        resources["nodes"] = 1
        try:
            resources["gpus"] = len(collect_system().gpus)
        except Exception:
            pass
    return AllocationObservation(
        id=new_ulid(), execution_id=execution_id, observed_at=utc_now(),
        resources=resources, hosts=[platform.node() or "unknown"],
        evidence={"environment": raw, "source": "compute_worker_allowlist"},
    )


def _preflight(
    plan: ExecutionPlan,
    allocation: AllocationObservation,
) -> tuple[list[str], dict[str, Any]]:
    problems: list[str] = []
    cwd = Path(plan.working_directory)
    try:
        cwd_ok = cwd.is_dir()
    except OSError:
        cwd_ok = False
    resolved = resolve_executable(plan.executable, cwd) if cwd_ok else None
    if not cwd_ok:
        problems.append("working directory is unavailable on the execution host")
    elif resolved is None:
        problems.append("requested executable is unavailable on the execution host")
    requested = plan.requested_resources
    for name in ("nodes", "cpus", "gpus", "mpi_ranks"):
        required = getattr(requested, name)
        observed = allocation.resources.get(name)
        if required is not None and observed is not None and int(observed) < required:
            problems.append(
                f"allocated {name} ({observed}) are below the requested value ({required})"
            )
    return problems, {
        "observed_at": utc_now(), "working_directory_available": cwd_ok,
        "requested_executable": plan.executable, "resolved_executable": resolved,
        "requested_resources": vars(requested),
        "observed_resources": allocation.resources,
    }


def _allocated_gpu_count(raw: dict[str, str]) -> int | None:
    if "cuda_visible_devices" in raw:
        value = raw["cuda_visible_devices"].strip()
        if value in {"", "-1", "NoDevFiles"}:
            return 0
        return len([item for item in value.split(",") if item.strip()])
    value = raw.get("slurm_gpus")
    if value is None:
        return None
    digits = [int(item) for item in re.findall(r"\d+", value)]
    return digits[-1] if digits else None


def _positive_int(value: str) -> int | None:
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _write_immutable(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
