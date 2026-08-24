"""Portable execution-plane worker used inside scheduler allocations."""

from __future__ import annotations

import json
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, TextIO

from .artifacts import capture_artifacts
from .collectors.system import collect_system
from .ids import new_ulid
from .execution_outcomes import build_telemetry_summary, evaluate_verification
from .execution_request import ExecutionRequest
from .lifecycle import run_experiment
from .models import ExperimentLineage, SystemProvenance
from .runtime_evidence import (
    RuntimeEvidence,
    RuntimeEvidenceGroup,
    TerminationEvidence,
)
from .worker_result import WorkerResult, encode_worker_result
from .workload import utc_now
from .workload_models import AllocationObservation, ExecutionPlan, WorkloadSpec

MAX_PLAN_BYTES = 1024 * 1024
MAX_CAPTURE_BYTES_PER_STREAM = 8 * 1024 * 1024
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
    "LSB_JOBID": "lsf_job_id",
    "LSB_QUEUE": "lsf_queue",
    "LSB_HOSTS": "lsf_hosts",
    "LSB_MCPU_HOSTS": "lsf_mcpu_hosts",
    "LSB_DJOB_NUMPROC": "mpi_ranks",
    "LSB_GPU_REQ": "lsf_gpu_request",
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
    stdout_stream: TextIO | None = None,
    stderr_stream: TextIO | None = None,
) -> WorkerResult:
    allocation = _observe_allocation(execution_id, direct=plan.backend == "direct")
    problems, preflight, environment = _preflight(plan, allocation)
    if problems:
        runtime = _runtime_evidence(
            execution_id, None, {}, allocation, preflight, plan
        )
        return WorkerResult(
            execution_id=execution_id, state="preflight_failed", created_at=utc_now(),
            experiment=None, artifacts=[], lineage=[], allocation=allocation,
            preflight={**preflight, "status": "failed", "problems": problems},
            error="; ".join(problems),
            request_id=None if request is None else request.id,
            runtime_evidence=runtime,
            termination=TerminationEvidence(
                phase="preflight", outcome="preflight_failed",
                source="compute_worker_preflight", result_evidence="missing",
                telemetry_evidence="unavailable", diagnostic="; ".join(problems),
            ),
            protocol_version=1 if request is None else 3,
        )
    cwd = Path(plan.working_directory)
    experiment_id = new_ulid()
    inputs = capture_artifacts(experiment_id, "input", list(plan.inputs), cwd)
    runtime_capture: list[dict[str, Any]] = []
    collect_runtime = request is not None and request.telemetry_mode != "off"
    execution_argv = _execution_argv(plan)
    experiment = run_experiment(
        execution_argv,
        cwd=cwd,
        stdout_stream=sys.stdout if stdout_stream is None else stdout_stream,
        stderr_stream=sys.stderr if stderr_stream is None else stderr_stream,
        experiment_id=experiment_id,
        environment=environment,
        runtime_capture_out=runtime_capture if collect_runtime else None,
        capture_limit_bytes=MAX_CAPTURE_BYTES_PER_STREAM,
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
    runtime = (
        None
        if request is None
        else _runtime_evidence(
            execution_id, experiment.id,
            (
                runtime_capture[0]
                if runtime_capture
                else {
                    "collector": "disabled",
                    "diagnostic": "runtime collection was disabled by the execution request",
                }
            ),
            allocation, preflight, plan,
            system=experiment.system,
        )
    )
    termination = (
        None
        if request is None
        else _termination_evidence(
            experiment,
            runtime_capture[0] if runtime_capture else {
                "collector": "disabled",
                "diagnostic": "runtime collection was disabled by the execution request",
            },
            verification,
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
        runtime_evidence=runtime,
        termination=termination,
        protocol_version=1 if request is None else 3,
    )


def _load_plan(
    path: Path, execution_id: str
) -> tuple[ExecutionPlan, WorkloadSpec, ExecutionRequest | None]:
    if path.stat().st_size > MAX_PLAN_BYTES:
        raise ValueError("staged plan exceeds the size limit")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema_version") not in {1, 2, 3, 4}:
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
    if value["schema_version"] in {2, 3, 4}:
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
        or (
            request.resolved_parent_experiment_id
            != workload.parent_experiment_id
        )
    ):
        raise ValueError("staged request does not match its workload")
    return plan, workload, request


def _validate_staged_models(
    plan: dict[str, Any], workload: dict[str, Any]
) -> None:
    if plan.get("backend") not in {"direct", "slurm", "pbs", "lsf"}:
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
    if value := raw.get("lsf_mcpu_hosts"):
        fields = value.split()
        if len(fields) % 2 == 0:
            slots = [_positive_int(item) for item in fields[1::2]]
            if all(item is not None for item in slots):
                resources["nodes"] = len(fields) // 2
                resources["cpus"] = sum(int(item) for item in slots if item is not None)
    elif value := raw.get("lsf_hosts"):
        resources["nodes"] = len(set(value.split()))
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
    hosts = _allocation_hosts(raw)
    return AllocationObservation(
        id=new_ulid(), execution_id=execution_id, observed_at=utc_now(),
        resources=resources, hosts=hosts or [platform.node() or "unknown"],
        evidence={"environment": raw, "source": "compute_worker_allowlist"},
    )


def _preflight(
    plan: ExecutionPlan,
    allocation: AllocationObservation,
) -> tuple[list[str], dict[str, Any], dict[str, str]]:
    problems: list[str] = []
    observed_image_digest: str | None = None
    environment, activation_evidence, activation_problem = _activation_environment(plan)
    if activation_problem is not None:
        problems.append(activation_problem)
    cwd = Path(plan.working_directory)
    try:
        cwd_ok = cwd.is_dir()
    except OSError:
        cwd_ok = False
    requested_executable = (
        plan.container.runtime if plan.container is not None else plan.executable
    )
    resolved = (
        _resolve_with_environment(requested_executable, cwd, environment)
        if cwd_ok and activation_problem is None
        else None
    )
    if not cwd_ok:
        problems.append("working directory is unavailable on the execution host")
    elif resolved is None:
        problems.append("requested executable or container runtime is unavailable on the execution host")
    if plan.container is not None:
        image = Path(plan.container.image)
        if not image.is_file():
            problems.append("planned existing container image is unavailable")
        elif plan.container.image_digest is not None:
            observed_image_digest = "sha256:" + _sha256_file(image)
            if observed_image_digest != plan.container.image_digest:
                problems.append("planned container image digest does not match")
        for mount in plan.container.mounts:
            if not Path(mount.source).exists():
                problems.append(f"planned container bind source is unavailable: {mount.source}")
    requested = plan.requested_resources
    if plan.resource_shape is not None:
        shape = plan.resource_shape
        requested_values = {
            "nodes": shape.nodes,
            "cpus": shape.total_cpus,
            "gpus": shape.gpus,
            "mpi_ranks": shape.mpi_ranks,
        }
    else:
        requested_values = {
            name: getattr(requested, name)
            for name in ("nodes", "cpus", "gpus", "mpi_ranks")
        }
    for name in ("nodes", "cpus", "gpus", "mpi_ranks"):
        required = requested_values[name]
        observed = allocation.resources.get(name)
        if required is not None and observed is not None and int(observed) < required:
            problems.append(
                f"allocated {name} ({observed}) are below the requested value ({required})"
            )
    return problems, {
        "observed_at": utc_now(), "working_directory_available": cwd_ok,
        "requested_executable": requested_executable, "resolved_executable": resolved,
        "requested_resources": vars(requested),
        "selected_resource_shape": (
            None if plan.resource_shape is None else plan.resource_shape.to_dict()
        ),
        "environment_activation": activation_evidence,
        "container": (
            None
            if plan.container is None
            else {
                **plan.container.to_dict(),
                "observed_image_digest": observed_image_digest,
            }
        ),
        "observed_resources": allocation.resources,
    }, environment


def _activation_environment(
    plan: ExecutionPlan,
) -> tuple[dict[str, str], dict[str, Any], str | None]:
    environment = dict(os.environ)
    if plan.environment is None:
        return environment, {"kind": "none", "status": "not_planned"}, None
    activation = plan.environment.activation
    evidence: dict[str, Any] = {
        "kind": activation.kind,
        "names": list(activation.names),
        "prefix": activation.prefix,
        "status": "planned",
    }
    if activation.kind == "none":
        evidence["status"] = "reproduced"
        return environment, evidence, None
    if activation.kind in {"virtualenv", "conda", "spack"}:
        prefix = Path(activation.prefix or "")
        binary = prefix / "bin"
        if not prefix.is_dir() or not binary.is_dir():
            evidence["status"] = "failed"
            return environment, evidence, "planned environment prefix is unavailable"
        environment["PATH"] = str(binary) + os.pathsep + environment.get("PATH", "")
        if activation.kind == "virtualenv":
            environment["VIRTUAL_ENV"] = str(prefix)
        elif activation.kind == "conda":
            environment["CONDA_PREFIX"] = str(prefix)
        else:
            environment["SPACK_ENV"] = str(prefix)
        evidence["status"] = "reproduced"
        return environment, evidence, None
    shell = shutil.which("sh")
    if shell is None:
        evidence["status"] = "failed"
        return environment, evidence, "module activation requires a POSIX shell"
    # Fixed Bourne-owned activation template. Scientific argv is never part of it.
    result = subprocess.run(
        [
            shell, "-lc",
            'module load "$@" >/dev/null 2>&1 && env -0',
            "bourne-module", *activation.names,
        ],
        input=b"", capture_output=True, check=False, timeout=10,
    )
    if result.returncode != 0 or len(result.stdout) > 4 * 1024 * 1024:
        evidence["status"] = "failed"
        return environment, evidence, "planned module environment could not be reproduced"
    environment = {
        item.split(b"=", 1)[0].decode("utf-8", "surrogateescape"):
        item.split(b"=", 1)[1].decode("utf-8", "surrogateescape")
        for item in result.stdout.split(b"\0")
        if b"=" in item
    }
    evidence["status"] = "reproduced"
    return environment, evidence, None


def _resolve_with_environment(
    executable: str, cwd: Path, environment: dict[str, str]
) -> str | None:
    path = Path(executable)
    if path.is_absolute() or "/" in executable:
        candidate = path if path.is_absolute() else cwd / path
        return (
            str(candidate.resolve(strict=False))
            if candidate.is_file() and os.access(candidate, os.X_OK)
            else None
        )
    return shutil.which(executable, path=environment.get("PATH"))


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


def _allocation_hosts(raw: dict[str, str]) -> list[str]:
    if value := raw.get("lsf_mcpu_hosts"):
        fields = value.split()
        return list(dict.fromkeys(fields[0::2]))[:4096]
    if value := raw.get("lsf_hosts"):
        return list(dict.fromkeys(value.split()))[:4096]
    nodefile = raw.get("pbs_nodefile")
    if nodefile:
        try:
            path = Path(nodefile)
            if path.stat().st_size <= 1024 * 1024:
                return list(dict.fromkeys(path.read_text(encoding="utf-8").split()))[:4096]
        except OSError:
            pass
    return []


def _execution_argv(plan: ExecutionPlan) -> list[str]:
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
    argv.extend([container.image, *plan.argv])
    return argv


def _runtime_evidence(
    execution_id: str,
    experiment_id: str | None,
    capture: dict[str, Any],
    allocation: AllocationObservation,
    preflight: dict[str, Any],
    plan: ExecutionPlan,
    *,
    system: SystemProvenance | None = None,
) -> RuntimeEvidence:
    procfs = capture.get("collector") == "linux_procfs"
    process = RuntimeEvidenceGroup(
        coverage="observed" if procfs else "unavailable",
        source="linux_procfs" if procfs else "runtime_collector",
        metrics={
            key: capture[key]
            for key in (
                "root_pid", "observed_processes", "peak_concurrent_processes",
                "samples", "sample_interval_seconds",
                "stdout_capture_bytes", "stderr_capture_bytes",
                "stdout_truncated", "stderr_truncated", "capture_limit_bytes",
            )
            if key in capture
        } | {
            "resolved_executable": preflight.get("resolved_executable"),
            "exact_argv": _execution_argv(plan),
            "scientific_argv": plan.argv,
            "working_directory": plan.working_directory,
            "started_at": capture.get("started_at"),
            "ended_at": capture.get("ended_at"),
            "duration_seconds": capture.get("duration_seconds"),
            "exit_code": capture.get("exit_code"),
            "signal": capture.get("termination_signal"),
        },
        diagnostic=capture.get("diagnostic"),
    )
    sampled = "partially_observed" if procfs else "unavailable"
    sampled_source = "linux_procfs" if procfs else "runtime_collector"
    cpu = RuntimeEvidenceGroup(
        coverage=sampled, source=sampled_source,
        metrics=(
            {"cpu_seconds": capture["cpu_seconds"]} if "cpu_seconds" in capture else {}
        ) | {"observed_cpu_allocation": allocation.resources.get("cpus")},
        diagnostic=(
            "sampled execution process tree on this allocation node"
            if procfs else capture.get("diagnostic")
        ),
    )
    memory = RuntimeEvidenceGroup(
        coverage=sampled, source=sampled_source,
        metrics={"peak_rss_bytes": capture["peak_rss_bytes"]} if "peak_rss_bytes" in capture else {},
        diagnostic=(
            "sampled RSS for the execution process tree on this allocation node"
            if procfs else capture.get("diagnostic")
        ),
    )
    io = RuntimeEvidenceGroup(
        coverage=sampled, source=sampled_source,
        metrics={key: capture[key] for key in ("read_bytes", "write_bytes") if key in capture},
        diagnostic=(
            "sampled process I/O counters; filesystem and remote-node I/O are not implied"
            if procfs else capture.get("diagnostic")
        ),
    )
    allocation_coverage = "observed" if allocation.resources else "unknown"
    if plan.backend != "direct" and not allocation.hosts:
        allocation_coverage = "partially_observed" if allocation.resources else "unknown"
    requested_shape = (
        plan.resource_shape.to_dict()
        if plan.resource_shape is not None
        else {
            "nodes": plan.requested_resources.nodes,
            "total_cpus": plan.requested_resources.cpus,
            "mpi_ranks": plan.requested_resources.mpi_ranks,
            "gpus": plan.requested_resources.gpus,
            "memory_bytes": plan.requested_resources.memory_bytes,
            "walltime_seconds": plan.requested_resources.walltime_seconds,
        }
    )
    discrepancies = _allocation_discrepancies(requested_shape, allocation.resources)
    allocation_group = RuntimeEvidenceGroup(
        coverage=allocation_coverage, source="compute_worker_allowlist",
        metrics={
            "requested_resource_shape": requested_shape,
            "compute_worker_observed": allocation.resources,
            "scheduler_environment_facts": allocation.evidence.get("environment", {}),
            "hosts": allocation.hosts,
            "discrepancies": discrepancies,
        },
        diagnostic=(
            "scheduler allocation facts visible to this compute worker; not scheduler accounting"
            if allocation.resources else "no scheduler allocation facts were visible"
        ),
    )
    visible = allocation.evidence.get("environment", {}).get("cuda_visible_devices")
    allocated_gpus = allocation.resources.get("gpus")
    nvidia_metrics = (
        {}
        if system is None or not system.gpu_available
        else {
            "nvidia_devices": system.gpus,
            "nvidia_driver_version": system.nvidia_driver_version,
            "cuda_version": system.cuda_version,
            "cuda_version_source": system.cuda_version_source,
        }
    )
    if visible is not None:
        gpu = RuntimeEvidenceGroup(
            coverage="partially_observed",
            source=(
                "CUDA_VISIBLE_DEVICES+nvidia_smi"
                if nvidia_metrics else "CUDA_VISIBLE_DEVICES"
            ),
            metrics={
                "visible_devices": visible, "allocated_gpus": allocated_gpus,
                **nvidia_metrics,
            },
            diagnostic="device identity was observed; utilization counters were not sampled",
        )
    elif nvidia_metrics:
        gpu = RuntimeEvidenceGroup(
            coverage="partially_observed", source="nvidia_smi",
            metrics=nvidia_metrics,
            diagnostic=(
                "host NVIDIA identity was observed; scheduler allocation and utilization "
                "were not established"
            ),
        )
    elif allocated_gpus == 0:
        gpu = RuntimeEvidenceGroup(
            coverage="unsupported", source="allocation",
            metrics={"allocated_gpus": 0}, diagnostic="no GPU was allocated",
        )
    else:
        gpu = RuntimeEvidenceGroup(
            coverage="unavailable", source="runtime_collector",
            diagnostic="execution-scoped GPU utilization was not available",
        )
    environment = RuntimeEvidenceGroup(
        coverage="observed", source="compute_worker_preflight",
        metrics={
            "platform": platform.system() or "unknown",
            "architecture": platform.machine() or "unknown",
            "python": platform.python_version(),
            "working_directory": plan.working_directory,
            "activation": preflight.get("environment_activation"),
            "container": preflight.get("container"),
        },
    )
    return RuntimeEvidence(
        id=new_ulid(), execution_id=execution_id, experiment_id=experiment_id,
        observed_at=utc_now(), process=process, allocation=allocation_group,
        cpu=cpu, memory=memory, io=io, gpu=gpu, environment=environment,
    )


def _allocation_discrepancies(
    requested: dict[str, Any], observed: dict[str, Any]
) -> list[dict[str, Any]]:
    mapping = {
        "nodes": "nodes", "total_cpus": "cpus", "mpi_ranks": "mpi_ranks",
        "gpus": "gpus", "memory_bytes": "memory_bytes",
    }
    result: list[dict[str, Any]] = []
    for requested_name, observed_name in mapping.items():
        expected = requested.get(requested_name)
        actual = observed.get(observed_name)
        if expected is not None and actual is not None and expected != actual:
            result.append(
                {"resource": observed_name, "requested": expected, "observed": actual}
            )
    return result


def _termination_evidence(
    experiment: Any,
    capture: dict[str, Any],
    verification: Any,
) -> TerminationEvidence:
    signal_number = capture.get("termination_signal")
    launch_error = bool(capture.get("launch_error"))
    if launch_error:
        phase, outcome = "launch", "launch_failed"
    elif signal_number is not None:
        phase, outcome = "running", "terminated_by_signal"
    elif experiment.status == "failed":
        phase, outcome = "running", "running_then_failed"
    elif verification is not None and verification.aggregate_state == "failed":
        phase, outcome = "verification", "verification_failed"
    else:
        phase, outcome = "running", "completed"
    return TerminationEvidence(
        phase=phase, outcome=outcome, source="compute_worker",
        exit_code=experiment.exit_code, signal=signal_number,
        result_evidence="complete",
        telemetry_evidence="observed" if capture.get("collector") == "linux_procfs" else "unavailable",
    )


def _positive_int(value: str) -> int | None:
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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
