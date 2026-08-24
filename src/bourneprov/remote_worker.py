"""One-shot, non-AI login-node worker exposing only typed Bourne operations."""

from __future__ import annotations

import getpass
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

from . import __version__
from .bounded_subprocess import BoundedCommandResult, run_bounded_command
from .discovery import discover_site
from .execution_request import ExecutionRequest
from .inventory_storage import InventoryStore
from .remote_transport import (
    MAX_REMOTE_REQUEST_BYTES,
    REMOTE_PROTOCOL,
    REMOTE_PROTOCOL_VERSION,
)
from .workload import utc_now
from .workload_models import ExecutionPlan, WorkloadSpec
from .worker_result import MAX_RESULT_BUNDLE_BYTES

MAX_RESULT_RETURN_BYTES = MAX_RESULT_BUNDLE_BYTES
REMOTE_COMMAND_TIMEOUT = 15.0
_EXECUTION_ID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}\Z")
_SLURM_JOB = re.compile(r"[0-9]+(?:_[0-9]+)?\Z")
_PBS_JOB = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_LSF_JOB = re.compile(r"[0-9]+\Z")
_OPERATIONS = {
    "hello", "discover", "validate_plan", "prepare", "submit",
    "reconcile", "collect", "cancel",
}


class RemoteWorkerError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2 or arguments[0] != "_remote" or arguments[1] not in _OPERATIONS:
        print("bourne remote worker accepts only typed internal operations", file=sys.stderr)
        return 2
    operation = arguments[1]
    try:
        raw = sys.stdin.buffer.read(MAX_REMOTE_REQUEST_BYTES + 1)
        if len(raw) > MAX_REMOTE_REQUEST_BYTES:
            raise RemoteWorkerError("remote request exceeds the size limit")
        envelope = json.loads(raw.decode("utf-8"))
        payload = _request_payload(envelope, operation)
        status, data = _dispatch(operation, payload)
        response = {
            "protocol": REMOTE_PROTOCOL,
            "protocol_version": REMOTE_PROTOCOL_VERSION,
            "operation": operation,
            "status": status,
            "data": data,
        }
        print(json.dumps(response, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(f"bourne remote worker failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 70


def _dispatch(operation: str, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    functions: dict[str, Callable[[dict[str, Any]], tuple[str, dict[str, Any]]]] = {
        "hello": _hello,
        "discover": _discover,
        "validate_plan": _validate_plan,
        "prepare": _prepare,
        "submit": _submit,
        "reconcile": _reconcile,
        "collect": _reconcile,
        "cancel": _cancel,
    }
    return functions[operation](payload)


def _request_payload(value: Any, operation: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RemoteWorkerError("remote request must be an object")
    if value.get("protocol") != REMOTE_PROTOCOL:
        raise RemoteWorkerError("remote request protocol is invalid")
    if value.get("protocol_version") != REMOTE_PROTOCOL_VERSION:
        raise RemoteWorkerError("remote protocol version is incompatible")
    if value.get("operation") != operation or not isinstance(value.get("payload"), dict):
        raise RemoteWorkerError("remote operation envelope does not match invocation")
    return dict(value["payload"])


def _hello(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    path = Path(sys.argv[0])
    digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    expected_version = payload.get("expected_version")
    expected_digest = payload.get("expected_sha256")
    compatible = (
        (expected_version is None or expected_version == __version__)
        and (expected_digest is None or expected_digest == digest)
    )
    return (
        "ok" if compatible else "unavailable",
        {
            "bourne_version": __version__, "worker_sha256": digest,
            "compatible": compatible, "role": "remote_worker",
            "ai": False, "mcp": False, "daemon": False,
        },
    )


def _discover(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    site_label = payload.get("site_label")
    working_directory = payload.get("working_directory")
    if not isinstance(site_label, str) or not site_label:
        raise RemoteWorkerError("remote discovery requires a site label")
    cwd = Path.cwd() if working_directory is None else Path(working_directory)
    if not cwd.is_dir():
        raise RemoteWorkerError("configured remote working directory is unavailable")
    with tempfile.TemporaryDirectory(prefix="bourne-remote-discovery-") as raw:
        store = InventoryStore(Path(raw) / "inventory.sqlite3")
        snapshot = discover_site(
            store, cwd=cwd, site_label=site_label,
            observation_scope="remote_ssh_login_access_node",
        )
    value = snapshot.to_dict()
    value["snapshot"]["metadata"].update(
        {
            "control_transport": "ssh",
            "observation_context": "login_access_node",
            "compute_allocation_facts": "not_observed",
        }
    )
    return "ok", {"inventory": value}


def _validate_plan(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    plan, workload, request = _models(payload)
    problems: list[str] = []
    if plan.backend not in {"slurm", "pbs", "lsf"}:
        problems.append("remote execution requires Slurm, PBS, or LSF")
    if plan.argv != workload.argv or plan.working_directory != workload.working_directory:
        problems.append("plan does not match its immutable workload")
    if request is not None and (
        request.argv != workload.argv
        or request.resolved_working_directory != workload.working_directory
    ):
        problems.append("request does not match its compiled workload")
    if not Path(plan.working_directory).is_dir():
        problems.append("remote working directory is unavailable")
    environment = plan.environment
    if environment is not None and environment.activation.prefix is not None:
        if not Path(environment.activation.prefix).is_dir():
            problems.append("planned existing environment is unavailable")
    return (
        "ok" if not problems else "failed",
        {
            "valid": not problems, "problems": problems,
            "observation_context": "login_access_node",
            "compute_preflight_required": True,
        },
    )


def _prepare(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    execution_id = _execution(payload)
    staging_root = _absolute_path(payload.get("staging_root"), "staging root")
    family = payload.get("scheduler_family")
    if family not in {"slurm", "pbs", "lsf"}:
        raise RemoteWorkerError("remote preparation requires Slurm, PBS, or LSF")
    expected = payload.get("expected_files")
    if not isinstance(expected, dict) or set(expected) != {"worker.pyz", "plan.json", "job.sh"}:
        raise RemoteWorkerError("remote preparation requires exact staged-file digests")
    if not all(re.fullmatch(r"[0-9a-f]{64}", item) for item in expected.values()):
        raise RemoteWorkerError("staged-file digest is invalid")
    variant = payload.get("variant")
    if variant is not None:
        if not isinstance(variant, dict) or set(variant) != {"remote_path", "sha256"}:
            raise RemoteWorkerError("remote variant staging request is invalid")
        variant_path = _absolute_path(variant["remote_path"], "variant path")
        allowed_root = staging_root / ".bourne" / "variants"
        try:
            variant_path.relative_to(allowed_root)
        except ValueError as exc:
            raise RemoteWorkerError("variant path is outside Bourne staging") from exc
        if not re.fullmatch(r"[0-9a-f]{64}", variant["sha256"]):
            raise RemoteWorkerError("variant digest is invalid")
        variant_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    staging = staging_root / ".bourne" / "executions" / execution_id
    state_path = staging / "submission-state.json"
    if state_path.exists():
        state = _read_state(state_path, execution_id)
        if (
            state.get("scheduler_family") != family
            or state.get("expected_files") != expected
            or state.get("expected_variant") != variant
        ):
            raise RemoteWorkerError(
                "retry content conflicts with immutable remote execution state"
            )
        return "ok", _public_state(state)
    staging.mkdir(mode=0o700, parents=True, exist_ok=False)
    state = {
        "execution_id": execution_id,
        "state": "prepared",
        "scheduler_family": family,
        "submitting_identity": getpass.getuser(),
        "staging_directory": str(staging),
        "expected_files": expected,
        "expected_variant": variant,
        "created_at": utc_now(),
        "updated_at": utc_now(),
    }
    _write_state(state_path, state)
    return "ok", _public_state(state)


def _submit(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    execution_id = _execution(payload)
    state_path, state = _state(payload, execution_id)
    if state["state"] == "submitted":
        return "ok", _public_state(state)
    if state["state"] in {"submitting", "ambiguous"}:
        state["state"] = "ambiguous"
        state["updated_at"] = utc_now()
        _write_state(state_path, state)
        return "ambiguous", _public_state(state)
    if state["state"] != "prepared":
        return "failed", _public_state(state)
    staging = Path(state["staging_directory"])
    for name, digest in state["expected_files"].items():
        path = staging / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            return "failed", {**_public_state(state), "diagnostic": f"{name} is absent or changed"}
    variant = state.get("expected_variant")
    if variant is not None:
        variant_path = Path(variant["remote_path"])
        if (
            not variant_path.is_file()
            or hashlib.sha256(variant_path.read_bytes()).hexdigest()
            != variant["sha256"]
        ):
            return "failed", {
                **_public_state(state),
                "diagnostic": "workload variant is absent or changed",
            }
    family = state["scheduler_family"]
    command = {"slurm": "sbatch", "pbs": "qsub", "lsf": "bsub"}[family]
    executable = shutil.which(command)
    if executable is None:
        state.update(state="submission_failed", updated_at=utc_now(), diagnostic=f"{command} unavailable")
        _write_state(state_path, state)
        return "unavailable", _public_state(state)
    state.update(state="submitting", updated_at=utc_now())
    _write_state(state_path, state)
    if family == "slurm":
        argv = [executable, "--parsable", str(staging / "job.sh")]
        input_bytes = None
    elif family == "pbs":
        argv = [executable, str(staging / "job.sh")]
        input_bytes = None
    else:
        argv = [executable]
        input_bytes = (staging / "job.sh").read_bytes()
    result = run_bounded_command(
        argv, timeout=REMOTE_COMMAND_TIMEOUT, input_bytes=input_bytes
    )
    if result.timed_out:
        state.update(state="ambiguous", updated_at=utc_now(), diagnostic="scheduler submission timed out")
        _write_state(state_path, state)
        return "ambiguous", _public_state(state)
    if result.returncode != 0:
        state.update(state="submission_failed", updated_at=utc_now(), diagnostic=result.stderr[:4096])
        _write_state(state_path, state)
        return "failed", _public_state(state)
    job_id = _job_id(family, result.stdout)
    if job_id is None:
        state.update(state="ambiguous", updated_at=utc_now(), diagnostic="scheduler returned no valid job identity")
        _write_state(state_path, state)
        return "ambiguous", _public_state(state)
    state.update(state="submitted", job_id=job_id, submitted_at=utc_now(), updated_at=utc_now())
    _write_state(state_path, state)
    return "ok", _public_state(state)


def _reconcile(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    execution_id = _execution(payload)
    state_path, state = _state(payload, execution_id)
    if state["state"] in {"submitting", "ambiguous"} and not state.get("job_id"):
        state["state"] = "ambiguous"
        state["updated_at"] = utc_now()
        _write_state(state_path, state)
        return "ambiguous", _public_state(state)
    if state.get("job_id") is None:
        return "unknown", _public_state(state)
    observation = _scheduler_observation(state)
    staging = Path(state["staging_directory"])
    result_path = staging / "result.json"
    data = {**_public_state(state), "scheduler": observation}
    if result_path.is_file():
        size = result_path.stat().st_size
        if size > MAX_RESULT_RETURN_BYTES:
            return "unavailable", {
                **data, "result": None, "result_state": "invalid",
                "diagnostic": "result bundle exceeds remote return bound",
            }
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return "failed", {
                **data, "result": None, "result_state": "invalid",
                "diagnostic": "result bundle is invalid",
            }
        return "ok", {**data, "result": result, "result_state": "available"}
    return (
        "unknown" if not observation["observable"] else "ok",
        {**data, "result": None, "result_state": "absent"},
    )


def _cancel(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    execution_id = _execution(payload)
    state_path, state = _state(payload, execution_id)
    job_id = state.get("job_id")
    if job_id is None:
        return "unknown", _public_state(state)
    family = state["scheduler_family"]
    command = {"slurm": "scancel", "pbs": "qdel", "lsf": "bkill"}[family]
    executable = shutil.which(command)
    if executable is None:
        return "unavailable", {**_public_state(state), "diagnostic": f"{command} unavailable"}
    result = run_bounded_command([executable, job_id], timeout=REMOTE_COMMAND_TIMEOUT)
    if result.returncode != 0 or result.timed_out:
        return "failed", {**_public_state(state), "diagnostic": result.stderr[:4096]}
    state.update(state="cancelled", updated_at=utc_now())
    _write_state(state_path, state)
    return "ok", _public_state(state)


def _scheduler_observation(state: dict[str, Any]) -> dict[str, Any]:
    family = state["scheduler_family"]
    job_id = state["job_id"]
    identity = state["submitting_identity"]
    command = {"slurm": "squeue", "pbs": "qstat", "lsf": "bjobs"}[family]
    executable = shutil.which(command)
    if executable is None:
        return {"state": "unobservable", "observable": False, "source": "unavailable"}
    argv = (
        [executable, "--noheader", "--jobs", job_id, "--user", identity, "--format=%T"]
        if family == "slurm"
        else [executable, "-f", job_id]
        if family == "pbs"
        else [executable, "-noheader", "-u", identity, "-o", "jobid stat", job_id]
    )
    result = run_bounded_command(argv, timeout=REMOTE_COMMAND_TIMEOUT)
    if family == "lsf":
        active = _lsf_observation(result, job_id, "active")
        if active["observable"]:
            return active
        if result.timed_out or (
            result.returncode != 0 and not _lsf_job_is_unobservable(result)
        ):
            return active
        if result.returncode == 0 and result.stdout.strip():
            return active
        historical = run_bounded_command(
            [
                executable, "-a", "-noheader", "-u", identity,
                "-o", "jobid stat", job_id,
            ],
            timeout=REMOTE_COMMAND_TIMEOUT,
        )
        return _lsf_observation(historical, job_id, "historical_accounting")
    if result.timed_out or result.returncode != 0:
        return {"state": "unobservable", "observable": False, "source": "active", "diagnostic": result.stderr[:4096]}
    if family == "slurm":
        lines = result.stdout.strip().splitlines()
        return {
            "state": "unobservable" if not lines else lines[0].strip().lower(),
            "observable": bool(lines), "source": "active",
        }
    match = re.search(r"^\s*job_state\s*=\s*([A-Za-z])\s*$", result.stdout, re.MULTILINE)
    if match is None:
        return {"state": "unknown", "observable": False, "source": "active"}
    return {
        "state": {"Q": "queued", "R": "running", "F": "finished", "C": "completed"}.get(match.group(1).upper(), "unknown"),
        "observable": True, "source": "active",
    }


def _models(payload: dict[str, Any]) -> tuple[ExecutionPlan, WorkloadSpec, ExecutionRequest | None]:
    if not isinstance(payload.get("plan"), dict) or not isinstance(payload.get("workload"), dict):
        raise RemoteWorkerError("plan validation requires plan and workload objects")
    plan = ExecutionPlan.from_dict(payload["plan"])
    workload = WorkloadSpec.from_dict(payload["workload"])
    request = None if payload.get("request") is None else ExecutionRequest.from_dict(payload["request"])
    return plan, workload, request


def _execution(payload: dict[str, Any]) -> str:
    value = payload.get("execution_id")
    if not isinstance(value, str) or not _EXECUTION_ID.fullmatch(value):
        raise RemoteWorkerError("canonical execution identity is required")
    return value


def _absolute_path(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value.startswith("/") or any(item in value for item in "\0\r\n"):
        raise RemoteWorkerError(f"{label} must be an absolute path")
    path = Path(value)
    if ".." in path.parts:
        raise RemoteWorkerError(f"{label} cannot traverse parent directories")
    return path


def _state(payload: dict[str, Any], execution_id: str) -> tuple[Path, dict[str, Any]]:
    staging = _absolute_path(payload.get("staging_directory"), "staging directory")
    if (
        staging.name != execution_id
        or staging.parent.name != "executions"
        or staging.parent.parent.name != ".bourne"
    ):
        raise RemoteWorkerError("staging directory is not a Bourne execution path")
    state_path = staging / "submission-state.json"
    return state_path, _read_state(state_path, execution_id)


def _read_state(path: Path, execution_id: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RemoteWorkerError("remote submission state is unavailable") from exc
    if not isinstance(value, dict) or value.get("execution_id") != execution_id:
        raise RemoteWorkerError("remote submission state does not match execution")
    return value


def _write_state(path: Path, value: dict[str, Any]) -> None:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _public_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in state.items()
        if key not in {"expected_files", "expected_variant"}
    }


def _job_id(family: str, stdout: str) -> str | None:
    if family == "lsf":
        matches = re.findall(r"\bJob\s+<([0-9]+)>", stdout)
        return matches[0] if len(matches) == 1 and _LSF_JOB.fullmatch(matches[0]) else None
    value = stdout.strip().split(";", 1)[0] if family == "slurm" else (stdout.strip().split()[0] if stdout.strip() else "")
    pattern = _SLURM_JOB if family == "slurm" else _PBS_JOB
    return value if pattern.fullmatch(value) else None


def _lsf_observation(
    result: BoundedCommandResult, job_id: str, source: str
) -> dict[str, Any]:
    if result.timed_out or result.returncode != 0:
        return {
            "state": "unobservable", "observable": False, "source": source,
            "diagnostic": (result.stderr or "LSF query failed")[:4096],
        }
    rows = [line.split() for line in result.stdout.splitlines() if line.strip()]
    rows = [row for row in rows if len(row) == 2 and row[0] == job_id]
    if len(rows) != 1:
        return {
            "state": "unobservable", "observable": False, "source": source,
            "diagnostic": "no unique exact LSF job record",
        }
    state = {
        "PEND": "pending", "WAIT": "pending", "PROV": "pending",
        "RUN": "running", "PSUSP": "suspended", "USUSP": "suspended",
        "SSUSP": "suspended", "DONE": "completed", "EXIT": "failed",
        "ZOMBI": "failed", "UNKWN": "unknown_terminal",
    }.get(rows[0][1].upper(), "unknown")
    return {"state": state, "observable": True, "source": source}


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
