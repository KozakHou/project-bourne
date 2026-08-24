"""Bounded, read-only providers for local compute-site observations."""

from __future__ import annotations

import getpass
import json
import os
import platform
import re
import shutil
import stat
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .bounded_subprocess import BoundedCommandResult, run_bounded_command
from .collectors.system import collect_system
from .ids import new_ulid
from .inventory_models import (
    Capability,
    CurrentIdentity,
    DiscoveredExecutionContext,
    DiscoveredTarget,
    DiscoveryEvidence,
    ProviderStatus,
    SchedulerResource,
    StorageResource,
)
from .inventory_storage import InventoryStore

COMMAND_TIMEOUT_SECONDS = 5.0
MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024
MAX_PATH_DIRECTORIES = 256
MAX_DIRECTORY_ENTRIES = 50_000
MAX_LOCAL_ENV_CANDIDATES = 256


CommandRunner = Callable[..., BoundedCommandResult]


@dataclass(frozen=True)
class DiscoveryRequest:
    snapshot_id: str
    cwd: Path
    environment: Mapping[str, str]
    store: InventoryStore
    runner: CommandRunner = run_bounded_command
    max_path_directories: int = MAX_PATH_DIRECTORIES
    max_directory_entries: int = MAX_DIRECTORY_ENTRIES


@dataclass
class ProviderOutput:
    status: ProviderStatus = "complete"
    diagnostic: str | None = None
    truncated: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
    identity: CurrentIdentity | None = None
    targets: list[DiscoveredTarget] = field(default_factory=list)
    storage: list[StorageResource] = field(default_factory=list)
    schedulers: list[SchedulerResource] = field(default_factory=list)
    contexts: list[DiscoveredExecutionContext] = field(default_factory=list)
    capabilities: list[Capability] = field(default_factory=list)
    evidence: list[DiscoveryEvidence] = field(default_factory=list)


@dataclass
class DiscoveryState:
    identity: CurrentIdentity | None = None
    targets: list[DiscoveredTarget] = field(default_factory=list)
    storage: list[StorageResource] = field(default_factory=list)
    schedulers: list[SchedulerResource] = field(default_factory=list)
    contexts: list[DiscoveredExecutionContext] = field(default_factory=list)
    capabilities: list[Capability] = field(default_factory=list)
    evidence: list[DiscoveryEvidence] = field(default_factory=list)

    def merge(self, output: ProviderOutput) -> None:
        if output.identity is not None:
            self.identity = output.identity
        self.targets.extend(output.targets)
        self.storage.extend(output.storage)
        self.schedulers.extend(output.schedulers)
        self.contexts.extend(output.contexts)
        self.capabilities.extend(output.capabilities)
        self.evidence.extend(output.evidence)

    def current_context(self) -> DiscoveredExecutionContext:
        return next(item for item in self.contexts if item.context_key == "current")


class DiscoveryProvider(Protocol):
    name: str

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        ...


def _evidence(
    request: DiscoveryRequest,
    *,
    subject_type: str,
    subject_id: str,
    provider: str,
    evidence_type: str,
    observed_now: bool = True,
    historical_only: bool = False,
    details: dict[str, Any] | None = None,
) -> DiscoveryEvidence:
    return DiscoveryEvidence(
        id=new_ulid(), snapshot_id=request.snapshot_id, subject_type=subject_type,
        subject_id=subject_id, provider=provider, evidence_type=evidence_type,
        observed_now=observed_now, historical_only=historical_only,
        details=details or {},
    )


_CLASSIFICATIONS = {
    "interpreter": {"python", "python3", "julia", "r", "matlab"},
    "compiler": {"cc", "gcc", "g++", "clang", "clang++", "gfortran", "nvcc", "rustc"},
    "launcher": {"mpirun", "mpiexec", "srun"},
    "scheduler-client": {"sbatch", "sinfo", "squeue", "qsub", "qstat"},
    "container-runtime": {"docker", "podman"},
    "shell": {"bash", "zsh", "fish", "sh"},
}


def classify_executable(name: str) -> list[str]:
    folded = name.casefold()
    tags = [tag for tag, names in _CLASSIFICATIONS.items() if folded in names]
    if folded.startswith("python3") and "interpreter" not in tags:
        tags.append("interpreter")
    return tags


def _scan_executable_directory(
    request: DiscoveryRequest,
    directory: Path,
    context_id: str,
    provider: str,
    *,
    precedence: int | None = None,
) -> tuple[list[Capability], list[DiscoveryEvidence], bool, str | None]:
    capabilities: list[Capability] = []
    evidence: list[DiscoveryEvidence] = []
    try:
        iterator = os.scandir(directory)
    except FileNotFoundError:
        return capabilities, evidence, False, f"path does not exist: {directory}"
    except OSError as exc:
        return capabilities, evidence, False, f"could not inspect {directory}: {exc}"

    truncated = False
    entries: list[tuple[str, str, bool]] = []
    with iterator:
        for index, entry in enumerate(iterator):
            if index >= request.max_directory_entries:
                truncated = True
                break
            try:
                is_file = entry.is_file(follow_symlinks=True)
            except OSError:
                is_file = False
            entries.append((entry.name, entry.path, is_file))

    for name, raw_path, is_file in sorted(entries, key=lambda item: item[0]):
        if not is_file or not _effective_access(Path(raw_path), os.X_OK):
            continue
        observed = Path(raw_path)
        capability = Capability(
            id=new_ulid(), snapshot_id=request.snapshot_id, context_id=context_id,
            kind="executable", name=name, locator=str(observed),
            observation_state="observed", provider=provider,
            classifications=classify_executable(name),
            metadata={
                "resolved_path": str(observed.resolve(strict=False)),
                **({"path_precedence": precedence} if precedence is not None else {}),
            },
        )
        capabilities.append(capability)
        evidence.append(
            _evidence(
                request, subject_type="capability", subject_id=capability.id,
                provider=provider, evidence_type="filesystem_executable",
                details={"directory": str(directory)},
            )
        )
    diagnostic = f"directory entry limit reached: {directory}" if truncated else None
    return capabilities, evidence, truncated, diagnostic


class IdentityProvider:
    name = "identity"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        uid = os.getuid() if hasattr(os, "getuid") else None
        gid = os.getgid() if hasattr(os, "getgid") else None
        group_ids = os.getgroups() if hasattr(os, "getgroups") else []
        groups: list[dict[str, Any]] = []
        try:
            import grp

            for group_id in sorted(set([*group_ids, *([] if gid is None else [gid])])):
                try:
                    name = grp.getgrgid(group_id).gr_name
                except KeyError:
                    name = None
                groups.append({"id": group_id, "name": name})
        except ImportError:
            groups = [{"id": group_id, "name": None} for group_id in group_ids]
        username: str | None
        try:
            username = getpass.getuser()
        except (OSError, KeyError):
            username = None
        home = request.environment.get("HOME")
        if not home:
            try:
                home = str(Path.home())
            except RuntimeError:
                home = None
        identity = CurrentIdentity(
            id=new_ulid(), snapshot_id=request.snapshot_id, username=username,
            uid=uid, primary_gid=gid, groups=groups,
            home=str(Path(home).expanduser().resolve(strict=False)) if home else None,
        )
        return ProviderOutput(identity=identity)


_SLURM_ALLOCATION_ENV = {
    "SLURM_JOB_ID": "job_id",
    "SLURM_JOB_PARTITION": "partition",
    "SLURM_JOB_NUM_NODES": "nodes",
    "SLURM_CPUS_ON_NODE": "cpus_on_node",
    "SLURM_GPUS": "gpus",
}
_PBS_ALLOCATION_ENV = {
    "PBS_JOBID": "job_id",
    "PBS_QUEUE": "queue",
    "PBS_NP": "processors",
    "PBS_NUM_NODES": "nodes",
}
_LSF_ALLOCATION_ENV = {
    "LSB_JOBID": "job_id",
    "LSB_QUEUE": "queue",
    "LSB_DJOB_NUMPROC": "processors",
    "LSB_HOSTS": "hosts",
    "LSB_MCPU_HOSTS": "host_slots",
    "LSB_GPU_REQ": "gpu_request",
}


def _allocation(environment: Mapping[str, str]) -> dict[str, Any]:
    if environment.get("SLURM_JOB_ID"):
        return {
            "scheduler": "slurm",
            **{
                field: value
                for variable, field in _SLURM_ALLOCATION_ENV.items()
                if (value := environment.get(variable))
            },
        }
    if environment.get("PBS_JOBID"):
        return {
            "scheduler": "pbs",
            **{
                field: value
                for variable, field in _PBS_ALLOCATION_ENV.items()
                if (value := environment.get(variable))
            },
        }
    if environment.get("LSB_JOBID"):
        return {
            "scheduler": "lsf",
            **{
                field: value
                for variable, field in _LSF_ALLOCATION_ENV.items()
                if (value := environment.get(variable))
            },
        }
    return {}


class CurrentTargetProvider:
    name = "current_target"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        try:
            system = collect_system()
            system_data = system.__dict__
            diagnostic = None
        except Exception as exc:
            system_data = {
                "operating_system": platform.system() or "unknown",
                "os_version": platform.version() or "unknown",
                "architecture": platform.machine() or "unknown",
                "hostname": platform.node() or "unknown",
                "cpu": None,
                "gpus": [],
            }
            diagnostic = f"system collector failed: {exc}"
        allocation = _allocation(request.environment)
        hostname = str(system_data["hostname"])
        target = DiscoveredTarget(
            id=new_ulid(), snapshot_id=request.snapshot_id, parent_target_id=None,
            kind="host",
            role="access_target", name=hostname,
            locator=f"local://{hostname}",
            state=("allocated_compute_environment" if allocation else "observed"),
            visible=True, authorization="observed-authorized", provider=self.name,
            metadata={
                "operating_system": system_data["operating_system"],
                "os_version": system_data["os_version"],
                "architecture": system_data["architecture"],
                "working_directory": str(request.cwd),
                "ssh_session": any(
                    request.environment.get(name)
                    for name in ("SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY")
                ),
                "node_role": (
                    "allocated_compute_environment" if allocation else "unknown"
                ),
                "allocation": allocation,
                "system": system_data,
            },
        )
        return ProviderOutput(
            status="partial" if diagnostic else "complete",
            diagnostic=diagnostic,
            targets=[target],
            evidence=[
                _evidence(
                    request, subject_type="target", subject_id=target.id,
                    provider=self.name, evidence_type="current_process_host",
                )
            ],
        )


class CurrentEnvironmentProvider:
    name = "current_environment"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        target = next(
            (item for item in state.targets if item.role == "access_target"), None
        )
        hostname = platform.node() or "unknown"
        context = DiscoveredExecutionContext(
            id=new_ulid(), snapshot_id=request.snapshot_id,
            target_id=None if target is None else target.id, context_key="current",
            kind="system", name="current environment",
            locator=target.locator if target is not None else f"local://{hostname}",
            state="active", provider=self.name,
            metadata={"recorder_executable": str(Path(sys.executable).resolve(strict=False))},
        )
        return ProviderOutput(
            contexts=[context],
            evidence=[
                _evidence(
                    request, subject_type="execution_context", subject_id=context.id,
                    provider=self.name, evidence_type="current_process_environment",
                )
            ],
        )


def _linux_mounts() -> list[tuple[str, str, bool]]:
    """Read only bounded mount metadata for mapping already-known paths."""

    try:
        with Path("/proc/self/mountinfo").open("rb") as stream:
            raw = stream.read(MAX_COMMAND_OUTPUT_BYTES + 1)
    except OSError:
        return []
    if len(raw) > MAX_COMMAND_OUTPUT_BYTES:
        return []
    mounts: list[tuple[str, str, bool]] = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        fields = left.split()
        right_fields = right.split()
        if len(fields) < 6 or len(right_fields) < 1:
            continue
        mount_point = fields[4].replace("\\040", " ")
        options = set(fields[5].split(","))
        mounts.append((mount_point, right_fields[0], "ro" in options))
    return mounts


def _mount_for(path: Path, mounts: list[tuple[str, str, bool]]) -> tuple[str, str, bool] | None:
    value = str(path)
    candidates = [
        item
        for item in mounts
        if value == item[0] or value.startswith(item[0].rstrip(os.sep) + os.sep)
    ]
    return max(candidates, key=lambda item: len(item[0])) if candidates else None


def _effective_access(path: Path, mode: int) -> bool:
    if os.access in getattr(os, "supports_effective_ids", set()):
        return os.access(path, mode, effective_ids=True)
    return os.access(path, mode)


class StorageProvider:
    name = "storage"
    _PATH_HINTS = {
        "HOME": "home",
        "SCRATCH": "scratch",
        "WORK": "work",
        "PROJECT": "project",
        "PROJECT_DIR": "project",
        "TMPDIR": "temporary",
        "TEMP": "temporary",
        "TMP": "temporary",
    }

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        target = next((item for item in state.targets if item.role == "access_target"), None)
        paths: dict[str, set[str]] = {str(request.cwd.resolve(strict=False)): {"cwd"}}
        if state.identity is not None and state.identity.home:
            paths.setdefault(state.identity.home, set()).add("home")
        for variable, hint in self._PATH_HINTS.items():
            raw = request.environment.get(variable)
            if raw:
                normalized = str(Path(raw).expanduser().resolve(strict=False))
                paths.setdefault(normalized, set()).add(hint)
        mounts = _linux_mounts()
        resources: list[StorageResource] = []
        evidence: list[DiscoveryEvidence] = []
        diagnostics: list[str] = []
        for raw_path, hints in sorted(paths.items()):
            path = Path(raw_path)
            try:
                info = path.stat()
                exists: bool | None = True
                is_directory = stat.S_ISDIR(info.st_mode)
                readable = _effective_access(path, os.R_OK)
                writable = _effective_access(path, os.W_OK)
                searchable = _effective_access(path, os.X_OK) if is_directory else None
            except FileNotFoundError:
                exists = False
                readable = writable = searchable = None
            except OSError as exc:
                exists = readable = writable = searchable = None
                diagnostics.append(f"could not inspect {path}: {exc}")
            mount = _mount_for(path, mounts)
            resource = StorageResource(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                target_id=None if target is None else target.id, path=raw_path,
                role_hints=sorted(hints), exists=exists, readable=readable,
                writable=writable, searchable=searchable,
                mount_point=None if mount is None else mount[0],
                filesystem_type=None if mount is None else mount[1],
                mount_read_only=None if mount is None else mount[2],
                provider=self.name,
                metadata={"policy": "unknown"},
            )
            resources.append(resource)
            evidence.append(
                _evidence(
                    request, subject_type="storage", subject_id=resource.id,
                    provider=self.name, evidence_type="allowlisted_current_path",
                    details={"role_hints": sorted(hints)},
                )
            )
        return ProviderOutput(
            status="partial" if diagnostics else "complete",
            diagnostic="; ".join(diagnostics) or None,
            storage=resources,
            evidence=evidence,
            metadata={"recursive_scan": False, "path_count": len(resources)},
        )


class PathExecutableProvider:
    name = "path"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        context = state.current_context()
        raw_entries = request.environment.get("PATH", "").split(os.pathsep)
        considered = raw_entries[: request.max_path_directories]
        truncated = len(raw_entries) > len(considered)
        directories: list[tuple[int, Path]] = []
        seen: set[str] = set()
        for precedence, raw in enumerate(considered):
            directory = Path(raw or ".")
            key = os.path.abspath(directory)
            if key not in seen:
                seen.add(key)
                directories.append((precedence, directory))
        capabilities: list[Capability] = []
        evidence: list[DiscoveryEvidence] = []
        diagnostics: list[str] = []
        missing_entries = 0
        for precedence, directory in directories:
            found, found_evidence, hit_limit, diagnostic = _scan_executable_directory(
                request, directory, context.id, self.name, precedence=precedence
            )
            capabilities.extend(found)
            evidence.extend(found_evidence)
            truncated = truncated or hit_limit
            if diagnostic and diagnostic.startswith("path does not exist"):
                missing_entries += 1
            elif diagnostic:
                diagnostics.append(diagnostic)
        inaccessible = any("could not inspect" in item for item in diagnostics)
        status: ProviderStatus = "partial" if truncated or inaccessible else "complete"
        return ProviderOutput(
            status=status, diagnostic="; ".join(diagnostics) or None,
            truncated=truncated, capabilities=capabilities, evidence=evidence,
            metadata={
                "path_entries_considered": len(considered),
                "unique_directories_inspected": len(directories),
                "nonexistent_entries": missing_entries,
                "recursive_scan": False,
                "executed_discovered_programs": False,
            },
        )


class SystemCapabilityProvider:
    name = "system_capabilities"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        context = state.current_context()
        target = next(item for item in state.targets if item.role == "access_target")
        system = target.metadata.get("system", {})
        observations: list[tuple[str, str, str | None, dict[str, Any]]] = [
            ("operating-system", str(system.get("operating_system", "unknown")), None, {}),
            ("architecture", str(system.get("architecture", "unknown")), None, {}),
        ]
        if system.get("cpu"):
            observations.append(("cpu", str(system["cpu"]), None, {}))
        for gpu in system.get("gpus", []):
            observations.append(
                ("accelerator", str(gpu.get("name", "NVIDIA GPU")), gpu.get("uuid"), dict(gpu))
            )
        if system.get("nvidia_driver_version"):
            observations.append(
                ("driver", "nvidia", None, {"version": system["nvidia_driver_version"]})
            )
        if system.get("cuda_version"):
            observations.append(
                (
                    "driver-supported-cuda", str(system["cuda_version"]), None,
                    {"semantics": system.get("cuda_version_source")},
                )
            )
        capabilities: list[Capability] = []
        evidence: list[DiscoveryEvidence] = []
        for kind, name, locator, metadata in observations:
            capability = Capability(
                id=new_ulid(), snapshot_id=request.snapshot_id, context_id=context.id,
                kind=kind, name=name, locator=locator, observation_state="observed",
                provider=self.name, metadata=metadata,
            )
            capabilities.append(capability)
            evidence.append(
                _evidence(
                    request, subject_type="capability", subject_id=capability.id,
                    provider=self.name, evidence_type="existing_system_collector",
                )
            )
        return ProviderOutput(capabilities=capabilities, evidence=evidence)


def _command_failure(result: BoundedCommandResult, label: str) -> ProviderOutput | None:
    if result.timed_out:
        return ProviderOutput(
            status="timeout", diagnostic=f"{label} timed out", truncated=result.truncated
        )
    if result.returncode != 0:
        raw_detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        detail = re.sub(
            r"(?i)(token|password|secret|credential|authorization)(\s*[:=]\s*)\S+",
            r"\1\2<redacted>",
            raw_detail[:1000],
        )
        detail = re.sub(r"://[^/@\s]+@", "://<redacted>@", detail)
        return ProviderOutput(
            status="error", diagnostic=f"{label} failed: {detail}",
            truncated=result.truncated,
        )
    return None


class CondaProvider:
    name = "conda"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        executable = shutil.which("conda", path=request.environment.get("PATH"))
        if executable is None:
            return ProviderOutput(status="unavailable", diagnostic="conda executable not found")
        try:
            result = request.runner(
                [executable, "env", "list", "--json"],
                timeout=COMMAND_TIMEOUT_SECONDS,
                max_output_bytes=MAX_COMMAND_OUTPUT_BYTES,
            )
        except OSError as exc:
            return ProviderOutput(status="error", diagnostic=f"conda failed: {exc}")
        if failure := _command_failure(result, "conda environment listing"):
            return failure
        try:
            payload = json.loads(result.stdout)
            prefixes = payload["envs"]
            if not isinstance(prefixes, list) or not all(isinstance(item, str) for item in prefixes):
                raise ValueError("envs must be a list of paths")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            return ProviderOutput(status="error", diagnostic=f"malformed conda response: {exc}")

        active = request.environment.get("CONDA_PREFIX")
        contexts: list[DiscoveredExecutionContext] = []
        capabilities: list[Capability] = []
        evidence: list[DiscoveryEvidence] = []
        diagnostics: list[str] = []
        truncated = result.truncated
        for prefix_value in dict.fromkeys(prefixes):
            prefix = Path(prefix_value).expanduser().resolve(strict=False)
            is_active = bool(active and prefix == Path(active).expanduser().resolve(strict=False))
            name = (
                request.environment.get("CONDA_DEFAULT_ENV")
                if is_active and request.environment.get("CONDA_DEFAULT_ENV")
                else prefix.name
            )
            context = DiscoveredExecutionContext(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                target_id=state.current_context().target_id,
                context_key=f"conda:{prefix}", kind="conda",
                name=str(name), locator=str(prefix),
                state="active" if is_active else "observed", provider=self.name,
                metadata={"active": is_active},
            )
            contexts.append(context)
            evidence.append(
                _evidence(
                    request, subject_type="execution_context", subject_id=context.id,
                    provider=self.name, evidence_type="conda_env_list",
                )
            )
            executable_directory = prefix / ("Scripts" if os.name == "nt" else "bin")
            found, found_evidence, hit_limit, diagnostic = _scan_executable_directory(
                request, executable_directory, context.id, self.name
            )
            capabilities.extend(found)
            evidence.extend(found_evidence)
            truncated = truncated or hit_limit
            if diagnostic and "does not exist" not in diagnostic:
                diagnostics.append(diagnostic)
        return ProviderOutput(
            status="partial" if truncated or diagnostics else "complete",
            diagnostic="; ".join(diagnostics) or None, truncated=truncated,
            contexts=contexts, capabilities=capabilities, evidence=evidence,
            metadata={"command": [executable, "env", "list", "--json"]},
        )


class VirtualenvProvider:
    name = "virtualenv"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        candidates: dict[str, bool] = {}
        active = request.environment.get("VIRTUAL_ENV")
        if active:
            candidates[str(Path(active).expanduser().resolve(strict=False))] = True
        for name in (".venv", "venv", "env"):
            candidate = (request.cwd / name).resolve(strict=False)
            if candidate != request.cwd:
                candidates.setdefault(str(candidate), False)

        home = request.environment.get("HOME")
        if not home or request.cwd.resolve(strict=False) != Path(home).expanduser().resolve(strict=False):
            try:
                with os.scandir(request.cwd) as entries:
                    for index, entry in enumerate(entries):
                        if index >= MAX_LOCAL_ENV_CANDIDATES:
                            break
                        try:
                            if entry.is_dir(follow_symlinks=False) and Path(entry.path, "pyvenv.cfg").is_file():
                                candidates.setdefault(
                                    str(Path(entry.path).resolve(strict=False)), False
                                )
                        except OSError:
                            continue
            except OSError:
                pass

        contexts: list[DiscoveredExecutionContext] = []
        capabilities: list[Capability] = []
        evidence: list[DiscoveryEvidence] = []
        diagnostics: list[str] = []
        truncated = False
        for raw_prefix, is_active in sorted(candidates.items()):
            prefix = Path(raw_prefix)
            valid = (prefix / "pyvenv.cfg").is_file()
            if not valid and not is_active:
                continue
            if not valid:
                diagnostics.append(f"active virtual environment is incomplete: {prefix}")
            context = DiscoveredExecutionContext(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                target_id=state.current_context().target_id,
                context_key=f"virtualenv:{prefix}", kind="virtualenv",
                name=prefix.name, locator=str(prefix),
                state=("active" if is_active else ("observed" if valid else "unknown")),
                provider=self.name, metadata={"active": is_active, "pyvenv_cfg": valid},
            )
            contexts.append(context)
            evidence.append(
                _evidence(
                    request, subject_type="execution_context", subject_id=context.id,
                    provider=self.name,
                    evidence_type=("active_environment" if is_active else "project_local_marker"),
                )
            )
            executable_directory = prefix / ("Scripts" if os.name == "nt" else "bin")
            found, found_evidence, hit_limit, diagnostic = _scan_executable_directory(
                request, executable_directory, context.id, self.name
            )
            capabilities.extend(found)
            evidence.extend(found_evidence)
            truncated = truncated or hit_limit
            if diagnostic and "does not exist" not in diagnostic:
                diagnostics.append(diagnostic)
        return ProviderOutput(
            status="partial" if diagnostics or truncated else "complete",
            diagnostic="; ".join(diagnostics) or None, truncated=truncated,
            contexts=contexts, capabilities=capabilities, evidence=evidence,
            metadata={"recursive_scan": False, "home_crawl": False},
        )


class ContainerProvider:
    def __init__(self, runtime: str):
        if runtime not in {"docker", "podman"}:
            raise ValueError(f"unsupported container runtime: {runtime}")
        self.runtime = runtime
        self.name = runtime

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        executable = shutil.which(self.runtime, path=request.environment.get("PATH"))
        if executable is None:
            return ProviderOutput(
                status="unavailable", diagnostic=f"{self.runtime} executable not found"
            )
        argv = (
            [executable, "ps", "--all", "--no-trunc", "--format", "{{json .}}"]
            if self.runtime == "docker"
            else [executable, "ps", "--all", "--format", "json"]
        )
        try:
            result = request.runner(
                argv, timeout=COMMAND_TIMEOUT_SECONDS,
                max_output_bytes=MAX_COMMAND_OUTPUT_BYTES,
            )
        except OSError as exc:
            return ProviderOutput(status="error", diagnostic=f"{self.runtime} failed: {exc}")
        if failure := _command_failure(result, f"{self.runtime} container listing"):
            return failure
        try:
            if self.runtime == "docker":
                records = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            else:
                parsed = json.loads(result.stdout or "[]")
                records = parsed if isinstance(parsed, list) else [parsed]
            if not all(isinstance(item, dict) for item in records):
                raise ValueError("container listing must contain objects")
        except (json.JSONDecodeError, ValueError) as exc:
            return ProviderOutput(status="error", diagnostic=f"malformed {self.runtime} response: {exc}")

        contexts: list[DiscoveredExecutionContext] = []
        evidence: list[DiscoveryEvidence] = []
        for record in records:
            container_id = str(record.get("ID") or record.get("Id") or record.get("id") or "")
            if not container_id:
                continue
            name = str(
                record.get("Names") or record.get("Name") or record.get("names")
                or record.get("name") or container_id[:12]
            )
            image = record.get("Image") or record.get("image")
            raw_state = str(record.get("State") or record.get("state") or record.get("Status") or "unknown")
            state_name = "running" if raw_state.casefold().startswith("running") or raw_state.casefold().startswith("up") else "stopped"
            context = DiscoveredExecutionContext(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                target_id=state.current_context().target_id,
                context_key=f"{self.runtime}:{container_id}", kind="container",
                name=name, locator=f"{self.runtime}://{container_id}", state=state_name,
                provider=self.name,
                metadata={"runtime": self.runtime, "container_id": container_id,
                          "image": image, "internal_capabilities": "unprobed"},
            )
            contexts.append(context)
            evidence.append(
                _evidence(
                    request, subject_type="execution_context", subject_id=context.id,
                    provider=self.name, evidence_type="container_metadata_listing",
                )
            )
        return ProviderOutput(
            status="partial" if result.truncated else "complete",
            truncated=result.truncated, contexts=contexts, evidence=evidence,
            metadata={"command": argv, "container_exec": False, "environment_inspected": False},
        )


class ModuleProvider:
    name = "modules"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        loaded_raw = request.environment.get("LOADEDMODULES")
        module_path = request.environment.get("MODULEPATH")
        if not loaded_raw and not module_path:
            return ProviderOutput(metadata={"module_commands_executed": False})
        loaded = [item for item in (loaded_raw or "").split(":") if item]
        context = DiscoveredExecutionContext(
            id=new_ulid(), snapshot_id=request.snapshot_id,
            target_id=state.current_context().target_id,
            context_key="modules:current", kind="modules", name="loaded modules",
            locator=None, state="active", provider=self.name,
            metadata={
                "loaded_modules": loaded,
                "module_path_entries": [item for item in (module_path or "").split(os.pathsep) if item],
                "available_inventory": "not_probed",
            },
        )
        capabilities: list[Capability] = []
        evidence = [
            _evidence(
                request, subject_type="execution_context", subject_id=context.id,
                provider=self.name, evidence_type="environment_metadata",
            )
        ]
        for module in loaded:
            capability = Capability(
                id=new_ulid(), snapshot_id=request.snapshot_id, context_id=context.id,
                kind="module", name=module, locator=None,
                observation_state="metadata-observed", provider=self.name,
            )
            capabilities.append(capability)
            evidence.append(
                _evidence(
                    request, subject_type="capability", subject_id=capability.id,
                    provider=self.name, evidence_type="loaded_module_metadata",
                )
            )
        return ProviderOutput(
            status="partial", diagnostic="full available-module inventory was not probed",
            contexts=[context], capabilities=capabilities, evidence=evidence,
            metadata={"module_commands_executed": False},
        )


class SlurmProvider:
    name = "slurm"
    _FORMAT = "%P|%a|%l|%D|%c|%m|%G"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        access_target = next((item for item in state.targets if item.role == "access_target"), None)
        allocation = {
            field: value
            for variable, field in _SLURM_ALLOCATION_ENV.items()
            if (value := request.environment.get(variable))
        }
        executable = shutil.which("sinfo", path=request.environment.get("PATH"))
        if executable is None:
            if allocation:
                scheduler = SchedulerResource(
                    id=new_ulid(), snapshot_id=request.snapshot_id,
                    access_target_id=None if access_target is None else access_target.id,
                    family="slurm",
                    state="allocation_observed", provider=self.name,
                    current_allocation=allocation,
                    metadata={"topology": "unavailable"},
                )
                return ProviderOutput(
                    status="partial", diagnostic="Slurm allocation observed but sinfo is unavailable",
                    schedulers=[scheduler],
                    evidence=[
                        _evidence(
                            request, subject_type="scheduler", subject_id=scheduler.id,
                            provider=self.name, evidence_type="allocation_environment",
                        )
                    ],
                )
            return ProviderOutput(status="unavailable", diagnostic="sinfo executable not found")
        argv = [executable, "--noheader", f"--format={self._FORMAT}"]
        try:
            result = request.runner(
                argv, timeout=COMMAND_TIMEOUT_SECONDS,
                max_output_bytes=MAX_COMMAND_OUTPUT_BYTES,
            )
        except OSError as exc:
            return ProviderOutput(status="error", diagnostic=f"Slurm query failed: {exc}")
        if failure := _command_failure(result, "Slurm partition query"):
            return failure
        scheduler = SchedulerResource(
            id=new_ulid(), snapshot_id=request.snapshot_id,
            access_target_id=None if access_target is None else access_target.id,
            family="slurm",
            state="observed", provider=self.name, current_allocation=allocation,
            metadata={"topology_source": "partition_summary"},
        )
        targets: list[DiscoveredTarget] = []
        evidence: list[DiscoveryEvidence] = [
            _evidence(
                request, subject_type="scheduler", subject_id=scheduler.id,
                provider=self.name, evidence_type="read_only_partition_summary",
            )
        ]
        malformed = 0
        hostname = platform.node() or "current-target"
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            fields = [item.strip() for item in line.split("|")]
            if len(fields) != 7:
                malformed += 1
                continue
            partition, availability, walltime, nodes, cpus, memory, generic_resources = fields
            partition = partition.rstrip("*")
            target = DiscoveredTarget(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                parent_target_id=None if access_target is None else access_target.id,
                kind="scheduler_target_class", role="execution_target_class",
                name=partition, locator=f"slurm://{hostname}/partition/{partition}",
                state=availability or "unknown", visible=True,
                authorization="unknown", provider=self.name,
                metadata={
                    "scheduler": "slurm", "wall_time_limit": walltime,
                    "visible_nodes": nodes, "cpus_per_node": cpus,
                    "memory_per_node": memory,
                    "generic_resources": generic_resources,
                    "node_names_enumerated": False,
                },
            )
            targets.append(target)
            evidence.append(
                _evidence(
                    request, subject_type="target", subject_id=target.id,
                    provider=self.name, evidence_type="scheduler_partition_summary",
                    details={"visibility": "visible", "authorization": "unknown"},
                )
            )
        if malformed and not targets:
            return ProviderOutput(
                status="error", diagnostic="malformed Slurm partition response",
                schedulers=[scheduler], evidence=evidence,
            )
        scheduler = replace(scheduler, execution_target_ids=[item.id for item in targets])
        partial = malformed > 0 or result.truncated
        return ProviderOutput(
            status="partial" if partial else "complete",
            diagnostic=(f"ignored {malformed} malformed partition row(s)" if malformed else None),
            truncated=result.truncated, schedulers=[scheduler], targets=targets,
            evidence=evidence,
            metadata={
                "command": argv, "job_query": "none", "node_detail_query": False,
                "submission_commands": False, "cancellation_commands": False,
            },
        )


class PBSProvider:
    name = "pbs"
    _SAFE_FIELDS = {
        "queue_type", "enabled", "started", "max_running", "max_user_run",
        "resources_default.walltime", "resources_max.walltime",
        "resources_max.ncpus", "resources_max.mem",
    }

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        access_target = next((item for item in state.targets if item.role == "access_target"), None)
        allocation = {
            field: value
            for variable, field in _PBS_ALLOCATION_ENV.items()
            if (value := request.environment.get(variable))
        }
        executable = shutil.which("qstat", path=request.environment.get("PATH"))
        if executable is None:
            if allocation:
                scheduler = SchedulerResource(
                    id=new_ulid(), snapshot_id=request.snapshot_id,
                    access_target_id=None if access_target is None else access_target.id,
                    family="pbs",
                    state="allocation_observed", provider=self.name,
                    current_allocation=allocation, metadata={"topology": "unavailable"},
                )
                return ProviderOutput(
                    status="partial", diagnostic="PBS allocation observed but qstat is unavailable",
                    schedulers=[scheduler],
                )
            return ProviderOutput(status="unavailable", diagnostic="qstat executable not found")
        argv = [executable, "-Q", "-f"]
        try:
            result = request.runner(
                argv, timeout=COMMAND_TIMEOUT_SECONDS,
                max_output_bytes=MAX_COMMAND_OUTPUT_BYTES,
            )
        except OSError as exc:
            return ProviderOutput(status="error", diagnostic=f"PBS query failed: {exc}")
        if failure := _command_failure(result, "PBS queue query"):
            return failure
        queues: list[tuple[str, dict[str, str]]] = []
        current_name: str | None = None
        current_fields: dict[str, str] = {}
        malformed = 0
        for line in result.stdout.splitlines():
            if line.startswith("Queue:"):
                if current_name is not None:
                    queues.append((current_name, current_fields))
                current_name = line.partition(":")[2].strip()
                current_fields = {}
            elif "=" in line and current_name is not None:
                key, _, value = line.strip().partition("=")
                key = key.strip()
                if key in self._SAFE_FIELDS:
                    current_fields[key] = value.strip()
            elif line.strip():
                malformed += 1
        if current_name is not None:
            queues.append((current_name, current_fields))
        if result.stdout.strip() and not queues:
            return ProviderOutput(status="error", diagnostic="malformed PBS queue response")
        scheduler = SchedulerResource(
            id=new_ulid(), snapshot_id=request.snapshot_id,
            access_target_id=None if access_target is None else access_target.id,
            family="pbs",
            state="observed", provider=self.name, current_allocation=allocation,
            metadata={"topology_source": "queue_summary"},
        )
        hostname = platform.node() or "current-target"
        targets = [
            DiscoveredTarget(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                parent_target_id=None if access_target is None else access_target.id,
                kind="scheduler_target_class", role="execution_target_class",
                name=name, locator=f"pbs://{hostname}/queue/{name}",
                state=fields.get("started", "unknown"), visible=True,
                authorization="unknown", provider=self.name,
                metadata={"scheduler": "pbs", **fields, "node_names_enumerated": False},
            )
            for name, fields in queues
        ]
        scheduler = replace(scheduler, execution_target_ids=[item.id for item in targets])
        evidence = [
            _evidence(
                request, subject_type="scheduler", subject_id=scheduler.id,
                provider=self.name, evidence_type="read_only_queue_summary",
            ),
            *[
                _evidence(
                    request, subject_type="target", subject_id=target.id,
                    provider=self.name, evidence_type="scheduler_queue_summary",
                    details={"visibility": "visible", "authorization": "unknown"},
                )
                for target in targets
            ],
        ]
        partial = malformed > 0 or result.truncated
        return ProviderOutput(
            status="partial" if partial else "complete",
            diagnostic=(f"ignored {malformed} malformed queue row(s)" if malformed else None),
            truncated=result.truncated, schedulers=[scheduler], targets=targets,
            evidence=evidence,
            metadata={
                "command": argv, "job_query": "none", "submission_commands": False,
                "cancellation_commands": False,
            },
        )


class LSFProvider:
    """Bounded IBM LSF queue discovery without job submission or mutation."""

    name = "lsf"
    _QUEUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}\Z")

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        access_target = next(
            (item for item in state.targets if item.role == "access_target"), None
        )
        allocation = {
            field: value
            for variable, field in _LSF_ALLOCATION_ENV.items()
            if (value := request.environment.get(variable))
        }
        executable = shutil.which("bqueues", path=request.environment.get("PATH"))
        if executable is None:
            if allocation:
                scheduler = SchedulerResource(
                    id=new_ulid(), snapshot_id=request.snapshot_id,
                    access_target_id=None if access_target is None else access_target.id,
                    family="lsf", state="allocation_observed", provider=self.name,
                    current_allocation=allocation,
                    metadata={"topology": "unavailable"},
                )
                return ProviderOutput(
                    status="partial",
                    diagnostic="LSF allocation observed but bqueues is unavailable",
                    schedulers=[scheduler],
                    evidence=[
                        _evidence(
                            request, subject_type="scheduler", subject_id=scheduler.id,
                            provider=self.name, evidence_type="allocation_environment",
                        )
                    ],
                )
            return ProviderOutput(
                status="unavailable", diagnostic="bqueues executable not found"
            )
        argv = [
            executable, "-noheader", "-o",
            "queue_name stat max jl_u njobs pend run",
        ]
        try:
            result = request.runner(
                argv, timeout=COMMAND_TIMEOUT_SECONDS,
                max_output_bytes=MAX_COMMAND_OUTPUT_BYTES,
            )
        except OSError as exc:
            return ProviderOutput(status="error", diagnostic=f"LSF query failed: {exc}")
        if failure := _command_failure(result, "LSF queue query"):
            return failure
        scheduler = SchedulerResource(
            id=new_ulid(), snapshot_id=request.snapshot_id,
            access_target_id=None if access_target is None else access_target.id,
            family="lsf", state="observed", provider=self.name,
            current_allocation=allocation,
            metadata={"topology_source": "queue_summary"},
        )
        hostname = platform.node() or "current-target"
        targets: list[DiscoveredTarget] = []
        evidence: list[DiscoveryEvidence] = [
            _evidence(
                request, subject_type="scheduler", subject_id=scheduler.id,
                provider=self.name, evidence_type="read_only_queue_summary",
            )
        ]
        malformed = 0
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            fields = line.split()
            if len(fields) != 7 or self._QUEUE.fullmatch(fields[0]) is None:
                malformed += 1
                continue
            queue, status, maximum, user_limit, jobs, pending, running = fields
            target = DiscoveredTarget(
                id=new_ulid(), snapshot_id=request.snapshot_id,
                parent_target_id=None if access_target is None else access_target.id,
                kind="scheduler_target_class", role="execution_target_class",
                name=queue, locator=f"lsf://{hostname}/queue/{queue}",
                state=status.casefold(), visible=True, authorization="unknown",
                provider=self.name,
                metadata={
                    "scheduler": "lsf", "max_jobs": maximum,
                    "max_user_jobs": user_limit, "jobs": jobs,
                    "pending_jobs": pending, "running_jobs": running,
                    "node_capacity": "unknown", "node_names_enumerated": False,
                },
            )
            targets.append(target)
            evidence.append(
                _evidence(
                    request, subject_type="target", subject_id=target.id,
                    provider=self.name, evidence_type="scheduler_queue_summary",
                    details={"visibility": "visible", "authorization": "unknown"},
                )
            )
        if malformed and not targets:
            return ProviderOutput(
                status="error", diagnostic="malformed LSF queue response",
                schedulers=[scheduler], evidence=evidence,
            )
        scheduler = replace(
            scheduler, execution_target_ids=[item.id for item in targets]
        )
        partial = malformed > 0 or result.truncated
        return ProviderOutput(
            status="partial" if partial else "complete",
            diagnostic=(
                f"ignored {malformed} malformed queue row(s)" if malformed else None
            ),
            truncated=result.truncated, schedulers=[scheduler], targets=targets,
            evidence=evidence,
            metadata={
                "command": argv, "job_query": "none",
                "submission_commands": False, "cancellation_commands": False,
            },
        )


class BourneHistoryProvider:
    name = "bourne_history"

    def discover(self, request: DiscoveryRequest, state: DiscoveryState) -> ProviderOutput:
        observations = request.store.history_observations()
        if not observations:
            return ProviderOutput()
        context = DiscoveredExecutionContext(
            id=new_ulid(), snapshot_id=request.snapshot_id, target_id=None,
            context_key="history:bourne", kind="history", name="Bourne history",
            locator=None, state="historical", provider=self.name,
            metadata={"current_availability": "not_established"},
        )
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for observation in observations:
            runtime = observation["execution_context"]
            locator = runtime.get("resolved_executable") or observation["command"]
            name = Path(str(locator)).name or str(observation["command"])
            key = (name, str(locator))
            aggregate = grouped.setdefault(
                key,
                {
                    "completed": 0, "failed": 0, "interrupted": 0,
                    "last_observed": observation["started_at"], "hosts": set(),
                    "context_hints": set(),
                },
            )
            status = observation["status"]
            if status in aggregate:
                aggregate[status] += 1
            host = observation["system"].get("hostname")
            if host:
                aggregate["hosts"].add(host)
            hints = runtime.get("environment_hints", {})
            aggregate["context_hints"].update(hints.keys())
        capabilities: list[Capability] = []
        evidence: list[DiscoveryEvidence] = [
            _evidence(
                request, subject_type="execution_context", subject_id=context.id,
                provider=self.name, evidence_type="bourne_experiment_history",
                observed_now=False, historical_only=True,
            )
        ]
        for (name, locator), aggregate in sorted(grouped.items()):
            metadata = {
                "completed_observations": aggregate["completed"],
                "failed_observations": aggregate["failed"],
                "interrupted_observations": aggregate["interrupted"],
                "last_observed": aggregate["last_observed"],
                "hosts": sorted(aggregate["hosts"]),
                "context_hint_names": sorted(aggregate["context_hints"]),
                "current_availability": "not_established",
            }
            capability = Capability(
                id=new_ulid(), snapshot_id=request.snapshot_id, context_id=context.id,
                kind="executable", name=name, locator=locator,
                observation_state="historical", provider=self.name,
                classifications=classify_executable(name), metadata=metadata,
            )
            capabilities.append(capability)
            evidence.append(
                _evidence(
                    request, subject_type="capability", subject_id=capability.id,
                    provider=self.name, evidence_type="bourne_experiment_history",
                    observed_now=False, historical_only=True, details=metadata,
                )
            )
        return ProviderOutput(
            contexts=[context], capabilities=capabilities, evidence=evidence,
            metadata={"experiment_records_considered": len(observations)},
        )


def default_providers() -> list[DiscoveryProvider]:
    return [
        IdentityProvider(),
        CurrentTargetProvider(),
        CurrentEnvironmentProvider(),
        StorageProvider(),
        PathExecutableProvider(),
        CondaProvider(),
        VirtualenvProvider(),
        ContainerProvider("docker"),
        ContainerProvider("podman"),
        ModuleProvider(),
        SlurmProvider(),
        PBSProvider(),
        LSFProvider(),
        SystemCapabilityProvider(),
        BourneHistoryProvider(),
    ]
