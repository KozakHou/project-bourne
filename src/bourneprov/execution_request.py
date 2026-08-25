"""Bounded, non-executable ExecutionRequest contract with v1 compatibility."""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, replace
from importlib.resources import files
from pathlib import Path
from typing import Any, Mapping, Sequence

from .ids import new_ulid
from .workload import utc_now
from .workload_models import ExecutionConstraints, ResourceRequirements

REQUEST_KIND = "bourne.execution-request"
RELEASED_REQUEST_SCHEMA_VERSION = 1
REQUEST_SCHEMA_VERSION = 2
MAX_REQUEST_BYTES = 1024 * 1024
MAX_REQUEST_ARGV = 4096
MAX_REQUEST_STRING = 16 * 1024
MAX_REQUEST_ARTIFACTS_PER_ROLE = 2048
MAX_REQUEST_CHECKS = 2048
MAX_REQUEST_JSON_DEPTH = 12
MAX_SOURCE_METADATA_FIELDS = 32

_SOURCE_KINDS = frozenset({"cli", "file", "sdk"})
_TELEMETRY_MODES = frozenset({"off", "summary"})
_CHECK_TYPES = frozenset({"output_exists", "output_min_bytes", "output_sha256"})
_SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")
_MEMORY = re.compile(r"([1-9][0-9]*)([KMGT](?:i?B)?|B)?\Z", re.IGNORECASE)
_WALLTIME = re.compile(r"([1-9][0-9]*)([HMS])\Z", re.IGNORECASE)
_CLOCK = re.compile(r"([0-9]+):([0-5][0-9]):([0-5][0-9])\Z")


class ExecutionRequestError(ValueError):
    """The request is malformed, unsupported, or exceeds a safety bound."""


@dataclass(frozen=True)
class RequestArtifacts:
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()


@dataclass(frozen=True)
class RequestSource:
    kind: str
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in _SOURCE_KINDS:
            raise ExecutionRequestError(f"unsupported request source kind: {self.kind}")
        if len(self.metadata) > MAX_SOURCE_METADATA_FIELDS:
            raise ExecutionRequestError("request source metadata exceeds the field limit")
        if len({key for key, _value in self.metadata}) != len(self.metadata):
            raise ExecutionRequestError("request source metadata keys must be unique")
        for key, value in self.metadata:
            _bounded_string(key, "request source metadata key", allow_empty=False)
            _bounded_string(value, "request source metadata value")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "metadata": dict(self.metadata)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RequestSource":
        _reject_unknown(value, {"kind", "metadata"}, "persisted request source")
        kind = value.get("kind")
        metadata = value.get("metadata", {})
        if not isinstance(kind, str) or not isinstance(metadata, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in metadata.items()
        ):
            raise ExecutionRequestError("persisted request source is invalid")
        return cls(
            kind=kind,
            metadata=tuple(sorted(metadata.items())),
        )


@dataclass(frozen=True)
class VerificationCheckSpec:
    type: str
    path: str
    min_bytes: int | None = None
    sha256: str | None = None

    def __post_init__(self) -> None:
        if self.type not in _CHECK_TYPES:
            raise ExecutionRequestError(f"unsupported verification check: {self.type}")
        _bounded_string(self.path, "verification path", allow_empty=False)
        if self.type == "output_exists":
            if self.min_bytes is not None or self.sha256 is not None:
                raise ExecutionRequestError("output_exists accepts only type and path")
        elif self.type == "output_min_bytes":
            if (
                isinstance(self.min_bytes, bool)
                or not isinstance(self.min_bytes, int)
                or self.min_bytes < 0
                or self.sha256 is not None
            ):
                raise ExecutionRequestError(
                    "output_min_bytes requires a non-negative min_bytes value"
                )
        elif (
            self.min_bytes is not None
            or not isinstance(self.sha256, str)
            or _SHA256.fullmatch(self.sha256) is None
        ):
            raise ExecutionRequestError("output_sha256 requires a 64-digit SHA-256 value")

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"type": self.type, "path": self.path}
        if self.min_bytes is not None:
            value["min_bytes"] = self.min_bytes
        if self.sha256 is not None:
            value["sha256"] = self.sha256
        return value


@dataclass(frozen=True)
class ExecutionRequest:
    """Immutable normalized intent, kept distinct from a WorkloadSpec."""

    id: str
    created_at: str
    command: tuple[str, ...]
    base_directory: str
    working_directory: str
    resolved_working_directory: str
    artifacts: RequestArtifacts
    resources: ResourceRequirements
    execution: ExecutionConstraints
    requested_parent_experiment: str | None
    resolved_parent_experiment_id: str | None
    telemetry_mode: str
    verification_checks: tuple[VerificationCheckSpec, ...]
    source: RequestSource
    kind: str = REQUEST_KIND
    request_schema_version: int = REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.kind != REQUEST_KIND:
            raise ExecutionRequestError(f"unsupported request kind: {self.kind}")
        if self.request_schema_version not in {
            RELEASED_REQUEST_SCHEMA_VERSION, REQUEST_SCHEMA_VERSION
        }:
            raise ExecutionRequestError(
                f"unsupported execution request version: {self.request_schema_version}"
            )
        if not self.id or not self.created_at:
            raise ExecutionRequestError("request ID and creation time are required")
        _validate_string_list(
            self.command, "command", MAX_REQUEST_ARGV, require_nonempty=True
        )
        _bounded_string(self.base_directory, "request base directory", allow_empty=False)
        _bounded_string(self.working_directory, "working directory", allow_empty=False)
        _bounded_string(
            self.resolved_working_directory,
            "resolved working directory",
            allow_empty=False,
        )
        _validate_string_list(
            self.artifacts.inputs,
            "input artifacts",
            MAX_REQUEST_ARTIFACTS_PER_ROLE,
        )
        _validate_string_list(
            self.artifacts.outputs,
            "output artifacts",
            MAX_REQUEST_ARTIFACTS_PER_ROLE,
        )
        if len(set(self.artifacts.inputs)) != len(self.artifacts.inputs):
            raise ExecutionRequestError("input artifact paths must be unique")
        if len(set(self.artifacts.outputs)) != len(self.artifacts.outputs):
            raise ExecutionRequestError("output artifact paths must be unique")
        if self.telemetry_mode not in _TELEMETRY_MODES:
            raise ExecutionRequestError(
                f"unsupported telemetry mode: {self.telemetry_mode}"
            )
        if len(self.verification_checks) > MAX_REQUEST_CHECKS:
            raise ExecutionRequestError("verification checks exceed the count limit")
        outputs = set(self.artifacts.outputs)
        for check in self.verification_checks:
            if check.path not in outputs:
                raise ExecutionRequestError(
                    f"verification path {check.path!r} is not a declared output"
                )
        if self.requested_parent_experiment is not None:
            _bounded_string(
                self.requested_parent_experiment,
                "requested parent experiment",
                allow_empty=False,
            )
        if self.resolved_parent_experiment_id is not None:
            if self.requested_parent_experiment is None:
                raise ExecutionRequestError(
                    "a resolved parent experiment requires a requested reference"
                )
            _bounded_string(
                self.resolved_parent_experiment_id,
                "resolved parent experiment ID",
                allow_empty=False,
            )

    @property
    def argv(self) -> list[str]:
        return list(self.command)

    def semantic_dict(self) -> dict[str, Any]:
        """Return user intent without identity or source provenance."""

        return {
            "command": list(self.command),
            "resolved_working_directory": self.resolved_working_directory,
            "artifacts": {
                "inputs": list(self.artifacts.inputs),
                "outputs": list(self.artifacts.outputs),
            },
            "resources": asdict(self.resources),
            "execution": asdict(self.execution),
            "provenance": {
                "parent_experiment": self.requested_parent_experiment
            },
            "telemetry": {"mode": self.telemetry_mode},
            "verification": {
                "checks": [item.to_dict() for item in self.verification_checks]
            },
        }

    def to_document(self) -> dict[str, Any]:
        """Return the stable public ExecutionRequest document."""

        value: dict[str, Any] = {
            "kind": REQUEST_KIND,
            "version": self.request_schema_version,
            "command": list(self.command),
            "working_directory": self.working_directory,
        }
        if self.artifacts.inputs or self.artifacts.outputs:
            value["artifacts"] = {
                "inputs": list(self.artifacts.inputs),
                "outputs": list(self.artifacts.outputs),
            }
        resources = {
            "cpus": self.resources.cpus,
            "gpus": self.resources.gpus,
            "nodes": self.resources.nodes,
            "mpi_ranks": self.resources.mpi_ranks,
            "memory": self.resources.memory_bytes,
            "walltime": self.resources.walltime_seconds,
        }
        resources = {key: item for key, item in resources.items() if item is not None}
        if resources:
            value["resources"] = resources
        execution = {
            "backend": self.execution.backend,
            "target": self.execution.target,
            "context": self.execution.context,
        }
        if execution != {"backend": "auto", "target": None, "context": None}:
            value["execution"] = {
                key: item for key, item in execution.items() if item is not None
            }
        if self.requested_parent_experiment is not None:
            value["provenance"] = {
                "parent_experiment": self.requested_parent_experiment
            }
        if self.telemetry_mode != "summary":
            value["telemetry"] = {"mode": self.telemetry_mode}
        if self.verification_checks:
            value["verification"] = {
                "checks": [item.to_dict() for item in self.verification_checks]
            }
        return value

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "kind": self.kind,
            "request_schema_version": self.request_schema_version,
            "command": list(self.command),
            "base_directory": self.base_directory,
            "working_directory": self.working_directory,
            "resolved_working_directory": self.resolved_working_directory,
            "artifacts": {
                "inputs": list(self.artifacts.inputs),
                "outputs": list(self.artifacts.outputs),
            },
            "resources": asdict(self.resources),
            "execution": asdict(self.execution),
            "requested_parent_experiment": self.requested_parent_experiment,
            "resolved_parent_experiment_id": self.resolved_parent_experiment_id,
            "telemetry_mode": self.telemetry_mode,
            "verification_checks": [
                item.to_dict() for item in self.verification_checks
            ],
            "source": self.source.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ExecutionRequest":
        _reject_unknown(
            value,
            {
                "id", "created_at", "kind", "request_schema_version", "command",
                "base_directory", "working_directory", "resolved_working_directory",
                "artifacts", "resources", "execution",
                "requested_parent_experiment", "resolved_parent_experiment_id",
                "telemetry_mode", "verification_checks", "source",
            },
            "persisted execution request",
        )
        artifacts = value.get("artifacts")
        source = value.get("source")
        command = value.get("command")
        resources = value.get("resources")
        execution = value.get("execution")
        checks = value.get("verification_checks")
        requested_parent = value.get("requested_parent_experiment")
        resolved_parent = value.get("resolved_parent_experiment_id")
        string_fields = (
            "id", "created_at", "kind", "base_directory", "working_directory",
            "resolved_working_directory", "telemetry_mode",
        )
        if (
            not all(isinstance(value.get(name), str) for name in string_fields)
            or isinstance(value.get("request_schema_version"), bool)
            or not isinstance(value.get("request_schema_version"), int)
            or not isinstance(command, list)
            or not isinstance(artifacts, dict)
            or not isinstance(artifacts.get("inputs"), list)
            or not isinstance(artifacts.get("outputs"), list)
            or not isinstance(resources, dict)
            or not isinstance(execution, dict)
            or not isinstance(checks, list)
            or not isinstance(source, dict)
            or (
                requested_parent is not None
                and not isinstance(requested_parent, str)
            )
            or (
                resolved_parent is not None
                and not isinstance(resolved_parent, str)
            )
        ):
            raise ExecutionRequestError("persisted request structure is invalid")
        return cls(
            id=value["id"],
            created_at=value["created_at"],
            kind=value["kind"],
            request_schema_version=value["request_schema_version"],
            command=tuple(command),
            base_directory=value["base_directory"],
            working_directory=value["working_directory"],
            resolved_working_directory=value["resolved_working_directory"],
            artifacts=RequestArtifacts(
                tuple(artifacts["inputs"]), tuple(artifacts["outputs"])
            ),
            resources=ResourceRequirements.from_dict(resources),
            execution=ExecutionConstraints.from_dict(execution),
            requested_parent_experiment=requested_parent,
            resolved_parent_experiment_id=resolved_parent,
            telemetry_mode=value["telemetry_mode"],
            verification_checks=tuple(
                VerificationCheckSpec(**item)
                for item in checks
            ),
            source=RequestSource.from_dict(source),
        )

    def with_resolved_parent_experiment(
        self, canonical_id: str
    ) -> "ExecutionRequest":
        if self.requested_parent_experiment is None:
            raise ExecutionRequestError(
                "cannot resolve a parent experiment that was not requested"
            )
        return replace(self, resolved_parent_experiment_id=canonical_id)


def parse_execution_request(
    value: object,
    *,
    base_directory: Path,
    source: RequestSource,
) -> ExecutionRequest:
    """Validate data and normalize paths without executing or interpolating it."""

    _validate_json_bounds(value)
    try:
        document_size = len(
            json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        )
    except (TypeError, ValueError) as exc:
        raise ExecutionRequestError(f"request is not JSON-serializable: {exc}") from exc
    if document_size > MAX_REQUEST_BYTES:
        raise ExecutionRequestError(
            f"request document exceeds {MAX_REQUEST_BYTES} bytes"
        )
    root = _object(value, "execution request")
    _reject_unknown(
        root,
        {
            "kind",
            "version",
            "command",
            "working_directory",
            "artifacts",
            "resources",
            "execution",
            "provenance",
            "telemetry",
            "verification",
        },
        "execution request",
    )
    if root.get("kind") != REQUEST_KIND:
        raise ExecutionRequestError(
            f"request kind must be {REQUEST_KIND!r}"
        )
    version = root.get("version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise ExecutionRequestError("request version must be an integer")
    if version not in {RELEASED_REQUEST_SCHEMA_VERSION, REQUEST_SCHEMA_VERSION}:
        raise ExecutionRequestError(
            f"unsupported execution request version: {version}"
        )
    command = _string_list(
        root.get("command"),
        "command",
        MAX_REQUEST_ARGV,
        require_nonempty=True,
    )
    working_directory = root.get("working_directory", ".")
    _bounded_string(working_directory, "working directory", allow_empty=False)

    artifacts_value = _optional_object(root, "artifacts")
    _reject_unknown(artifacts_value, {"inputs", "outputs"}, "artifacts")
    inputs = _string_list(
        artifacts_value.get("inputs", []),
        "input artifacts",
        MAX_REQUEST_ARTIFACTS_PER_ROLE,
    )
    outputs = _string_list(
        artifacts_value.get("outputs", []),
        "output artifacts",
        MAX_REQUEST_ARTIFACTS_PER_ROLE,
    )

    resources_value = _optional_object(root, "resources")
    _reject_unknown(
        resources_value,
        {"cpus", "gpus", "nodes", "mpi_ranks", "memory", "walltime"},
        "resources",
    )
    resources = ResourceRequirements(
        cpus=_resource_integer(resources_value, "cpus", minimum=1),
        gpus=_resource_integer(resources_value, "gpus", minimum=0),
        nodes=_resource_integer(resources_value, "nodes", minimum=1),
        mpi_ranks=_resource_integer(resources_value, "mpi_ranks", minimum=1),
        memory_bytes=(
            None
            if "memory" not in resources_value
            else _parse_memory(resources_value["memory"])
        ),
        walltime_seconds=(
            None
            if "walltime" not in resources_value
            else _parse_walltime(resources_value["walltime"])
        ),
    )

    execution_value = _optional_object(root, "execution")
    _reject_unknown(execution_value, {"backend", "target", "context"}, "execution")
    backend = execution_value.get("backend", "auto")
    target = execution_value.get("target")
    context = execution_value.get("context")
    _bounded_string(backend, "execution backend", allow_empty=False)
    if target is not None:
        _bounded_string(target, "execution target", allow_empty=False)
    if context is not None:
        _bounded_string(context, "execution context", allow_empty=False)
    execution = ExecutionConstraints(backend=backend, target=target, context=context)
    if version == RELEASED_REQUEST_SCHEMA_VERSION and backend == "lsf":
        raise ExecutionRequestError("LSF backend requires execution request version 2")

    provenance_value = _optional_object(root, "provenance")
    _reject_unknown(provenance_value, {"parent_experiment"}, "provenance")
    parent = provenance_value.get("parent_experiment")
    if parent is not None:
        _bounded_string(parent, "parent experiment", allow_empty=False)

    telemetry_value = _optional_object(root, "telemetry")
    _reject_unknown(telemetry_value, {"mode"}, "telemetry")
    telemetry_mode = telemetry_value.get("mode", "summary")
    _bounded_string(telemetry_mode, "telemetry mode", allow_empty=False)

    verification_value = _optional_object(root, "verification")
    _reject_unknown(verification_value, {"checks"}, "verification")
    checks_value = verification_value.get("checks", [])
    if not isinstance(checks_value, list) or len(checks_value) > MAX_REQUEST_CHECKS:
        raise ExecutionRequestError("verification checks must be a bounded array")
    checks = tuple(_parse_check(item, index) for index, item in enumerate(checks_value))

    base = base_directory.resolve(strict=False)
    lexical_path = Path(working_directory)
    resolved = (
        lexical_path
        if lexical_path.is_absolute()
        else base / lexical_path
    ).resolve(strict=False)
    return ExecutionRequest(
        id=new_ulid(),
        created_at=utc_now(),
        command=tuple(command),
        base_directory=str(base),
        working_directory=working_directory,
        resolved_working_directory=str(resolved),
        artifacts=RequestArtifacts(tuple(inputs), tuple(outputs)),
        resources=resources,
        execution=execution,
        requested_parent_experiment=parent,
        resolved_parent_experiment_id=None,
        telemetry_mode=telemetry_mode,
        verification_checks=checks,
        source=source,
        request_schema_version=version,
    )


def load_execution_request(path: Path) -> ExecutionRequest:
    """Read one bounded UTF-8 JSON request relative to its file directory."""

    request_path = path.resolve(strict=False)
    try:
        size = request_path.stat().st_size
    except OSError as exc:
        raise ExecutionRequestError(f"could not read request file: {exc}") from exc
    if size > MAX_REQUEST_BYTES:
        raise ExecutionRequestError(
            f"request document exceeds {MAX_REQUEST_BYTES} bytes"
        )
    try:
        raw = request_path.read_bytes()
        if len(raw) > MAX_REQUEST_BYTES:
            raise ExecutionRequestError(
                f"request document exceeds {MAX_REQUEST_BYTES} bytes"
            )
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExecutionRequestError(f"request file is not valid UTF-8 JSON: {exc}") from exc
    return parse_execution_request(
        value,
        base_directory=request_path.parent,
        source=RequestSource(
            "file", (("path", str(request_path)),)
        ),
    )


def execution_request_from_cli(
    argv: Sequence[str],
    *,
    cwd: Path,
    inputs: Sequence[str] = (),
    outputs: Sequence[str] = (),
    resources: ResourceRequirements | None = None,
    execution: ExecutionConstraints | None = None,
    parent_experiment_id: str | None = None,
    telemetry_mode: str = "summary",
    verification_checks: Sequence[VerificationCheckSpec] = (),
    source_kind: str = "cli",
) -> ExecutionRequest:
    """Compile explicit callers into the same validated request contract."""

    requested = resources or ResourceRequirements()
    constraints = execution or ExecutionConstraints()
    document: dict[str, Any] = {
        "kind": REQUEST_KIND,
        "version": REQUEST_SCHEMA_VERSION,
        "command": [str(item) for item in argv],
        "working_directory": ".",
        "artifacts": {"inputs": list(inputs), "outputs": list(outputs)},
        "resources": {
            "cpus": requested.cpus,
            "gpus": requested.gpus,
            "nodes": requested.nodes,
            "mpi_ranks": requested.mpi_ranks,
            "memory": requested.memory_bytes,
            "walltime": requested.walltime_seconds,
        },
        "execution": {
            "backend": constraints.backend,
            "target": constraints.target,
            "context": constraints.context,
        },
        "provenance": {"parent_experiment": parent_experiment_id},
        "telemetry": {"mode": telemetry_mode},
        "verification": {
            "checks": [item.to_dict() for item in verification_checks]
        },
    }
    document["resources"] = {
        key: item for key, item in document["resources"].items() if item is not None
    }
    document["execution"] = {
        key: item for key, item in document["execution"].items() if item is not None
    }
    if parent_experiment_id is None:
        document.pop("provenance")
    return parse_execution_request(
        document,
        base_directory=cwd,
        source=RequestSource(source_kind),
    )


def encode_execution_request(request: ExecutionRequest) -> bytes:
    raw = json.dumps(
        request.to_document(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8") + b"\n"
    if len(raw) > MAX_REQUEST_BYTES:
        raise ExecutionRequestError("serialized request exceeds the size limit")
    return raw


def execution_request_schema() -> dict[str, Any]:
    """Return the packaged public schema without a network dependency."""

    resource = files("bourneprov").joinpath(
        "schemas/execution-request-v2.schema.json"
    )
    return json.loads(resource.read_text(encoding="utf-8"))


def _parse_check(value: object, index: int) -> VerificationCheckSpec:
    check = _object(value, f"verification check {index + 1}")
    _reject_unknown(
        check,
        {"type", "path", "min_bytes", "sha256"},
        f"verification check {index + 1}",
    )
    check_type = check.get("type")
    path = check.get("path")
    _bounded_string(check_type, "verification check type", allow_empty=False)
    _bounded_string(path, "verification path", allow_empty=False)
    sha = check.get("sha256")
    if isinstance(sha, str):
        sha = sha.casefold()
    return VerificationCheckSpec(
        type=check_type,
        path=path,
        min_bytes=check.get("min_bytes"),
        sha256=sha,
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ExecutionRequestError(f"duplicate request field: {key}")
        value[key] = item
    return value


def _parse_memory(value: object) -> int:
    if isinstance(value, bool):
        raise ExecutionRequestError("memory must be positive bytes or a size string")
    if isinstance(value, int):
        if value < 1:
            raise ExecutionRequestError("memory must be at least 1 byte")
        return value
    if not isinstance(value, str):
        raise ExecutionRequestError("memory must be positive bytes or a size string")
    match = _MEMORY.fullmatch(value)
    if match is None:
        raise ExecutionRequestError("memory must be a value such as 4G or 512MiB")
    unit = (match.group(2) or "B").upper().replace("IB", "").replace("B", "")
    return int(match.group(1)) * {
        "": 1,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }[unit]


def _parse_walltime(value: object) -> int:
    if isinstance(value, bool):
        raise ExecutionRequestError("walltime must be positive seconds or a duration string")
    if isinstance(value, int):
        if value < 1:
            raise ExecutionRequestError("walltime must be at least 1 second")
        return value
    if not isinstance(value, str):
        raise ExecutionRequestError("walltime must be positive seconds or a duration string")
    if re.fullmatch(r"[1-9][0-9]*", value) is not None:
        return int(value)
    match = _WALLTIME.fullmatch(value)
    if match is not None:
        return int(match.group(1)) * {
            "H": 3600,
            "M": 60,
            "S": 1,
        }[match.group(2).upper()]
    match = _CLOCK.fullmatch(value)
    if match is not None:
        seconds = (
            int(match.group(1)) * 3600
            + int(match.group(2)) * 60
            + int(match.group(3))
        )
        if seconds > 0:
            return seconds
    raise ExecutionRequestError("walltime must be seconds, 2h/30m, or HH:MM:SS")


def _resource_integer(
    value: Mapping[str, Any], key: str, *, minimum: int
) -> int | None:
    if key not in value:
        return None
    item = value[key]
    if isinstance(item, bool) or not isinstance(item, int) or item < minimum:
        raise ExecutionRequestError(f"resources.{key} must be at least {minimum}")
    return item


def _optional_object(parent: Mapping[str, Any], key: str) -> dict[str, Any]:
    if key not in parent:
        return {}
    return _object(parent[key], key)


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ExecutionRequestError(f"{label} must be a JSON object")
    return value


def _reject_unknown(
    value: Mapping[str, Any], allowed: set[str], label: str
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ExecutionRequestError(
            f"unknown {label} field(s): {', '.join(unknown)}"
        )


def _string_list(
    value: object,
    label: str,
    limit: int,
    *,
    require_nonempty: bool = False,
) -> list[str]:
    if not isinstance(value, list):
        raise ExecutionRequestError(f"{label} must be an array of strings")
    items = tuple(value)
    _validate_string_list(items, label, limit, require_nonempty=require_nonempty)
    return list(items)


def _validate_string_list(
    value: Sequence[object],
    label: str,
    limit: int,
    *,
    require_nonempty: bool = False,
) -> None:
    if len(value) > limit:
        raise ExecutionRequestError(f"{label} exceeds the count limit ({limit})")
    if require_nonempty and not value:
        raise ExecutionRequestError(f"{label} must contain at least one value")
    for item in value:
        _bounded_string(item, label, allow_empty=False)


def _bounded_string(value: object, label: str, *, allow_empty: bool = True) -> None:
    if not isinstance(value, str):
        raise ExecutionRequestError(f"{label} must be a string")
    if not allow_empty and not value:
        raise ExecutionRequestError(f"{label} must not be empty")
    if len(value) > MAX_REQUEST_STRING:
        raise ExecutionRequestError(
            f"{label} exceeds the {MAX_REQUEST_STRING}-character limit"
        )
    if "\0" in value:
        raise ExecutionRequestError(f"{label} must not contain NUL")


def _validate_json_bounds(value: object, *, depth: int = 0) -> None:
    if depth > MAX_REQUEST_JSON_DEPTH:
        raise ExecutionRequestError("request JSON exceeds the nesting-depth limit")
    if value is None or isinstance(value, (bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExecutionRequestError("request JSON contains a non-finite number")
        return
    if isinstance(value, str):
        _bounded_string(value, "request string")
        return
    if isinstance(value, list):
        if len(value) > MAX_REQUEST_ARGV:
            raise ExecutionRequestError("request JSON array exceeds the count limit")
        for item in value:
            _validate_json_bounds(item, depth=depth + 1)
        return
    if isinstance(value, dict):
        if len(value) > 256:
            raise ExecutionRequestError("request JSON object exceeds the field limit")
        for key, item in value.items():
            _bounded_string(key, "request field name", allow_empty=False)
            _validate_json_bounds(item, depth=depth + 1)
        return
    raise ExecutionRequestError("request contains a non-JSON value")
