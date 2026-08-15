"""Human-readable CLI presentation."""

from __future__ import annotations

import json
import shlex
from typing import Any, Sequence

from .models import Artifact, Experiment
from .tracing import ArtifactTrace

DEFAULT_ID_PREFIX_LENGTH = 10


def format_command(experiment: Experiment) -> str:
    return shlex.join(experiment.argv)


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    return f"{seconds:.3f} s"


def format_list(experiments: list[Experiment], full_id: bool = False) -> str:
    if not experiments:
        return "No experiments recorded."

    headers = ("EXPERIMENT", "STATUS", "STARTED (UTC)", "DURATION", "COMMAND")
    rows = [
        (
            experiment.id if full_id else experiment.id[:DEFAULT_ID_PREFIX_LENGTH],
            experiment.status,
            experiment.started_at,
            format_duration(experiment.duration_seconds),
            format_command(experiment),
        )
        for experiment in experiments
    ]
    widths = [max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(4)]
    output = [
        "  ".join(headers[index].ljust(widths[index]) for index in range(4))
        + "  "
        + headers[4]
    ]
    for row in rows:
        output.append(
            "  ".join(row[index].ljust(widths[index]) for index in range(4))
            + "  "
            + row[4]
        )
    return "\n".join(output)


def _display(value: object) -> str:
    if value is None:
        return "unavailable"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _artifact_state(artifact: Artifact) -> str:
    if not artifact.exists:
        return "missing"
    if artifact.capture_error:
        return "present (capture incomplete)"
    return "present"


def _artifact_lines(artifact: Artifact, indent: str = "  ") -> list[str]:
    size = "unavailable" if artifact.size_bytes is None else f"{artifact.size_bytes} bytes"
    lines = [
        f"{indent}{artifact.original_path}",
        f"{indent}  Record: {artifact.id}",
        f"{indent}  Resolved path: {artifact.resolved_path}",
        f"{indent}  State: {_artifact_state(artifact)}",
        f"{indent}  SHA-256: {_display(artifact.sha256)}",
        f"{indent}  Size: {size}",
        f"{indent}  Modified (UTC): {_display(artifact.modified_at)}",
        f"{indent}  Captured (UTC): {artifact.captured_at}",
    ]
    if artifact.capture_error:
        lines.append(f"{indent}  Capture note: {artifact.capture_error}")
    return lines


def format_show(
    experiment: Experiment,
    artifacts: Sequence[Artifact] = (),
    parent: Experiment | None = None,
) -> str:
    git = experiment.git
    system = experiment.system
    context = experiment.execution_context
    lines = [
        f"Experiment: {experiment.id}",
        f"Status: {experiment.status}",
        f"Command: {format_command(experiment)}",
        f"Executable: {experiment.command}",
        f"Arguments: {json.dumps(experiment.arguments, ensure_ascii=False)}",
        f"Working directory: {experiment.working_directory}",
        f"Started (UTC): {experiment.started_at}",
        f"Ended (UTC): {experiment.ended_at}",
        f"Duration: {format_duration(experiment.duration_seconds)}",
        f"Exit code: {experiment.exit_code}",
        "",
        "Git provenance:",
        f"  Available: {_display(git.available)}",
        f"  Repository root: {_display(git.repository_root)}",
        f"  Commit: {_display(git.commit_sha)}",
        f"  Branch: {_display(git.branch)}",
        f"  Dirty: {_display(git.dirty)}",
        f"  Collection note: {_display(git.error)}",
        "",
        "System provenance:",
        f"  Operating system: {system.operating_system}",
        f"  OS version: {system.os_version}",
        f"  Architecture: {system.architecture}",
        f"  Hostname: {system.hostname}",
        f"  CPU: {_display(system.cpu)}",
        f"  NVIDIA GPU available: {_display(system.gpu_available)}",
    ]
    if system.gpus:
        for gpu in system.gpus:
            lines.append(
                "  GPU {index}: {name} (UUID {uuid}, {memory_total_mib} MiB)".format(**gpu)
            )
    else:
        lines.append("  GPUs: unavailable")
    lines.extend(
        [
            f"  NVIDIA driver: {_display(system.nvidia_driver_version)}",
            f"  CUDA: {_display(system.cuda_version)}",
            f"  CUDA source: {_display(system.cuda_version_source)}",
            f"  GPU collection note: {_display(system.gpu_error)}",
            f"  System collection note: {_display(system.collector_error)}",
            "",
            "Execution context:",
            f"  Requested executable: {_display(context.requested_executable)}",
            f"  Resolved executable: {_display(context.resolved_executable)}",
            f"  Bourne recorder executable: {_display(context.recorder_executable)}",
            f"  Container marker observed: {_display(context.containerized)}",
            "  Safe environment hints:",
        ]
    )
    if context.environment_hints:
        for name, value in sorted(context.environment_hints.items()):
            lines.append(f"    {name}: {value}")
    else:
        lines.append("    unavailable")

    inputs = [artifact for artifact in artifacts if artifact.role == "input"]
    outputs = [artifact for artifact in artifacts if artifact.role == "output"]
    lines.extend(["", "Inputs:"])
    if inputs:
        for artifact in inputs:
            lines.extend(_artifact_lines(artifact))
    else:
        lines.append("  None declared.")
    lines.extend(["", "Outputs:"])
    if outputs:
        for artifact in outputs:
            lines.extend(_artifact_lines(artifact))
    else:
        lines.append("  None declared.")
    lines.extend(["", "Lineage:"])
    if parent is None:
        lines.append("  No parent experiment.")
    else:
        lines.extend(
            [
                "  Derived from:",
                f"    Experiment: {parent.id}",
                f"    Command: {format_command(parent)}",
            ]
        )

    lines.extend(
        [
            "",
            "Stdout:",
            experiment.stdout.rstrip("\n") or "  <empty>",
            "",
            "Stderr:",
            experiment.stderr.rstrip("\n") or "  <empty>",
        ]
    )
    return "\n".join(lines)


def format_trace(trace: ArtifactTrace) -> str:
    artifact = trace.artifact
    producer = trace.producer
    context = producer.execution_context
    git = producer.git
    size = "unavailable" if artifact.size_bytes is None else f"{artifact.size_bytes} bytes"
    lines = [
        f"Artifact: {artifact.original_path}",
        f"Artifact record: {artifact.id}",
        f"Resolved path: {artifact.resolved_path}",
        f"State: {_artifact_state(artifact)}",
        f"SHA-256: {_display(artifact.sha256)}",
        f"Size: {size}",
        "",
        "Producing experiment:" if artifact.exists else "Declaring experiment:",
        f"  Experiment: {producer.id}",
        f"  Status: {producer.status}",
        f"  Command: {format_command(producer)}",
        f"  Working directory: {producer.working_directory}",
        f"  Requested executable: {_display(context.requested_executable)}",
        f"  Resolved executable: {_display(context.resolved_executable)}",
        f"  Git commit: {_display(git.commit_sha)}",
        f"  Git branch: {_display(git.branch)}",
        f"  Git dirty: {_display(git.dirty)}",
        f"  Host: {producer.system.hostname}",
        f"  Environment hints: {_display(context.environment_hints or None)}",
        "",
        "Inputs:",
    ]
    if trace.inputs:
        for item in trace.inputs:
            lines.extend(_artifact_lines(item))
    else:
        lines.append("  None declared.")
    association = "produced artifact" if artifact.exists else "declared missing output"
    lines.extend(["", "Ancestry:", f"  {producer.id} ({association})"])
    for parent in trace.ancestry:
        lines.append(f"  <- {parent.id} (derived_from; {format_command(parent)})")
    if not trace.ancestry:
        lines.append("  No parent experiment.")
    return "\n".join(lines)


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    if isinstance(value, dict):
        for key in sorted(value):
            name = f"{prefix}.{key}" if prefix else key
            flattened.update(_flatten(value[key], name))
    else:
        flattened[prefix] = value
    return flattened


def _comparison_value(value: Any) -> str:
    rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return rendered.replace("\n", "\\n")


def format_compare(first: Experiment, second: Experiment) -> str:
    first_values = _flatten(first.to_dict())
    second_values = _flatten(second.to_dict())
    # IDs identify the records rather than describing a provenance difference.
    first_values.pop("id", None)
    second_values.pop("id", None)

    fields = sorted(set(first_values) | set(second_values))
    differences = [field for field in fields if first_values.get(field) != second_values.get(field)]
    lines = [f"Comparing {first.id} -> {second.id}", "", "Differing fields:"]
    if not differences:
        lines.append("  None")
        return "\n".join(lines)

    for field in differences:
        lines.append(f"  {field}")
        lines.append(f"    A: {_comparison_value(first_values.get(field))}")
        lines.append(f"    B: {_comparison_value(second_values.get(field))}")
    return "\n".join(lines)
