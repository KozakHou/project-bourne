"""Human-readable planning and execution lifecycle presentation."""

from __future__ import annotations

import shlex

from .presentation import DEFAULT_ID_PREFIX_LENGTH
from .workload_models import ExecutionAttempt, ExecutionView, ResolutionResult


def format_resolution(result: ResolutionResult) -> str:
    lines = ["Execution candidates:"]
    if not result.candidates:
        lines.append("  None discovered.")
    for index, candidate in enumerate(result.candidates, start=1):
        target = candidate.execution_target_id or "unknown"
        lines.extend(
            [
                f"  {index}. {candidate.backend} / {target}",
                f"     Compatibility: {candidate.compatibility_state}",
            ]
        )
        for item in candidate.decision_evidence:
            lines.append(f"     Evidence ({item.state}): {item.message}")
        for item in candidate.unresolved_conditions:
            lines.append(f"     Unresolved: {item}")
    if result.selected is None:
        lines.extend(["", f"No plan selected: {result.reason or 'selection is unresolved'}"])
    else:
        plan = result.selected
        lines.extend(
            [
                "", f"Plan: {plan.id}", f"Backend: {plan.backend}",
                f"Inventory: {plan.inventory_snapshot_id}",
                f"Command: {shlex.join(plan.argv)}",
                f"Compatibility: {plan.compatibility_state}",
            ]
        )
    return "\n".join(lines)


def format_execution_list(executions: list[ExecutionAttempt]) -> str:
    if not executions:
        return "No executions recorded."
    headers = ("EXECUTION", "BACKEND", "STATE", "UPDATED (UTC)")
    rows = [
        (item.id[:DEFAULT_ID_PREFIX_LENGTH], item.backend, item.state, item.updated_at)
        for item in executions
    ]
    widths = [max(len(headers[i]), *(len(row[i]) for row in rows)) for i in range(3)]
    lines = ["  ".join(headers[i].ljust(widths[i]) for i in range(3)) + "  " + headers[3]]
    lines.extend(
        "  ".join(row[i].ljust(widths[i]) for i in range(3)) + "  " + row[3]
        for row in rows
    )
    return "\n".join(lines)


def format_execution(view: ExecutionView) -> str:
    plan = view.plan
    execution = view.execution
    lines = [
        f"Execution: {execution.id}", f"State: {execution.state}",
        f"Backend: {execution.backend}", f"Plan: {plan.id}",
        f"Request: {view.request_id or 'unavailable (pre-v0.5 plan)'}",
        f"Workload: {view.workload.id}",
        f"Inventory: {plan.inventory_snapshot_id}",
        f"Command: {shlex.join(plan.argv)}",
        f"Working directory: {plan.working_directory}",
        f"Requested resources: {plan.requested_resources}",
        f"Compatibility: {plan.compatibility_state}",
        "Unresolved planning conditions:",
    ]
    if plan.unresolved_conditions:
        lines.extend(f"  {item}" for item in plan.unresolved_conditions)
    else:
        lines.append("  none")
    lines.append("Planning evidence:")
    if plan.decision_evidence:
        lines.extend(
            f"  {item.state}: {item.message}" for item in plan.decision_evidence
        )
    else:
        lines.append("  none")
    if view.scheduler_job is not None:
        lines.extend(
            [
                f"Scheduler: {view.scheduler_job.family}",
                f"Scheduler job: {view.scheduler_job.job_id}",
                f"Scheduler state: {view.scheduler_job.state}",
            ]
        )
    lines.append(f"Actual experiment: {view.experiment_id or 'unavailable'}")
    lines.append(
        "Verification: "
        + (
            "unavailable"
            if view.verification is None
            else str(view.verification["aggregate_state"])
        )
    )
    lines.append(
        "Telemetry: "
        + (
            "off or unavailable"
            if view.telemetry is None
            else str(view.telemetry["state"])
        )
    )
    lines.append("Allocated resources:")
    if view.allocations:
        for allocation in view.allocations:
            lines.append(f"  {allocation.resources} on {', '.join(allocation.hosts)}")
    else:
        lines.append("  unknown")
    lines.append("Lifecycle:")
    lines.extend(f"  {item.occurred_at}  {item.state}" for item in view.events)
    if execution.error:
        lines.append(f"Error: {execution.error}")
    return "\n".join(lines)
