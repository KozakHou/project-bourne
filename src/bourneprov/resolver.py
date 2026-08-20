"""Evidence-preserving workload-to-inventory execution resolution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

from .ids import new_ulid
from .inventory_models import (
    Capability,
    DiscoveredExecutionContext,
    DiscoveredTarget,
    InventorySnapshot,
    SchedulerResource,
)
from .workload import utc_now
from .workload_models import (
    CompatibilityState,
    DecisionEvidence,
    ExecutionPlan,
    PlanCandidate,
    ResolutionResult,
    WorkloadSpec,
)


def resolve_execution(
    workload: WorkloadSpec,
    inventory: InventorySnapshot,
) -> ResolutionResult:
    """Return all candidates and select only when the choice is conservative."""

    candidates = _candidates(workload, inventory)
    viable = [item for item in candidates if item.compatibility_state != "incompatible"]
    selected_candidate: PlanCandidate | None = None
    reason: str | None = None
    explicit_choice = any(
        (
            workload.constraints.backend != "auto",
            workload.constraints.target is not None,
            workload.constraints.context is not None,
        )
    )
    if explicit_choice:
        if len(viable) == 1:
            selected_candidate = viable[0]
        elif not viable:
            reason = "the explicit constraints have no viable candidate"
        else:
            reason = "the explicit constraints still match multiple candidates"
    else:
        compatible_direct = [
            item for item in viable
            if item.backend == "direct" and item.compatibility_state == "compatible"
        ]
        if len(compatible_direct) == 1:
            selected_candidate = compatible_direct[0]
        else:
            scheduled = [item for item in viable if item.backend != "direct"]
            if len(scheduled) == 1:
                selected_candidate = scheduled[0]
            elif len(scheduled) > 1:
                reason = "multiple scheduler candidates remain; select a target explicitly"
            elif viable:
                reason = "direct requirements are not all known satisfied; select a backend explicitly"
            else:
                reason = "no viable execution candidate was discovered"

    selected = (
        None
        if selected_candidate is None
        else _make_plan(workload, inventory, selected_candidate)
    )
    return ResolutionResult(candidates=candidates, selected=selected, reason=reason)


def _candidates(
    workload: WorkloadSpec,
    inventory: InventorySnapshot,
) -> list[PlanCandidate]:
    access = inventory.current_target
    if access is None:
        return []
    contexts = {item.id: item for item in inventory.execution_contexts}
    result: list[PlanCandidate] = []
    if workload.constraints.backend in {"auto", "direct"}:
        current_context = next(
            (item for item in inventory.execution_contexts if item.context_key == "current"),
            None,
        )
        if _matches_target(workload.constraints.target, access) and _matches_context(
            workload.constraints.context, current_context
        ):
            state, unresolved, evidence = _evaluate_direct(
                workload, inventory, access, current_context
            )
            result.append(
                PlanCandidate(
                    backend="direct", access_target_id=access.id,
                    execution_target_id=access.id,
                    execution_context_id=(
                        None if current_context is None else current_context.id
                    ),
                    scheduler_id=None, compatibility_state=state,
                    unresolved_conditions=unresolved, decision_evidence=evidence,
                )
            )

    for scheduler in inventory.schedulers:
        if scheduler.family not in {"slurm", "pbs"}:
            continue
        if workload.constraints.backend not in {"auto", scheduler.family}:
            continue
        scheduler_targets = [
            item for item in inventory.targets
            if item.id in scheduler.execution_target_ids
        ]
        if not scheduler_targets:
            scheduler_targets = [None]
        for target in scheduler_targets:
            if not _matches_target(workload.constraints.target, target):
                continue
            target_contexts = [
                item for item in inventory.execution_contexts
                if target is not None and item.target_id == target.id
            ]
            if workload.constraints.context is not None:
                target_contexts = [
                    item for item in target_contexts
                    if _matches_context(workload.constraints.context, item)
                ]
                if not target_contexts:
                    continue
            context = target_contexts[0] if len(target_contexts) == 1 else None
            state, unresolved, evidence = _evaluate_scheduler(
                workload, inventory, scheduler, target, context
            )
            result.append(
                PlanCandidate(
                    backend=scheduler.family, access_target_id=access.id,
                    execution_target_id=None if target is None else target.id,
                    execution_context_id=None if context is None else context.id,
                    scheduler_id=scheduler.id, compatibility_state=state,
                    unresolved_conditions=unresolved, decision_evidence=evidence,
                )
            )
    return result


def _evaluate_direct(
    workload: WorkloadSpec,
    inventory: InventorySnapshot,
    target: DiscoveredTarget,
    context: DiscoveredExecutionContext | None,
) -> tuple[CompatibilityState, list[str], list[DecisionEvidence]]:
    unresolved: list[str] = []
    evidence = [
        DecisionEvidence(
            state="observed", subject="target",
            message="current Bourne host is directly accessible", subject_id=target.id,
        )
    ]
    incompatible = False
    executable_state = _executable_state(workload, inventory.capabilities, context)
    if executable_state == "observed":
        evidence.append(
            DecisionEvidence(
                state="observed", subject="executable",
                message="requested executable is available in the current context",
            )
        )
    elif executable_state == "missing":
        incompatible = True
        evidence.append(
            DecisionEvidence(
                state="observed", subject="executable",
                message="requested executable is not available in the current context",
            )
        )
    else:
        unresolved.append("current executable availability is unknown")
        evidence.append(
            DecisionEvidence(
                state=executable_state, subject="executable",
                message="current executable availability is not established",
            )
        )

    requested = workload.resources
    if requested.gpus is not None and requested.gpus > 0:
        observed_gpus = _direct_gpu_count(target)
        if observed_gpus is None:
            unresolved.append("current GPU count is unknown")
        elif observed_gpus < requested.gpus:
            incompatible = True
            evidence.append(
                DecisionEvidence(
                    state="observed", subject="resources.gpus",
                    message=f"observed {observed_gpus} GPU(s), requested {requested.gpus}",
                )
            )
        else:
            evidence.append(
                DecisionEvidence(
                    state="observed", subject="resources.gpus",
                    message=f"observed {observed_gpus} GPU(s), requested {requested.gpus}",
                )
            )
    if requested.nodes not in (None, 1):
        incompatible = True
        evidence.append(
            DecisionEvidence(
                state="observed", subject="resources.nodes",
                message="direct execution represents one observed host",
            )
        )
    if requested.cpus is not None and requested.cpus > 1:
        unresolved.append("available direct CPU count was not recorded")
    if requested.memory_bytes is not None:
        unresolved.append("available direct memory was not recorded")
    if requested.mpi_ranks is not None and requested.mpi_ranks > 1:
        if workload.launcher_requirement is None or workload.launcher_requirement.name is None:
            unresolved.append("MPI rank count has no explicit launcher")
    if incompatible:
        return "incompatible", unresolved, evidence
    return ("partial" if unresolved else "compatible"), unresolved, evidence


def _evaluate_scheduler(
    workload: WorkloadSpec,
    inventory: InventorySnapshot,
    scheduler: SchedulerResource,
    target: DiscoveredTarget | None,
    context: DiscoveredExecutionContext | None,
) -> tuple[CompatibilityState, list[str], list[DecisionEvidence]]:
    unresolved = [
        "compute-node executable availability is unknown",
        "staging visibility from the compute allocation is unknown",
    ]
    evidence = [
        DecisionEvidence(
            state="observed", subject="scheduler",
            message=f"{scheduler.family} scheduler was observed",
            subject_id=scheduler.id,
        )
    ]
    incompatible = False
    if target is None:
        unresolved.append("scheduler execution target class is unknown")
    else:
        evidence.append(
            DecisionEvidence(
                state="observed", subject="target",
                message=f"scheduler target '{target.name}' is visible",
                subject_id=target.id,
            )
        )
        if target.authorization == "unknown":
            unresolved.append("scheduler authorization is unknown")
        if target.visible is False:
            incompatible = True
        incompatible = _evaluate_scheduler_resources(
            workload, scheduler, target, unresolved, evidence
        ) or incompatible
    if workload.resources.mpi_ranks is not None and workload.resources.mpi_ranks > 1:
        if workload.launcher_requirement is None or workload.launcher_requirement.name is None:
            unresolved.append("MPI rank count has no explicit launcher")
    if context is None:
        unresolved.append("compute execution context is unknown before allocation")
    if incompatible:
        return "incompatible", _deduplicate(unresolved), evidence
    return "partial", _deduplicate(unresolved), evidence


def _evaluate_scheduler_resources(
    workload: WorkloadSpec,
    scheduler: SchedulerResource,
    target: DiscoveredTarget,
    unresolved: list[str],
    evidence: list[DecisionEvidence],
) -> bool:
    requested = workload.resources
    metadata = target.metadata
    incompatible = False
    if requested.nodes is not None:
        raw_nodes = metadata.get("visible_nodes")
        available = _leading_int(raw_nodes)
        if available is None:
            unresolved.append("scheduler node capacity is unknown")
        elif requested.nodes > available:
            incompatible = True
            evidence.append(
                DecisionEvidence(
                    state="observed", subject="resources.nodes",
                    message=f"target reports {available} visible node(s), requested {requested.nodes}",
                    subject_id=target.id,
                )
            )
    if requested.cpus is not None:
        cpu_key = "cpus_per_node" if scheduler.family == "slurm" else "resources_max.ncpus"
        available = _leading_int(metadata.get(cpu_key))
        if available is None:
            unresolved.append("scheduler CPU capacity is unknown")
        elif requested.cpus > available * (requested.nodes or 1):
            incompatible = True
            evidence.append(
                DecisionEvidence(
                    state="observed", subject="resources.cpus",
                    message=f"target reports {available} CPU(s) per unit, requested {requested.cpus}",
                    subject_id=target.id,
                )
            )
    if requested.gpus is not None and requested.gpus > 0:
        available = _slurm_gpu_count(metadata.get("generic_resources"))
        if scheduler.family != "slurm" or available is None:
            unresolved.append("scheduler GPU capacity is unknown")
        elif requested.gpus > available * (requested.nodes or 1):
            incompatible = True
            evidence.append(
                DecisionEvidence(
                    state="observed", subject="resources.gpus",
                    message=f"target reports {available} GPU(s) per node, requested {requested.gpus}",
                    subject_id=target.id,
                )
            )
    return incompatible


def _executable_state(
    workload: WorkloadSpec,
    capabilities: Iterable[Capability],
    context: DiscoveredExecutionContext | None,
) -> str:
    requested = workload.executable
    if os.sep in requested or (os.altsep is not None and os.altsep in requested):
        path = Path(requested)
        if not path.is_absolute():
            path = Path(workload.working_directory) / path
        try:
            if path.is_file() and os.access(path, os.X_OK):
                return "observed"
            return "missing"
        except OSError:
            return "unknown"
    if context is None:
        return "unknown"
    name = Path(requested).name
    matching = [
        item for item in capabilities
        if item.context_id == context.id and item.kind == "executable" and item.name == name
    ]
    if any(item.observation_state != "historical" for item in matching):
        return "observed"
    if matching:
        return "historical"
    return "missing"


def _direct_gpu_count(target: DiscoveredTarget) -> int | None:
    system = target.metadata.get("system")
    if not isinstance(system, dict):
        return None
    gpus = system.get("gpus")
    return len(gpus) if isinstance(gpus, list) else None


def _matches_target(reference: str | None, target: DiscoveredTarget | None) -> bool:
    if reference is None:
        return True
    if target is None:
        return False
    return reference in {target.id, target.name, target.locator}


def _matches_context(
    reference: str | None, context: DiscoveredExecutionContext | None
) -> bool:
    if reference is None:
        return True
    if context is None:
        return False
    return reference in {context.id, context.context_key, context.name, context.locator}


def _leading_int(value: object) -> int | None:
    if value is None:
        return None
    match = re.match(r"\s*(\d+)", str(value))
    return None if match is None else int(match.group(1))


def _slurm_gpu_count(value: object) -> int | None:
    if value is None or str(value).strip() in {"", "(null)", "N/A"}:
        return None
    counts = [int(item) for item in re.findall(r"(?:^|,)gpu(?::[^,:()]+)*:(\d+)", str(value))]
    return sum(counts) if counts else None


def _deduplicate(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _make_plan(
    workload: WorkloadSpec,
    inventory: InventorySnapshot,
    candidate: PlanCandidate,
) -> ExecutionPlan:
    return ExecutionPlan(
        id=new_ulid(), workload_id=workload.id,
        inventory_snapshot_id=inventory.id, backend=candidate.backend,
        access_target_id=candidate.access_target_id,
        execution_target_id=candidate.execution_target_id,
        execution_context_id=candidate.execution_context_id,
        scheduler_id=candidate.scheduler_id,
        requested_resources=workload.resources,
        executable=workload.executable, arguments=list(workload.arguments),
        working_directory=workload.working_directory,
        inputs=list(workload.inputs), outputs=list(workload.outputs),
        compatibility_state=candidate.compatibility_state,
        unresolved_conditions=list(candidate.unresolved_conditions),
        decision_evidence=list(candidate.decision_evidence), created_at=utc_now(),
    )
