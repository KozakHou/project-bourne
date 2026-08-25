"""Deterministic, bounded site-aware candidate planning."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import itertools
import math
import re
from typing import Any, Iterable, Sequence

from .constraint_providers import DeclarativeConstraintProvider
from .inventory_models import InventorySnapshot
from .planning_models import (
    CandidateExploration,
    CandidateReason,
    EnvironmentActivation,
    PlanningCandidate,
    ResolvedEnvironment,
    ResourceShape,
    canonical_digest,
)
from .site_models import SitePolicyClaim
from .workload_models import ResourceRequirements, WorkloadSpec

DEFAULT_CANDIDATE_LIMIT = 64


def resource_shapes_from_inventory(snapshot: InventorySnapshot) -> list[ResourceShape]:
    """Normalize observed target classes without inventing allocations or access."""

    shapes: list[ResourceShape] = []
    for target in snapshot.execution_targets:
        raw_shapes = target.metadata.get("resource_shapes")
        if raw_shapes is None:
            # Scheduler discovery describes bounded target classes rather than
            # concrete allocations. Preserve those partial facts as a shape,
            # but do not turn visible capacity/queue maxima into a request and
            # do not turn visibility into authorization.
            scheduler = target.metadata.get("scheduler")
            if scheduler not in {"slurm", "pbs", "lsf"}:
                continue
            normalized: dict[str, Any] = {"scheduler_class": target.name}
            if scheduler == "slurm":
                cpus_per_node = _positive_int(target.metadata.get("cpus_per_node"))
                if cpus_per_node is not None:
                    normalized["cpus_per_node"] = cpus_per_node
            raw_shapes = [normalized]
        if not isinstance(raw_shapes, list):
            continue
        for raw in raw_shapes[:DEFAULT_CANDIDATE_LIMIT]:
            if not isinstance(raw, dict):
                continue
            value = dict(raw)
            evidence = list(value.get("evidence", []))
            evidence.append(
                {
                    "kind": "observed_now",
                    "scope": "remote_login_scheduler_surface",
                    "target_id": target.id,
                    "visibility": target.visible,
                    "authorization": target.authorization,
                }
            )
            value["evidence"] = evidence
            try:
                shapes.append(ResourceShape.from_dict(value))
            except (TypeError, ValueError):
                continue
    return sorted(shapes, key=lambda item: item.identity)


def generate_resource_shapes(
    snapshot: InventorySnapshot,
    workload: WorkloadSpec,
    *,
    provider: DeclarativeConstraintProvider | None = None,
    policy_claims: Sequence[SitePolicyClaim] = (),
    limit: int = DEFAULT_CANDIDATE_LIMIT,
) -> list[ResourceShape]:
    """Generate bounded request shapes from observed capacity and explicit intent.

    Visible node counts are deliberately absent from the generation inputs: they
    describe a discovery surface, not authorization or a permitted allocation.
    """

    if limit < 1 or limit > DEFAULT_CANDIDATE_LIMIT:
        raise ValueError(f"shape limit must be between 1 and {DEFAULT_CANDIDATE_LIMIT}")
    hints = {} if provider is None else provider.resource_value_hints(limit)
    per_target: list[list[ResourceShape]] = []
    for target in sorted(snapshot.execution_targets, key=lambda item: item.id):
        raw_shapes = target.metadata.get("resource_shapes")
        generated: list[ResourceShape] = []
        if isinstance(raw_shapes, list):
            for raw in raw_shapes[:limit]:
                if not isinstance(raw, dict):
                    continue
                try:
                    provisional = ResourceShape.from_dict(raw)
                    generated.append(
                        _shape_with_target_evidence(
                            provisional, target, policy_claims
                        )
                    )
                except (TypeError, ValueError):
                    continue
        elif target.metadata.get("scheduler") in {"slurm", "pbs", "lsf"}:
            generated.extend(
                _generated_target_shapes(target, workload, hints, policy_claims, limit)
            )
        if generated:
            by_identity = {shape.identity: shape for shape in generated}
            per_target.append([by_identity[key] for key in sorted(by_identity)])

    # Deterministic round-robin prevents one scheduler target from consuming the
    # entire shape budget.
    shapes: list[ResourceShape] = []
    for layer in itertools.zip_longest(*per_target):
        for shape in layer:
            if shape is not None:
                shapes.append(shape)
                if len(shapes) == limit:
                    return shapes
    return shapes


def policy_applies(claim: SitePolicyClaim, shape: ResourceShape) -> bool:
    """Return whether a typed policy scope applies to a candidate shape."""

    scope = claim.applicability.scope
    value = claim.applicability.value
    if scope == "global":
        return True
    if scope in {"scheduler_class", "queue", "partition"}:
        return shape.scheduler_class == value
    if scope == "node_class":
        return shape.node_class == value
    if scope == "account":
        return shape.placement.get("account") == value
    return False


def _generated_target_shapes(
    target: Any,
    workload: WorkloadSpec,
    hints: dict[str, tuple[int, ...]],
    policy_claims: Sequence[SitePolicyClaim],
    limit: int,
) -> list[ResourceShape]:
    metadata = target.metadata
    scheduler = metadata["scheduler"]
    requested = workload.resources
    rank_values: tuple[int | None, ...] = _optional_dimension_values(
        requested.mpi_ranks, hints.get("mpi_ranks")
    )
    hinted_cpus = hints.get("total_cpus")
    generated: list[ResourceShape] = []
    capacity = _target_capacity(metadata, scheduler)
    max_nodes = _applicable_hard_max_nodes(target, policy_claims)
    for mpi_ranks in rank_values:
        if requested.cpus is not None:
            cpu_values = (requested.cpus,)
        elif hinted_cpus:
            cpu_values = hinted_cpus
        elif mpi_ranks is not None:
            cpu_values = (mpi_ranks,)
        else:
            cpu_values = (None,)
        for provisional_cpus in cpu_values:
            provider_nodes = _provider_layout_nodes(
                total_cpus=provisional_cpus,
                mpi_ranks=mpi_ranks,
                hinted_nodes=hints.get("nodes"),
                hinted_cpus_per_node=hints.get("cpus_per_node"),
                hinted_ranks_per_node=hints.get("ranks_per_node"),
            )
            node_values = _derived_node_values(
                requested_nodes=requested.nodes,
                hinted_nodes=provider_nodes,
                total_cpus=provisional_cpus,
                mpi_ranks=mpi_ranks,
                gpus=requested.gpus,
                memory_bytes=requested.memory_bytes,
                capacity=capacity,
                hard_max_nodes=max_nodes,
                limit=limit,
                preserve_unknown_topology=scheduler == "lsf",
            )
            for nodes in node_values:
                total_cpus = (
                    provisional_cpus
                    if provisional_cpus is not None or nodes is None
                    else nodes
                )
                if (
                    nodes is not None
                    and (
                        total_cpus % nodes
                        or (mpi_ranks is not None and mpi_ranks % nodes)
                    )
                ):
                    continue
                if (
                    mpi_ranks is not None
                    and total_cpus is not None
                    and total_cpus % mpi_ranks
                ):
                    continue
                gpus = requested.gpus
                if nodes is not None and gpus is not None and gpus % nodes:
                    continue
                memory = requested.memory_bytes
                if nodes is not None and memory is not None and memory % nodes:
                    continue
                shape = ResourceShape(
                    nodes=nodes,
                    cpus_per_node=(
                        None if nodes is None or total_cpus is None
                        else total_cpus // nodes
                    ),
                    total_cpus=total_cpus,
                    mpi_ranks=mpi_ranks,
                    ranks_per_node=(
                        None if nodes is None or mpi_ranks is None
                        else mpi_ranks // nodes
                    ),
                    threads_per_rank=(
                        None if mpi_ranks is None or total_cpus is None
                        else total_cpus // mpi_ranks
                    ),
                    gpus=gpus,
                    gpus_per_node=(
                        None if nodes is None or gpus is None else gpus // nodes
                    ),
                    memory_bytes=memory,
                    memory_per_node_bytes=(
                        None if nodes is None or memory is None else memory // nodes
                    ),
                    architecture=_bounded_metadata_string(metadata.get("architecture")),
                    node_class=_bounded_metadata_string(metadata.get("node_class")),
                    walltime_seconds=requested.walltime_seconds,
                    scheduler_class=target.name,
                    placement=_target_placement(target, scheduler),
                    evidence=[],
                )
                if not _within_observed_capacity(shape, capacity):
                    continue
                capacity_evidence: dict[str, Any] = {
                    "kind": "observed_now",
                    "scope": "scheduler_target_capacity",
                    "target_id": target.id,
                    "capacity": capacity,
                    "topology": "unknown" if nodes is None else "derived",
                }
                if provider_nodes and nodes in provider_nodes:
                    capacity_evidence["provider_layout_hints"] = {
                        key: list(hints[key])
                        for key in ("nodes", "cpus_per_node", "ranks_per_node")
                        if key in hints
                    }
                shape = ResourceShape.from_dict(
                    {**shape.to_dict(), "evidence": [capacity_evidence]}
                )
                generated.append(
                    _shape_with_target_evidence(shape, target, policy_claims)
                )
                if len(generated) == limit:
                    return generated
    return generated


def _derived_node_values(
    *,
    requested_nodes: int | None,
    hinted_nodes: tuple[int, ...] | None,
    total_cpus: int | None,
    mpi_ranks: int | None,
    gpus: int | None,
    memory_bytes: int | None,
    capacity: dict[str, int],
    hard_max_nodes: int | None,
    limit: int,
    preserve_unknown_topology: bool = False,
) -> tuple[int | None, ...]:
    """Derive exact, capacity-feasible allocation widths without guessing access."""

    # Explicit request intent is authoritative. Policy evaluation can reject it,
    # but generation must not silently replace it with automatic alternatives.
    if hard_max_nodes is not None and hard_max_nodes < 1:
        return ()
    if requested_nodes is not None:
        return (requested_nodes,)
    if hinted_nodes:
        return tuple(
            value
            for value in sorted(set(hinted_nodes))
            if hard_max_nodes is None or value <= hard_max_nodes
        )[:limit]

    per_node_bounds = [
        (total_cpus, capacity.get("cpus_per_node")),
        (gpus, capacity.get("gpus_per_node")),
        (memory_bytes, capacity.get("memory_per_node_bytes")),
    ]
    known_bounds = [
        math.ceil(total / per_node)
        for total, per_node in per_node_bounds
        if total is not None and per_node is not None
    ]
    aggregate_values = [
        value for value in (total_cpus, mpi_ranks, gpus)
        if value is not None and value > 0
    ]
    if not known_bounds or not aggregate_values:
        if preserve_unknown_topology:
            return (None,)
        return (1,) if hard_max_nodes is None or hard_max_nodes >= 1 else ()

    minimum_nodes = max(1, *known_bounds)
    common_divisor = math.gcd(*aggregate_values)
    values = [
        nodes
        for nodes in _positive_divisors(common_divisor)
        if nodes >= minimum_nodes
        and (hard_max_nodes is None or nodes <= hard_max_nodes)
        and (memory_bytes is None or memory_bytes % nodes == 0)
    ]
    return tuple(values[:limit])


def _provider_layout_nodes(
    *,
    total_cpus: int | None,
    mpi_ranks: int | None,
    hinted_nodes: tuple[int, ...] | None,
    hinted_cpus_per_node: tuple[int, ...] | None,
    hinted_ranks_per_node: tuple[int, ...] | None,
) -> tuple[int, ...] | None:
    """Derive only node counts directly justified by provider layout hints."""

    values = set(hinted_nodes or ())
    if total_cpus is not None:
        values.update(
            total_cpus // per_node
            for per_node in hinted_cpus_per_node or ()
            if per_node > 0 and total_cpus % per_node == 0
        )
    if mpi_ranks is not None:
        values.update(
            mpi_ranks // per_node
            for per_node in hinted_ranks_per_node or ()
            if per_node > 0 and mpi_ranks % per_node == 0
        )
    return tuple(sorted(value for value in values if value > 0)) or None


def _positive_divisors(value: int) -> tuple[int, ...]:
    lower: list[int] = []
    upper: list[int] = []
    for candidate in range(1, math.isqrt(value) + 1):
        if value % candidate:
            continue
        lower.append(candidate)
        paired = value // candidate
        if paired != candidate:
            upper.append(paired)
    return tuple(lower + list(reversed(upper)))


def _applicable_hard_max_nodes(
    target: Any, claims: Sequence[SitePolicyClaim]
) -> int | None:
    scheduler = target.metadata["scheduler"]
    target_shape = ResourceShape(
        scheduler_class=target.name,
        node_class=_bounded_metadata_string(target.metadata.get("node_class")),
        placement=_target_placement(target, scheduler),
    )
    applicable = [
        claim.value
        for claim in claims
        if claim.is_hard
        and claim.property == "max_nodes"
        and policy_applies(claim, target_shape)
    ]
    if not applicable or any(
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        for value in applicable
    ):
        return None
    normalized = {math.floor(value) for value in applicable}
    if len(normalized) != 1:
        return None
    return normalized.pop()


def _target_placement(target: Any, scheduler: str) -> dict[str, str]:
    placement = {"target_id": target.id, "scheduler": scheduler}
    account = _bounded_metadata_string(target.metadata.get("account"))
    if account is not None:
        placement["account"] = account
    return placement


def _shape_with_target_evidence(
    shape: ResourceShape,
    target: Any,
    policy_claims: Sequence[SitePolicyClaim],
) -> ResourceShape:
    evidence = list(shape.evidence)
    evidence.append(
        {
            "kind": "observed_now",
            "scope": "remote_login_scheduler_surface",
            "target_id": target.id,
            "visibility": target.visible,
            "authorization": target.authorization,
        }
    )
    provisional = ResourceShape.from_dict({**shape.to_dict(), "evidence": evidence})
    for claim in policy_claims:
        if (
            claim.property in {"authorization", "authorized"}
            and claim.evidence_kind in {"site_declared", "user_declared"}
            and policy_applies(claim, provisional)
        ):
            authorization = _authorization_claim_value(claim.value)
            if authorization is not None:
                evidence.append(
                    {
                        "kind": claim.evidence_kind,
                        "scope": "authorization_claim",
                        "claim_id": claim.id,
                        "authorization": authorization,
                    }
                )
    return ResourceShape.from_dict({**shape.to_dict(), "evidence": evidence})


def _authorization_claim_value(value: Any) -> str | None:
    if value is True or (
        isinstance(value, str)
        and value in {"authorized", "user-declared-authorized"}
    ):
        return "user-declared-authorized"
    if value is False or (
        isinstance(value, str)
        and value in {"denied", "unauthorized", "observed-unauthorized"}
    ):
        return "unauthorized"
    return None


def _optional_dimension_values(
    requested: int | None, hinted: tuple[int, ...] | None
) -> tuple[int | None, ...]:
    if requested is not None:
        return (requested,)
    return hinted or (None,)


def _target_capacity(metadata: dict[str, Any], scheduler: str) -> dict[str, int]:
    capacity: dict[str, int] = {}
    if scheduler == "slurm":
        for source, destination in (
            ("cpus_per_node", "cpus_per_node"),
            ("memory_per_node", "memory_per_node_bytes"),
        ):
            value = _positive_int(metadata.get(source))
            if value is not None:
                capacity[destination] = (
                    value * 1024 * 1024 if source == "memory_per_node" else value
                )
        gpu_count = _gpu_count(metadata.get("generic_resources"))
        if gpu_count is not None:
            capacity["gpus_per_node"] = gpu_count
        walltime = _duration_seconds(metadata.get("wall_time_limit"))
        if walltime is not None:
            capacity["walltime_seconds"] = walltime
    else:
        ncpus = _positive_int(metadata.get("resources_max.ncpus"))
        if ncpus is not None:
            capacity["total_cpus"] = ncpus
        memory = _memory_bytes(metadata.get("resources_max.mem"))
        if memory is not None:
            capacity["memory_bytes"] = memory
        walltime = _duration_seconds(metadata.get("resources_max.walltime"))
        if walltime is not None:
            capacity["walltime_seconds"] = walltime
    return capacity


def _within_observed_capacity(shape: ResourceShape, capacity: dict[str, int]) -> bool:
    return all(
        getattr(shape, name) is None or getattr(shape, name) <= maximum
        for name, maximum in capacity.items()
    )


def _gpu_count(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    counts = [int(item) for item in re.findall(r"(?:^|,)gpu(?::[^,:]+)*:(\d+)", value)]
    return max(counts) if counts else None


def _memory_bytes(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\s*(\d+)\s*([kmgt]?)(?:i?b)?\s*", value, re.IGNORECASE)
    if match is None:
        return None
    power = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4}[match.group(2).lower()]
    return int(match.group(1)) * (1024 ** power)


def _duration_seconds(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    if not isinstance(value, str) or value.lower() in {"infinite", "unlimited", "--"}:
        return None
    match = re.fullmatch(r"(?:(\d+)-)?(\d+):(\d+)(?::(\d+))?", value.strip())
    if match is None:
        return None
    days = int(match.group(1) or 0)
    if match.group(4) is None:
        hours, minutes, seconds = 0, int(match.group(2)), int(match.group(3))
    else:
        hours, minutes, seconds = map(int, match.group(2, 3, 4))
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _bounded_metadata_string(value: Any) -> str | None:
    return value if isinstance(value, str) and 0 < len(value) <= 256 else None


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdigit():
        parsed = int(value)
        return parsed if parsed > 0 else None
    return None


def resolve_environments(
    snapshot: InventorySnapshot,
    requirements: Sequence[dict[str, Any]],
) -> list[ResolvedEnvironment]:
    """Resolve only environments and capabilities observed in this snapshot."""

    capabilities_by_context: dict[str, list[Any]] = {}
    for capability in snapshot.capabilities:
        capabilities_by_context.setdefault(capability.context_id, []).append(capability)
    resolved: list[ResolvedEnvironment] = []
    for context in sorted(snapshot.execution_contexts, key=lambda item: item.context_key):
        capabilities = capabilities_by_context.get(context.id, [])
        missing: list[str] = []
        evidence: list[dict[str, Any]] = []
        for requirement in requirements:
            matching = [
                item for item in capabilities
                if item.name == requirement["name"]
                and (
                    requirement.get("kind") in {"any", item.kind}
                    or requirement.get("kind") in item.classifications
                )
                and item.observation_state == "observed"
            ]
            if not matching and requirement.get("required", True):
                missing.append(
                    f"required {requirement.get('kind')} '{requirement['name']}' is not observed"
                )
            evidence.extend(
                {
                    "kind": "observed_now",
                    "capability_id": item.id,
                    "context_id": context.id,
                }
                for item in matching
            )
        activation_value = context.metadata.get("activation", {"kind": "none"})
        try:
            activation = EnvironmentActivation.from_dict(activation_value)
        except (TypeError, ValueError) as exc:
            missing.append(f"environment activation is invalid: {exc}")
            activation = EnvironmentActivation()
        state = "compatible" if not missing else "unresolved"
        resolved.append(
            ResolvedEnvironment(
                context_id=context.id, name=context.name, kind=context.kind,
                state=state, activation=activation, evidence=evidence,
                unresolved=missing,
            )
        )
    return resolved


def explore_candidates(
    workload: WorkloadSpec,
    resource_shapes: Sequence[ResourceShape],
    environments: Sequence[ResolvedEnvironment],
    *,
    provider: DeclarativeConstraintProvider | None = None,
    policy_claims: Sequence[SitePolicyClaim] = (),
    limit: int = DEFAULT_CANDIDATE_LIMIT,
) -> CandidateExploration:
    """Combine inputs with hard pruning and fair bounded group enumeration."""

    if limit < 1 or limit > DEFAULT_CANDIDATE_LIMIT:
        raise ValueError(f"candidate limit must be between 1 and {DEFAULT_CANDIDATE_LIMIT}")
    shapes = sorted(resource_shapes, key=lambda item: item.identity)
    envs: list[ResolvedEnvironment | None] = sorted(
        environments, key=lambda item: (item.context_id, item.name)
    ) or [None]
    if provider is None:
        assignments, assignment_count = ([{}], 1)
    else:
        assignments, assignment_count = provider.parameter_assignments(limit)
    theoretical = len(shapes) * len(envs) * assignment_count
    candidates: list[PlanningCandidate] = []
    groups = [(shape, environment) for shape in shapes for environment in envs]
    expandable: list[
        tuple[
            ResourceShape,
            ResolvedEnvironment | None,
            list[dict[str, Any]],
        ]
    ] = []
    hard_groups: list[tuple[ResourceShape, ResolvedEnvironment | None]] = []
    hard_pruned = 0
    explored_groups: set[tuple[str, str | None]] = set()

    for shape, environment in groups:
        if _group_is_hard_invalid(
            workload, shape, environment, provider, policy_claims
        ):
            hard_groups.append((shape, environment))
            hard_pruned += assignment_count
            explored_groups.add(
                (shape.identity, None if environment is None else environment.context_id)
            )
        else:
            expandable.append(
                (
                    shape,
                    environment,
                    _assignments_by_constraint_feasibility(
                        provider, assignments, shape
                    ),
                )
            )

    # One assignment layer per group gives every viable shape/environment group
    # an opportunity before any group receives its next combination.
    for assignment_index in range(len(assignments)):
        for shape, environment, group_assignments in expandable:
            if len(candidates) >= limit:
                break
            candidates.append(
                _evaluate_candidate(
                    workload, shape, environment, group_assignments[assignment_index],
                    provider, policy_claims,
                )
            )
            explored_groups.add(
                (shape.identity, None if environment is None else environment.context_id)
            )
        if len(candidates) >= limit:
            break

    # Keep a bounded diagnostic representative for cheaply-pruned groups only
    # after expandable groups have received fair exploration.
    representative = assignments[0] if assignments else {}
    for shape, environment in hard_groups:
        if len(candidates) >= limit:
            break
        candidates.append(
            _evaluate_candidate(
                workload, shape, environment, representative, provider, policy_claims
            )
        )
        hard_pruned -= 1

    hard_invalid = sum(
        item.state in {"hard_invalid", "policy_incompatible"} for item in candidates
    )
    viable = sum(item.state == "viable" for item in candidates)
    considered = len(candidates) + hard_pruned
    truncated = theoretical > considered
    if truncated:
        coverage = (
            f"explored a bounded subset: materialized {len(candidates)} and "
            f"hard-pruned {hard_pruned} of {theoretical} deterministic combinations "
            f"across {len(explored_groups)} of {len(groups)} shape/environment groups; "
            "viable and rejection counts describe only that explored subset and do not "
            "establish global absence of viable candidates"
        )
    else:
        coverage = (
            f"explored all {theoretical} deterministic combinations: materialized "
            f"{len(candidates)} and hard-pruned {hard_pruned} across {len(groups)} "
            "shape/environment groups"
        )
    return CandidateExploration(
        candidates=candidates, generated_count=len(candidates),
        theoretical_count=theoretical, hard_invalid_count=hard_invalid,
        viable_count=viable, truncated=truncated, coverage=coverage,
        hard_pruned_count=hard_pruned,
        explored_group_count=len(explored_groups), total_group_count=len(groups),
    )


def _assignments_by_constraint_feasibility(
    provider: DeclarativeConstraintProvider | None,
    assignments: list[dict[str, Any]],
    shape: ResourceShape,
) -> list[dict[str, Any]]:
    if provider is None:
        return assignments

    def priority(item: tuple[int, dict[str, Any]]) -> tuple[int, int]:
        index, parameters = item
        evaluations = provider.evaluate(parameters, shape)
        if any(result.state == "violated" and result.hard for result in evaluations):
            return (2, index)
        if any(result.state == "unknown" for result in evaluations):
            return (1, index)
        return (0, index)

    return [
        parameters
        for _, parameters in sorted(enumerate(assignments), key=priority)
    ]


def _group_is_hard_invalid(
    workload: WorkloadSpec,
    shape: ResourceShape,
    environment: ResolvedEnvironment | None,
    provider: DeclarativeConstraintProvider | None,
    policy_claims: Sequence[SitePolicyClaim],
) -> bool:
    _, authorization_state = _authorization(shape)
    if authorization_state == "hard_invalid":
        return True
    if any(
        item.code == "resource_incompatible"
        for item in _resource_requirements(workload.resources, shape)
    ):
        return True
    _, policy_state = _policy(shape, policy_claims)
    if policy_state == "policy_incompatible":
        return True
    if provider is not None:
        # Parameter-dependent constraints remain unknown here. A hard violation
        # at this stage is therefore shape-only and safely prunes the full group.
        if any(
            item.state == "violated" and item.hard
            for item in provider.evaluate({}, shape)
        ):
            return True
    return False


def _evaluate_candidate(
    workload: WorkloadSpec,
    shape: ResourceShape,
    environment: ResolvedEnvironment | None,
    parameters: dict[str, Any],
    provider: DeclarativeConstraintProvider | None,
    policy_claims: Sequence[SitePolicyClaim],
) -> PlanningCandidate:
    reasons: list[CandidateReason] = []
    unresolved: list[str] = []
    state = "viable"
    authorization_reasons, authorization_state = _authorization(shape)
    reasons.extend(authorization_reasons)
    if authorization_state == "hard_invalid":
        state = "hard_invalid"
    elif authorization_state == "unresolved":
        state = "unresolved"
        unresolved.extend(item.message for item in authorization_reasons)
    requirement_reasons = _resource_requirements(workload.resources, shape)
    if shape.placement.get("scheduler") == "lsf":
        for name, requested in (
            ("memory", workload.resources.memory_bytes),
            ("GPU", workload.resources.gpus),
        ):
            if requested not in {None, 0}:
                requirement_reasons.append(
                    CandidateReason(
                        "resource_unknown",
                        f"LSF {name} request mapping is unresolved for this site",
                        "unknown",
                    )
                )
    reasons.extend(requirement_reasons)
    if any(item.code == "resource_incompatible" for item in requirement_reasons):
        state = "hard_invalid"
    elif any(item.code == "resource_unknown" for item in requirement_reasons):
        if state == "viable":
            state = "unresolved"
        unresolved.extend(
            item.message for item in requirement_reasons
            if item.code == "resource_unknown"
        )

    policy_reasons, policy_state = _policy(shape, policy_claims)
    reasons.extend(policy_reasons)
    if policy_state == "policy_incompatible" and state != "hard_invalid":
        state = policy_state
    elif policy_state == "unresolved":
        if state == "viable":
            state = "unresolved"
        unresolved.extend(
            item.message for item in policy_reasons
            if item.code == "policy_unknown"
        )

    if environment is None:
        if provider is not None and provider.environment_requirements:
            state = "unresolved" if state == "viable" else state
            unresolved.append("no existing environment was resolved")
    elif not environment.is_compatible:
        state = "unresolved" if state == "viable" else state
        unresolved.extend(environment.unresolved)

    if provider is not None:
        for item in provider.evaluate(parameters, shape):
            if item.state == "violated" and item.hard:
                reasons.append(
                    CandidateReason(
                        "constraint_violated", item.message,
                        "provider_contract", (item.constraint_id,),
                    )
                )
                state = "hard_invalid"
            elif item.state == "unknown":
                reasons.append(
                    CandidateReason(
                        "constraint_unknown", item.message,
                        "unknown", (item.constraint_id,),
                    )
                )
                if state == "viable":
                    state = "unresolved"
                unresolved.append(item.message)

    candidate_value = {
        "resource_shape": shape.to_dict(),
        "environment": None if environment is None else environment.to_dict(),
        "parameters": parameters,
    }
    return PlanningCandidate(
        id=canonical_digest(candidate_value), resource_shape=shape,
        environment=environment, parameters=dict(parameters), state=state,
        reasons=reasons, unresolved=sorted(set(unresolved)),
    )


def rejection_reason_summary(
    candidates: Iterable[PlanningCandidate], limit: int = 32
) -> list[dict[str, Any]]:
    counts = Counter(
        (reason.code, reason.message, reason.evidence_kind)
        for candidate in candidates
        if candidate.state != "viable"
        for reason in candidate.reasons
    )
    return [
        {
            "code": code, "message": message, "evidence_kind": evidence_kind,
            "count": count,
        }
        for (code, message, evidence_kind), count in sorted(
            counts.items(), key=lambda item: (-item[1], item[0])
        )[:limit]
    ]


def _resource_requirements(
    requested: ResourceRequirements, shape: ResourceShape
) -> list[CandidateReason]:
    mapping = {
        "cpus": "total_cpus", "gpus": "gpus", "nodes": "nodes",
        "mpi_ranks": "mpi_ranks", "memory_bytes": "memory_bytes",
        "walltime_seconds": "walltime_seconds",
    }
    result: list[CandidateReason] = []
    for request_name, shape_name in mapping.items():
        required = getattr(requested, request_name)
        if required is None:
            continue
        available = getattr(shape, shape_name)
        if available is None:
            result.append(
                CandidateReason(
                    "resource_unknown",
                    f"{shape_name} is unknown for this resource shape",
                    "unknown",
                )
            )
        elif available < required:
            result.append(
                CandidateReason(
                    "resource_incompatible",
                    f"{shape_name} {available} is below requested {required}",
                    "observed_now",
                )
            )
    return result


def _authorization(
    shape: ResourceShape,
) -> tuple[list[CandidateReason], str]:
    values = [
        (item.get("authorization"), item.get("kind", "unknown"))
        for item in shape.evidence
        if isinstance(item, dict) and "authorization" in item
    ]
    denied = next(
        (
            (value, kind) for value, kind in values
            if value in {"denied", "unauthorized", "observed-unauthorized"}
        ),
        None,
    )
    if denied is not None:
        return [
            CandidateReason(
                "authorization_incompatible",
                "resource visibility does not grant authorization for this shape",
                denied[1],
            )
        ], "hard_invalid"
    if any(
        value in {
            "authorized", "observed-authorized", "user-declared-authorized"
        }
        for value, _ in values
    ):
        return [], "viable"
    return [
        CandidateReason(
            "authorization_unknown",
            "authorization for this visible resource shape is unknown",
            "unknown",
        )
    ], "unresolved"


_POLICY_PROPERTIES = {
    "max_nodes": "nodes",
    "max_total_cpus": "total_cpus",
    "max_gpus": "gpus",
    "max_walltime_seconds": "walltime_seconds",
}


def _policy(
    shape: ResourceShape, claims: Sequence[SitePolicyClaim]
) -> tuple[list[CandidateReason], str]:
    result: list[CandidateReason] = []
    state = "viable"
    grouped: dict[str, list[SitePolicyClaim]] = {}
    for claim in claims:
        if (
            claim.is_hard
            and claim.property in _POLICY_PROPERTIES
            and policy_applies(claim, shape)
        ):
            grouped.setdefault(claim.property, []).append(claim)
    for property_name, items in grouped.items():
        invalid = [
            item for item in items
            if not isinstance(item.value, (int, float)) or isinstance(item.value, bool)
        ]
        if invalid:
            result.extend(
                CandidateReason(
                    "policy_unknown", f"hard policy {property_name} is not numeric",
                    item.evidence_kind, (item.id,),
                )
                for item in invalid
            )
            if state == "viable":
                state = "unresolved"
            continue
        raw_values = [item.value for item in items]
        dimension = _POLICY_PROPERTIES[property_name]
        requested = getattr(shape, dimension)
        if requested is None:
            result.extend(
                CandidateReason(
                    "policy_unknown", f"{dimension} is unknown under hard policy",
                    item.evidence_kind, (item.id,),
                )
                for item in items
            )
            if state == "viable":
                state = "unresolved"
            continue
        if requested > min(raw_values):
            state = "policy_incompatible"
            result.extend(
                CandidateReason(
                    "policy_incompatible",
                    f"{dimension} {requested} exceeds applicable hard {property_name}={item.value}",
                    item.evidence_kind, (item.id,),
                )
                for item in items
                if requested > item.value
            )
        elif len(set(raw_values)) > 1:
            result.extend(
                CandidateReason(
                    "policy_conflict_preserved",
                    f"true {property_name} remains unknown/conflicted; this shape satisfies every current hard interpretation",
                    item.evidence_kind, (item.id,),
                )
                for item in items
            )
    return result, state
