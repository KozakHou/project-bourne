"""Deterministic, bounded site-aware candidate planning."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict
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
            # Slurm/PBS discovery describes bounded target classes rather than
            # concrete allocations. Preserve those partial facts as a shape,
            # but do not turn visible capacity/queue maxima into a request and
            # do not turn visibility into authorization.
            scheduler = target.metadata.get("scheduler")
            if scheduler not in {"slurm", "pbs"}:
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
    """Combine shapes, environments, policy, and typed constraints under a hard cap."""

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

    for shape in shapes:
        for environment in envs:
            for parameters in assignments:
                if len(candidates) >= limit:
                    break
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
                if policy_state == "policy_incompatible":
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
                    evaluations = provider.evaluate(parameters, shape)
                    for item in evaluations:
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
                candidates.append(
                    PlanningCandidate(
                        id=canonical_digest(candidate_value),
                        resource_shape=shape,
                        environment=environment,
                        parameters=dict(parameters),
                        state=state,
                        reasons=reasons,
                        unresolved=sorted(set(unresolved)),
                    )
                )
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break

    hard_invalid = sum(
        item.state in {"hard_invalid", "policy_incompatible"} for item in candidates
    )
    viable = sum(item.state == "viable" for item in candidates)
    truncated = theoretical > len(candidates)
    coverage = (
        f"materialized {len(candidates)} of {theoretical} deterministic combinations"
        if truncated
        else f"materialized all {len(candidates)} deterministic combinations"
    )
    return CandidateExploration(
        candidates=candidates, generated_count=len(candidates),
        theoretical_count=theoretical, hard_invalid_count=hard_invalid,
        viable_count=viable, truncated=truncated, coverage=coverage,
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
        item.get("authorization")
        for item in shape.evidence
        if isinstance(item, dict) and "authorization" in item
    ]
    if any(value in {"denied", "unauthorized", "observed-unauthorized"} for value in values):
        return [
            CandidateReason(
                "authorization_incompatible",
                "resource visibility does not grant authorization for this shape",
                "observed_now",
            )
        ], "hard_invalid"
    if any(
        value in {
            "authorized", "observed-authorized", "user-declared-authorized"
        }
        for value in values
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
        if claim.is_hard and claim.property in _POLICY_PROPERTIES:
            grouped.setdefault(claim.property, []).append(claim)
    for property_name, items in grouped.items():
        raw_values = [item.value for item in items]
        if not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in raw_values):
            result.append(
                CandidateReason(
                    "policy_unknown", f"hard policy {property_name} is not numeric",
                    "unknown", tuple(item.id for item in items),
                )
            )
            if state == "viable":
                state = "unresolved"
            continue
        dimension = _POLICY_PROPERTIES[property_name]
        requested = getattr(shape, dimension)
        if requested is None:
            result.append(
                CandidateReason(
                    "policy_unknown", f"{dimension} is unknown under hard policy",
                    "unknown", tuple(item.id for item in items),
                )
            )
            if state == "viable":
                state = "unresolved"
            continue
        if requested > min(raw_values):
            state = "policy_incompatible"
            result.append(
                CandidateReason(
                    "policy_incompatible",
                    f"{dimension} {requested} is not justified under every credible hard {property_name} interpretation",
                    "site_declared", tuple(item.id for item in items),
                )
            )
        elif len(set(raw_values)) > 1:
            result.append(
                CandidateReason(
                    "policy_conflict_preserved",
                    f"true {property_name} remains unknown/conflicted; this shape satisfies every current hard interpretation",
                    "site_declared", tuple(item.id for item in items),
                )
            )
    return result, state
