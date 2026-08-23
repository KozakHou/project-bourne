"""Structured site discovery and candidate-planning services for CLI/MCP adapters."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from .constraint_providers import DeclarativeConstraintProvider
from .discovery import discover_site
from .execution_request import ExecutionRequest, RequestArtifacts, RequestSource
from .execution_service import request_to_workload
from .ids import new_ulid
from .inventory_models import InventorySnapshot
from .inventory_storage import InventoryStore
from .planning_models import (
    CandidateExploration,
    CandidateSelectionSummary,
    PlanningCandidate,
    ResourceShape,
    WorkloadVariant,
)
from .remote_transport import OpenSSHTransport, RemoteWorkerClient
from .site_models import Site, SitePolicyClaim
from .site_planning import (
    explore_candidates,
    rejection_reason_summary,
    resolve_environments,
    resource_shapes_from_inventory,
)
from .site_storage import SiteStore
from .workload import utc_now
from .workload_models import DecisionEvidence, ExecutionPlan
from .workload_storage import ExecutionStore


@dataclass(frozen=True)
class SitePlanningSession:
    request: ExecutionRequest
    workload_id: str
    inventory_snapshot_id: str
    site_id: str
    exploration: CandidateExploration

    def to_dict(self) -> dict[str, Any]:
        return {
            "request": self.request.to_dict(),
            "workload_id": self.workload_id,
            "inventory_snapshot_id": self.inventory_snapshot_id,
            "site_id": self.site_id,
            "exploration": {
                "candidates": [item.to_dict() for item in self.exploration.candidates],
                "generated_count": self.exploration.generated_count,
                "theoretical_count": self.exploration.theoretical_count,
                "hard_invalid_count": self.exploration.hard_invalid_count,
                "viable_count": self.exploration.viable_count,
                "truncated": self.exploration.truncated,
                "coverage": self.exploration.coverage,
            },
        }


class SiteService:
    """Own site configuration/discovery without exposing generic SSH commands."""

    def __init__(
        self,
        database_path: Path,
        *,
        remote_clients: Mapping[str, RemoteWorkerClient] | None = None,
    ):
        self.sites = SiteStore(database_path)
        self.inventories = InventoryStore(database_path)
        self.executions = ExecutionStore(database_path)
        self.remote_clients = {} if remote_clients is None else dict(remote_clients)

    def add_site(
        self,
        name: str,
        *,
        ssh_host: str | None = None,
        ssh_username: str | None = None,
        ssh_port: int | None = None,
        scheduler_hint: str | None = None,
        local_project_root: str | None = None,
        remote_project_root: str | None = None,
        remote_worker_path: str | None = None,
    ) -> Site:
        site = Site(
            id=new_ulid(), name=name,
            kind="local" if ssh_host is None else "remote_ssh",
            created_at=utc_now(), ssh_host=ssh_host,
            ssh_username=ssh_username, ssh_port=ssh_port,
            scheduler_hint=scheduler_hint,
            local_project_root=local_project_root,
            remote_project_root=remote_project_root,
            remote_worker_path=remote_worker_path,
        )
        self.sites.save(site)
        return site

    def discover(self, site_reference: str, *, cwd: Path | None = None) -> InventorySnapshot:
        site = self.sites.get(site_reference)
        if site.kind == "local":
            snapshot = discover_site(
                self.inventories, cwd=cwd, site_label=site.name,
                observation_scope="local_control_plane",
            )
        else:
            client = self._client(site)
            response = client.call(
                "discover",
                {
                    "site_label": site.name,
                    "working_directory": site.remote_project_root,
                },
            )
            if response.status != "ok" or not isinstance(response.data.get("inventory"), dict):
                raise RuntimeError("bounded remote site discovery was unavailable")
            snapshot = InventorySnapshot.from_dict(response.data["inventory"])
            if snapshot.site_label != site.name:
                raise ValueError("remote inventory site identity does not match")
            self.inventories.save(snapshot)
        self.sites.link_inventory(site.id, snapshot.id)
        return snapshot

    def add_policy_claim(self, claim: SitePolicyClaim) -> None:
        self.sites.save_policy_claim(claim)

    def _client(self, site: Site) -> RemoteWorkerClient:
        return self.remote_clients.get(site.id) or RemoteWorkerClient(
            site, OpenSSHTransport()
        )


class SitePlanningService:
    """Explore ephemerally; persist only the selection summary and selected plan."""

    def __init__(self, database_path: Path):
        self.sites = SiteStore(database_path)
        self.inventories = InventoryStore(database_path)
        self.executions = ExecutionStore(database_path)

    def explore_request(
        self,
        request: ExecutionRequest,
        site_reference: str,
        inventory: InventorySnapshot,
        *,
        provider: DeclarativeConstraintProvider | None = None,
        resource_shapes: Sequence[ResourceShape] | None = None,
        limit: int = 64,
    ) -> SitePlanningSession:
        site = self.sites.get(site_reference)
        linked_site = self.sites.site_for_inventory(inventory.id)
        if linked_site is None or linked_site.id != site.id:
            raise ValueError("inventory does not belong to the selected site")
        effective = self._effective_request(request, site)
        workload = request_to_workload(effective)
        self.executions.save_request_with_workload(effective, workload)
        shapes = list(
            resource_shapes
            if resource_shapes is not None
            else resource_shapes_from_inventory(inventory)
        )
        requirements: list[dict[str, Any]] = []
        if provider is not None:
            for requirement in (
                *provider.environment_requirements,
                *provider.launcher_requirements,
            ):
                if requirement not in requirements:
                    requirements.append(requirement)
        environments = resolve_environments(inventory, requirements)
        exploration = explore_candidates(
            workload, shapes, environments, provider=provider,
            policy_claims=self.sites.policy_claims(site.id), limit=limit,
        )
        return SitePlanningSession(
            request=effective, workload_id=workload.id,
            inventory_snapshot_id=inventory.id, site_id=site.id,
            exploration=exploration,
        )

    def select(
        self,
        session: SitePlanningSession,
        candidate_id: str,
        *,
        selection_source: str,
        selection_rationale: str | None = None,
        variant: WorkloadVariant | None = None,
    ) -> ExecutionPlan:
        if (
            not isinstance(selection_source, str)
            or not selection_source.strip()
            or len(selection_source) > 128
        ):
            raise ValueError("selection source must be a bounded non-empty string")
        if selection_rationale is not None and (
            not isinstance(selection_rationale, str)
            or len(selection_rationale) > 4096
        ):
            raise ValueError("selection rationale exceeds the persisted summary bound")
        candidates = [
            item for item in session.exploration.candidates if item.id == candidate_id
        ]
        if len(candidates) != 1:
            raise ValueError("selected candidate is absent from bounded exploration")
        candidate = candidates[0]
        if candidate.state != "viable":
            raise ValueError(
                f"candidate {candidate.id} is {candidate.state} and cannot be selected"
            )
        workload = self.executions.get_workload(session.workload_id)
        inventory = self.inventories.get(session.inventory_snapshot_id)
        site = self.sites.get(session.site_id)
        if variant is not None:
            if variant.workload_id != workload.id:
                raise ValueError("workload variant does not derive from this workload")
            self.sites.save_variant(variant)
        summary = CandidateSelectionSummary(
            id=new_ulid(), workload_id=workload.id, site_id=site.id,
            created_at=utc_now(),
            generated_count=session.exploration.generated_count,
            hard_invalid_count=session.exploration.hard_invalid_count,
            viable_count=session.exploration.viable_count,
            selected_candidate_id=candidate.id,
            selected_candidate_summary=candidate.to_dict(),
            rejection_reasons=rejection_reason_summary(session.exploration.candidates),
            selection_source=selection_source,
            selection_rationale=selection_rationale,
            unresolved_conditions=list(candidate.unresolved),
            truncated=session.exploration.truncated,
            coverage=session.exploration.coverage,
        )
        self.sites.save_selection(summary)
        planned_workload = workload
        if variant is not None:
            planned_request = _variant_request(session.request, site, variant)
            planned_workload = request_to_workload(planned_request)
            self.executions.save_request_with_workload(planned_request, planned_workload)
        family, scheduler_id, target_id = _scheduler_selection(
            planned_workload.constraints.backend, site.scheduler_hint, inventory, candidate
        )
        access = inventory.current_target
        if access is None:
            raise ValueError("site inventory has no access target")
        plan = ExecutionPlan(
            id=new_ulid(), workload_id=planned_workload.id,
            inventory_snapshot_id=inventory.id, backend=family,
            access_target_id=access.id, execution_target_id=target_id,
            execution_context_id=(
                None if candidate.environment is None else candidate.environment.context_id
            ),
            scheduler_id=scheduler_id,
            requested_resources=planned_workload.resources,
            executable=planned_workload.executable,
            arguments=list(planned_workload.arguments),
            working_directory=planned_workload.working_directory,
            inputs=list(planned_workload.inputs), outputs=list(planned_workload.outputs),
            compatibility_state="compatible",
            unresolved_conditions=list(candidate.unresolved),
            decision_evidence=[
                DecisionEvidence(
                    state="explicit", subject="candidate_selection",
                    message="human/agent selected one viable candidate",
                    subject_id=candidate.id,
                ),
                *[
                    DecisionEvidence(
                        state=(
                            reason.evidence_kind
                            if reason.evidence_kind in {
                                "observed", "inferred", "historical", "unknown", "explicit"
                            }
                            else "explicit" if reason.evidence_kind in {"site_declared", "user_declared"}
                            else "observed"
                        ),
                        subject=reason.code, message=reason.message,
                        subject_id=reason.source_ids[0] if reason.source_ids else None,
                    )
                    for reason in candidate.reasons
                ],
            ],
            created_at=utc_now(), site_id=site.id,
            resource_shape=candidate.resource_shape,
            environment=candidate.environment,
            workload_variant_id=None if variant is None else variant.id,
            selection_summary_id=summary.id,
            policy_basis=[item.to_dict() for item in self.sites.policy_claims(site.id)],
        )
        self.executions.save_plan(plan)
        return plan

    @staticmethod
    def _effective_request(request: ExecutionRequest, site: Site) -> ExecutionRequest:
        if site.kind != "remote_ssh" or site.remote_project_root is None:
            return request
        resolved = request.resolved_working_directory
        if site.local_project_root is not None:
            local_root = Path(site.local_project_root).resolve(strict=False)
            local_working = Path(resolved).resolve(strict=False)
            try:
                relative = local_working.relative_to(local_root)
            except ValueError as exc:
                raise ValueError("request working directory is outside the configured site mapping") from exc
            remote = str(PurePosixPath(site.remote_project_root) / PurePosixPath(relative.as_posix()))
        else:
            remote = site.remote_project_root
        metadata = dict(request.source.metadata)
        metadata.update(
            {
                "site_id": site.id,
                "original_resolved_working_directory": resolved,
            }
        )
        return replace(
            request,
            resolved_working_directory=remote,
            source=RequestSource(request.source.kind, tuple(sorted(metadata.items()))),
        )


def _scheduler_selection(
    requested_backend: str,
    scheduler_hint: str | None,
    inventory: InventorySnapshot,
    candidate: PlanningCandidate,
) -> tuple[str, str, str | None]:
    families = sorted({item.family for item in inventory.schedulers if item.family in {"slurm", "pbs"}})
    if requested_backend in {"slurm", "pbs"}:
        family = requested_backend
    elif scheduler_hint in {"slurm", "pbs"}:
        family = scheduler_hint
    elif len(families) == 1:
        family = families[0]
    else:
        raise ValueError("scheduler family remains ambiguous")
    schedulers = [item for item in inventory.schedulers if item.family == family]
    if len(schedulers) != 1:
        raise ValueError("scheduler identity remains ambiguous")
    scheduler = schedulers[0]
    target_id = next(
        (
            str(item.get("target_id"))
            for item in candidate.resource_shape.evidence
            if item.get("target_id") in scheduler.execution_target_ids
        ),
        None,
    )
    if target_id is None and candidate.resource_shape.scheduler_class is not None:
        target_id = next(
            (
                target.id for target in inventory.targets
                if target.id in scheduler.execution_target_ids
                and target.name == candidate.resource_shape.scheduler_class
            ),
            None,
        )
    if target_id is None and len(scheduler.execution_target_ids) == 1:
        target_id = scheduler.execution_target_ids[0]
    if target_id is None:
        raise ValueError("scheduler execution target remains ambiguous")
    return family, scheduler.id, target_id


def _variant_request(
    request: ExecutionRequest, site: Site, variant: WorkloadVariant
) -> ExecutionRequest:
    original = Path(variant.original_path).resolve(strict=False)
    if site.kind == "remote_ssh":
        if site.remote_project_root is None:
            raise ValueError("remote workload variant requires a remote project root")
        derived = str(
            PurePosixPath(site.remote_project_root) / ".bourne" / "variants"
            / variant.id / Path(variant.derived_path).name
        )
    else:
        derived = variant.derived_path

    def replace_path(value: str) -> str:
        candidate = Path(value)
        if not candidate.is_absolute():
            candidate = Path(request.base_directory) / candidate
        return derived if candidate.resolve(strict=False) == original else value

    command = tuple(replace_path(item) for item in request.command)
    inputs = tuple(replace_path(item) for item in request.artifacts.inputs)
    if command == request.command and inputs == request.artifacts.inputs:
        raise ValueError("workload variant source is not referenced by request argv/inputs")
    metadata = dict(request.source.metadata)
    metadata.update(
        {"effective_from_request": request.id, "workload_variant_id": variant.id}
    )
    return replace(
        request,
        id=new_ulid(), created_at=utc_now(), command=command,
        artifacts=RequestArtifacts(inputs=inputs, outputs=request.artifacts.outputs),
        source=RequestSource(request.source.kind, tuple(sorted(metadata.items()))),
    )
