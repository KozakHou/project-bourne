"""Transactional persistence for sites, policy, planning, and remote truth."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .planning_models import CandidateSelectionSummary, WorkloadVariant
from .site_models import Site, SitePolicyClaim
from .storage import ExperimentStore


class SiteNotFound(LookupError):
    pass


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class SiteStore:
    def __init__(self, path: Path):
        self.path = path

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        ExperimentStore(self.path).initialize()
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def save(self, site: Site) -> None:
        try:
            with self._connection() as connection:
                connection.execute(
                    """
                    INSERT INTO sites (id, name, kind, created_at, site_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        site.id, site.name, site.kind, site.created_at,
                        _json(site.to_dict()),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"site name or identity already exists: {site.name}") from exc

    def get(self, reference: str) -> Site:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT site_json FROM sites WHERE id = ? OR name = ?",
                (reference, reference),
            ).fetchone()
        if row is None:
            raise SiteNotFound(reference)
        return Site.from_dict(json.loads(row["site_json"]))

    def list(self) -> list[Site]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT site_json FROM sites ORDER BY name, id"
            ).fetchall()
        return [Site.from_dict(json.loads(row["site_json"])) for row in rows]

    def save_policy_claim(self, claim: SitePolicyClaim) -> None:
        self.get(claim.site_id)
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO site_policy_claims (
                    id, site_id, subject, property, evidence_kind,
                    interpretation_status, created_at, claim_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    claim.id, claim.site_id, claim.subject, claim.property,
                    claim.evidence_kind, claim.interpretation_status,
                    claim.created_at, _json(claim.to_dict()),
                ),
            )

    def policy_claims(self, site_id: str) -> list[SitePolicyClaim]:
        self.get(site_id)
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT claim_json FROM site_policy_claims
                WHERE site_id = ? ORDER BY created_at, id
                """,
                (site_id,),
            ).fetchall()
        return [SitePolicyClaim.from_dict(json.loads(row["claim_json"])) for row in rows]

    def link_inventory(self, site_id: str, snapshot_id: str) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO inventory_site_links (snapshot_id, site_id, relationship)
                VALUES (?, ?, 'observed_at')
                """,
                (snapshot_id, site_id),
            )

    def site_for_inventory(self, snapshot_id: str) -> Site | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT s.site_json FROM sites s
                JOIN inventory_site_links l ON l.site_id = s.id
                WHERE l.snapshot_id = ?
                """,
                (snapshot_id,),
            ).fetchone()
        return None if row is None else Site.from_dict(json.loads(row["site_json"]))

    def inventory_ids(self, site_id: str, limit: int = 100) -> list[str]:
        self.get(site_id)
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT l.snapshot_id FROM inventory_site_links l
                JOIN inventory_snapshots i ON i.id = l.snapshot_id
                WHERE l.site_id = ?
                ORDER BY i.captured_at DESC, i.id DESC LIMIT ?
                """,
                (site_id, limit),
            ).fetchall()
        return [str(row["snapshot_id"]) for row in rows]

    def save_selection(self, summary: CandidateSelectionSummary) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO candidate_selection_summaries (
                    id, workload_id, site_id, created_at, generated_count,
                    hard_invalid_count, viable_count, truncated, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    summary.id, summary.workload_id, summary.site_id,
                    summary.created_at, summary.generated_count,
                    summary.hard_invalid_count, summary.viable_count,
                    summary.truncated, _json(summary.to_dict()),
                ),
            )

    def get_selection(self, summary_id: str) -> CandidateSelectionSummary:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT summary_json FROM candidate_selection_summaries WHERE id = ?",
                (summary_id,),
            ).fetchone()
        if row is None:
            raise LookupError(summary_id)
        return CandidateSelectionSummary.from_dict(json.loads(row["summary_json"]))

    def save_variant(self, variant: WorkloadVariant) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO workload_variants (
                    id, workload_id, created_at, original_sha256,
                    derived_sha256, variant_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    variant.id, variant.workload_id, variant.created_at,
                    variant.original_sha256, variant.derived_sha256,
                    _json(variant.to_dict()),
                ),
            )

    def get_variant(self, variant_id: str) -> WorkloadVariant:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT variant_json FROM workload_variants WHERE id = ?",
                (variant_id,),
            ).fetchone()
        if row is None:
            raise LookupError(variant_id)
        return WorkloadVariant.from_dict(json.loads(row["variant_json"]))

    def save_remote_state(
        self,
        execution_id: str,
        site_id: str,
        state: str,
        observed_at: str,
        *,
        remote_staging_directory: str | None = None,
        scheduler_family: str | None = None,
        scheduler_job_id: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO remote_execution_states (
                    execution_id, site_id, state, remote_staging_directory,
                    scheduler_family, scheduler_job_id, observed_at, evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(execution_id) DO UPDATE SET
                    state = excluded.state,
                    remote_staging_directory = excluded.remote_staging_directory,
                    scheduler_family = excluded.scheduler_family,
                    scheduler_job_id = excluded.scheduler_job_id,
                    observed_at = excluded.observed_at,
                    evidence_json = excluded.evidence_json
                """,
                (
                    execution_id, site_id, state, remote_staging_directory,
                    scheduler_family, scheduler_job_id, observed_at,
                    _json({} if evidence is None else evidence),
                ),
            )

    def remote_state(self, execution_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM remote_execution_states WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["evidence"] = json.loads(result.pop("evidence_json"))
        return result
