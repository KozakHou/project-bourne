"""Explicit, streaming filesystem artifact capture."""

from __future__ import annotations

import hashlib
import stat
from datetime import datetime, timezone
from pathlib import Path

from .ids import new_ulid
from .models import Artifact

HASH_CHUNK_SIZE = 1024 * 1024
_ROLES = frozenset({"input", "output"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def resolve_artifact_path(path: str, cwd: Path) -> Path:
    """Normalize a declared path against the experiment working directory."""

    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = cwd / candidate
    return candidate.resolve(strict=False)


def sha256_file(path: Path, chunk_size: int = HASH_CHUNK_SIZE) -> str:
    """Hash *path* incrementally without copying or loading it into memory."""

    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def capture_artifact(
    experiment_id: str,
    role: str,
    path: str,
    cwd: Path,
) -> Artifact:
    """Capture one declared file while preserving missing/error state explicitly."""

    if role not in _ROLES:
        raise ValueError(f"unsupported artifact role: {role}")

    resolved = resolve_artifact_path(path, cwd)
    captured_at = _utc_now()
    common = {
        "id": new_ulid(),
        "experiment_id": experiment_id,
        "role": role,
        "original_path": path,
        "resolved_path": str(resolved),
        "captured_at": captured_at,
    }
    try:
        stat_before = resolved.stat()
    except FileNotFoundError:
        return Artifact(
            **common,
            existence_state="missing",
            capture_status="complete",
            sha256=None,
            size_bytes=None,
            modified_at=None,
            capture_error=None,
        )
    except OSError as exc:
        return Artifact(
            **common,
            existence_state="unknown",
            capture_status="unreadable",
            sha256=None,
            size_bytes=None,
            modified_at=None,
            capture_error=f"could not inspect artifact: {exc}",
        )

    if not stat.S_ISREG(stat_before.st_mode):
        return Artifact(
            **common,
            existence_state="present",
            capture_status="unsupported",
            sha256=None,
            size_bytes=stat_before.st_size,
            modified_at=_timestamp(stat_before.st_mtime),
            capture_error="declared artifact is not a regular file",
        )

    try:
        digest = sha256_file(resolved)
        stat_after = resolved.stat()
    except OSError as exc:
        return Artifact(
            **common,
            existence_state="present",
            capture_status="unreadable",
            sha256=None,
            size_bytes=stat_before.st_size,
            modified_at=_timestamp(stat_before.st_mtime),
            capture_error=f"could not fingerprint artifact: {exc}",
        )

    changed = (
        stat_before.st_size != stat_after.st_size
        or stat_before.st_mtime_ns != stat_after.st_mtime_ns
    )
    return Artifact(
        **common,
        existence_state="present",
        capture_status="changed" if changed else "complete",
        sha256=None if changed else digest,
        size_bytes=stat_after.st_size,
        modified_at=_timestamp(stat_after.st_mtime),
        capture_error=("artifact changed while being fingerprinted" if changed else None),
    )


def capture_artifacts(
    experiment_id: str,
    role: str,
    paths: list[str],
    cwd: Path,
) -> list[Artifact]:
    """Capture declared paths independently so one inaccessible file loses no others."""

    return [capture_artifact(experiment_id, role, path, cwd) for path in paths]
