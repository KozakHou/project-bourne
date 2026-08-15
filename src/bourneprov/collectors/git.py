"""Read-only Git provenance collection."""

from __future__ import annotations

import subprocess
from pathlib import Path

from ..models import GitProvenance

_TIMEOUT_SECONDS = 5


def _git(cwd: Path, *arguments: str) -> tuple[str | None, str | None]:
    try:
        completed = subprocess.run(
            ["git", "-C", str(cwd), *arguments],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError:
        return None, "git executable not found"
    except subprocess.TimeoutExpired:
        return None, "git command timed out"
    except OSError as exc:
        return None, f"git command failed: {exc}"

    if completed.returncode != 0:
        detail = completed.stderr.strip() or f"git exited with code {completed.returncode}"
        return None, detail
    return completed.stdout.strip(), None


def collect_git(cwd: Path) -> GitProvenance:
    """Collect the repository state visible from *cwd*, if any."""

    root, root_error = _git(cwd, "rev-parse", "--show-toplevel")
    if root is None:
        friendly_error = (
            "not a Git repository"
            if root_error and "not a git repository" in root_error.lower()
            else root_error
        )
        return GitProvenance(available=False, error=friendly_error)

    repository_root = Path(root)
    errors: list[str] = []

    commit, error = _git(repository_root, "rev-parse", "HEAD")
    if error:
        errors.append(f"commit: {error}")

    branch, error = _git(repository_root, "symbolic-ref", "--quiet", "--short", "HEAD")
    if error:
        # A detached HEAD is valid and has no branch name.
        branch = None

    status, error = _git(repository_root, "status", "--porcelain", "--untracked-files=normal")
    dirty = None if error else bool(status)
    if error:
        errors.append(f"dirty state: {error}")

    return GitProvenance(
        available=True,
        repository_root=str(repository_root),
        commit_sha=commit,
        branch=branch,
        dirty=dirty,
        error="; ".join(errors) or None,
    )
