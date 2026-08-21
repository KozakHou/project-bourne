"""Build and stage the self-contained stdlib-only scheduler worker."""

from __future__ import annotations

import json
import shutil
import tempfile
import zipapp
from pathlib import Path

from .execution_request import ExecutionRequest
from .workload_models import ExecutionPlan, WorkloadSpec

RELEASED_V04_STAGED_PLAN_SCHEMA_VERSION = 1
STAGED_PLAN_SCHEMA_VERSION = 2


def build_worker_zipapp(target: Path) -> Path:
    """Package the installed Bourne source without requiring it on compute nodes."""

    package_source = Path(__file__).resolve().parent
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(target)
    with tempfile.TemporaryDirectory(prefix="bourne-worker-") as raw_directory:
        source = Path(raw_directory)
        shutil.copytree(
            package_source, source / "bourneprov",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        (source / "__main__.py").write_text(
            "from bourneprov.compute_worker import main\nraise SystemExit(main())\n",
            encoding="utf-8",
        )
        zipapp.create_archive(source, target=target, compressed=True)
    return target


def write_staged_plan(
    target: Path,
    execution_id: str,
    plan: ExecutionPlan,
    workload: WorkloadSpec,
    request: ExecutionRequest | None = None,
) -> Path:
    if plan.workload_id != workload.id:
        raise ValueError("workload does not match staged plan")
    value = {
        "schema_version": (
            RELEASED_V04_STAGED_PLAN_SCHEMA_VERSION
            if request is None
            else STAGED_PLAN_SCHEMA_VERSION
        ),
        "execution_id": execution_id,
        "plan": plan.to_dict(),
        "workload": workload.to_dict(),
    }
    if request is not None:
        if (
            request.argv != workload.argv
            or request.resolved_parent_experiment_id
            != workload.parent_experiment_id
        ):
            raise ValueError("execution request does not match staged workload")
        value["request"] = request.to_dict()
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    if len(raw) > 1024 * 1024:
        raise ValueError("staged plan exceeds the size limit")
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
        stream.write(raw)
    return target
