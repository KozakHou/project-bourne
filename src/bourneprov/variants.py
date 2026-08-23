"""Safe execution-scoped JSON workload-variant materialization."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .constraint_providers import (
    DeclarativeConstraintProvider,
    ProviderContractError,
    automatic_change_allowed,
)
from .ids import new_ulid
from .planning_models import WorkloadVariant
from .workload import utc_now

MAX_VARIANT_INPUT_BYTES = 16 * 1024 * 1024


def materialize_json_variant(
    workload_id: str,
    original: Path,
    staging_root: Path,
    changes: Mapping[str, Any],
    provider: DeclarativeConstraintProvider,
    *,
    proposer: str,
    approvals: Mapping[str, bool] | None = None,
    explicit_user_declarations: Mapping[str, bool] | None = None,
) -> WorkloadVariant:
    """Create an independent derived JSON input; the source is never written."""

    raw = original.read_bytes()
    if len(raw) > MAX_VARIANT_INPUT_BYTES:
        raise ValueError("variant source exceeds the size limit")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"variant source is not valid UTF-8 JSON: {exc}") from exc
    approvals = {} if approvals is None else dict(approvals)
    declarations = (
        {} if explicit_user_declarations is None else dict(explicit_user_declarations)
    )
    variant_id = new_ulid()
    changed_fields: list[dict[str, Any]] = []
    classifications: dict[str, Any] = {}
    supporting: list[dict[str, Any]] = []
    for name in sorted(changes):
        parameter = provider.parameter(name)
        if parameter.binding is None:
            raise ProviderContractError(f"parameter '{name}' has no safe materialization binding")
        if Path(parameter.binding["input"]).name != original.name:
            raise ProviderContractError(
                f"parameter '{name}' binding does not target {original.name}"
            )
        approved = approvals.get(name, False)
        if not automatic_change_allowed(
            parameter,
            explicit_user_declaration=declarations.get(name, False),
            explicit_change_approval=approved,
        ):
            raise PermissionError(
                f"parameter '{name}' ({parameter.classification}) is not approved for this change"
            )
        old = _replace(value, parameter.binding["path"], changes[name])
        changed_fields.append(
            {
                "parameter": name, "path": list(parameter.binding["path"]),
                "before": old, "after": changes[name],
            }
        )
        classifications[name] = parameter.classification
        supporting.append(dict(parameter.classification_evidence))

    directory = staging_root.resolve(strict=False) / variant_id
    directory.mkdir(parents=True, exist_ok=False)
    derived = directory / original.name
    encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with derived.open("xb") as stream:
        stream.write(encoded)
    return WorkloadVariant(
        id=variant_id, workload_id=workload_id, created_at=utc_now(),
        original_path=str(original.resolve(strict=False)),
        derived_path=str(derived),
        original_sha256="sha256:" + hashlib.sha256(raw).hexdigest(),
        derived_sha256="sha256:" + hashlib.sha256(encoded).hexdigest(),
        changed_fields=changed_fields, proposer=proposer,
        classifications=classifications, supporting_evidence=supporting,
        approval={
            "specific_change_approvals": approvals,
            "explicit_user_declarations": declarations,
            "classification_unchanged": True,
        },
    )


def _replace(document: Any, path: list[Any], replacement: Any) -> Any:
    current = document
    for component in path[:-1]:
        if isinstance(component, int):
            if not isinstance(current, list) or not 0 <= component < len(current):
                raise ProviderContractError("variant binding index does not exist")
        elif not isinstance(current, dict) or component not in current:
            raise ProviderContractError("variant binding key does not exist")
        current = current[component]
    final = path[-1]
    if isinstance(final, int):
        if not isinstance(current, list) or not 0 <= final < len(current):
            raise ProviderContractError("variant binding index does not exist")
    elif not isinstance(current, dict) or final not in current:
        raise ProviderContractError("variant binding key does not exist")
    original = current[final]
    current[final] = replacement
    return original
