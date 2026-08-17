"""Human-friendly references scoped to plans and execution attempts."""

from __future__ import annotations

import re
from typing import Callable, TypeVar

from .workload_models import ExecutionAttempt, ExecutionPlan
from .workload_storage import ExecutionNotFound, ExecutionStore, PlanNotFound

_RELATIVE = re.compile(r"@([1-9][0-9]*)\Z")
_T = TypeVar("_T", ExecutionPlan, ExecutionAttempt)


class WorkloadReferenceError(LookupError):
    pass


def resolve_plan(store: ExecutionStore, reference: str) -> ExecutionPlan:
    return _resolve(
        reference, "plan", store.get_plan_by_recency, store.find_plan_ids_by_prefix,
        store.get_plan, store.count_plans, PlanNotFound,
    )


def resolve_execution_attempt(
    store: ExecutionStore, reference: str
) -> ExecutionAttempt:
    return _resolve(
        reference, "execution", store.get_execution_by_recency,
        store.find_execution_ids_by_prefix, store.get_execution,
        store.count_executions, ExecutionNotFound,
    )


def _resolve(
    reference: str,
    label: str,
    by_recency: Callable[[int], _T],
    by_prefix: Callable[[str], list[str]],
    by_id: Callable[[str], _T],
    count: Callable[[], int],
    missing_type: type[LookupError],
) -> _T:
    position = 1 if reference.casefold() == "latest" else None
    match = _RELATIVE.fullmatch(reference)
    if match:
        position = int(match.group(1))
    if position is not None:
        try:
            return by_recency(position)
        except missing_type:
            raise WorkloadReferenceError(
                f"{label.title()} reference '{reference}' is out of range; "
                f"only {count()} are recorded."
            ) from None
    matches = by_prefix(reference.upper())
    if not matches:
        raise WorkloadReferenceError(
            f"No {label} matches reference '{reference}'."
        )
    if len(matches) > 1:
        candidates = "\n".join(f"  {item}" for item in matches[:20])
        raise WorkloadReferenceError(
            f"Ambiguous {label} reference '{reference}'.\n\n"
            f"Matches:\n{candidates}\n\nProvide a longer prefix."
        )
    return by_id(matches[0])
