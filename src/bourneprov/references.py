"""Human-friendly references layered over canonical experiment ULIDs."""

from __future__ import annotations

import re

from .models import Experiment
from .storage import ExperimentNotFound, ExperimentStore

_RELATIVE_REFERENCE = re.compile(r"@([1-9][0-9]*)\Z")
_AMBIGUITY_CANDIDATE_LIMIT = 20


class ExperimentReferenceError(LookupError):
    """Base class for an experiment reference that cannot be resolved."""


class MissingExperimentReference(ExperimentReferenceError):
    def __init__(self, reference: str):
        self.reference = reference
        super().__init__(reference)

    def __str__(self) -> str:
        return f"No experiment matches reference '{self.reference}'."


class AmbiguousExperimentReference(ExperimentReferenceError):
    def __init__(self, reference: str, matches: list[str]):
        self.reference = reference
        self.matches = matches
        super().__init__(reference)

    def __str__(self) -> str:
        candidates = "\n".join(f"  {experiment_id}" for experiment_id in self.matches)
        return (
            f"Ambiguous experiment reference '{self.reference}'.\n\n"
            f"Matches:\n{candidates}\n\nProvide a longer prefix."
        )


class RelativeExperimentReferenceOutOfRange(ExperimentReferenceError):
    def __init__(self, reference: str, available: int):
        self.reference = reference
        self.available = available
        super().__init__(reference)

    def __str__(self) -> str:
        return (
            f"Experiment reference '{self.reference}' is out of range; "
            f"only {self.available} experiment(s) are recorded."
        )


def resolve_experiment(store: ExperimentStore, reference: str) -> Experiment:
    """Resolve a full ULID, unique prefix, ``latest``, or one-based ``@N``."""

    if reference.casefold() == "latest":
        return _resolve_relative(store, reference, 1)

    relative = _RELATIVE_REFERENCE.fullmatch(reference)
    if relative:
        return _resolve_relative(store, reference, int(relative.group(1)))

    normalized = reference.upper()
    matches = store.find_ids_by_prefix(normalized, limit=_AMBIGUITY_CANDIDATE_LIMIT)
    if not matches:
        raise MissingExperimentReference(reference)
    if len(matches) > 1:
        raise AmbiguousExperimentReference(reference, matches)
    return store.get(matches[0])


def _resolve_relative(
    store: ExperimentStore, reference: str, position: int
) -> Experiment:
    try:
        return store.get_by_recency(position)
    except ExperimentNotFound:
        raise RelativeExperimentReferenceOutOfRange(
            reference, store.count()
        ) from None
