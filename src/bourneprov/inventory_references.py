"""References scoped exclusively to persisted inventory snapshots."""

from __future__ import annotations

import re

from .inventory_models import InventorySnapshot
from .inventory_storage import InventoryNotFound, InventoryStore

_RELATIVE = re.compile(r"@([1-9][0-9]*)\Z")


class InventoryReferenceError(LookupError):
    pass


def resolve_inventory(store: InventoryStore, reference: str) -> InventorySnapshot:
    if reference.casefold() == "latest":
        position = 1
    elif match := _RELATIVE.fullmatch(reference):
        position = int(match.group(1))
    else:
        matches = store.find_ids_by_prefix(reference.upper())
        if not matches:
            raise InventoryReferenceError(
                f"No inventory matches reference '{reference}'."
            )
        if len(matches) > 1:
            candidates = "\n".join(f"  {item}" for item in matches[:20])
            raise InventoryReferenceError(
                f"Ambiguous inventory reference '{reference}'.\n\n"
                f"Matches:\n{candidates}\n\nProvide a longer prefix."
            )
        return store.get(matches[0])

    try:
        return store.get_by_recency(position)
    except InventoryNotFound:
        available = store.count()
        raise InventoryReferenceError(
            f"Inventory reference '{reference}' is out of range; "
            f"only {available} snapshot(s) are recorded."
        ) from None
