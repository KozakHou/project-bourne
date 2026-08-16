"""Human presentation for compute-site inventories."""

from __future__ import annotations

from collections import Counter

from .discovery import CapabilityMatch
from .inventory_models import InventorySnapshot


def _display(value: object) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def format_inventory(snapshot: InventorySnapshot, *, discovered: bool = False) -> str:
    identity = snapshot.identity
    target = snapshot.current_target
    title = "Discovered inventory" if discovered else "Inventory"
    lines = [
        f"{title}: {snapshot.id}",
        f"Captured (UTC): {snapshot.captured_at}",
        f"Site label: {_display(snapshot.site_label)}",
        "",
        "Topology:",
        f"  Identity: {_display(None if identity is None else identity.username)}",
        f"    -> Access target: {_display(None if target is None else target.name)}",
    ]
    if target is not None:
        lines.extend(
            [
                f"       SSH session: {_display(target.metadata.get('ssh_session'))}",
                f"       Node role: {_display(target.metadata.get('node_role'))}",
            ]
        )
    if snapshot.schedulers:
        for scheduler in snapshot.schedulers:
            lines.append(f"       -> Scheduler: {scheduler.family} ({scheduler.state})")
            targets = [
                item
                for item in snapshot.execution_targets
                if item.provider == scheduler.provider
            ]
            for execution_target in targets:
                lines.append(
                    f"          -> {execution_target.name}: visible "
                    f"(authorization {execution_target.authorization})"
                )
    else:
        lines.append("       -> Scheduler: none observed (direct site is valid)")

    lines.extend(["", "Storage:"])
    if snapshot.storage:
        for resource in snapshot.storage:
            hints = ", ".join(resource.role_hints) or "none"
            filesystem = resource.filesystem_type or "unknown"
            lines.extend(
                [
                    f"  {resource.path}",
                    f"    role hint: {hints}",
                    f"    access: read={_display(resource.readable)} "
                    f"write={_display(resource.writable)} "
                    f"search={_display(resource.searchable)}",
                    f"    filesystem: {filesystem}; policy: unknown",
                ]
            )
    else:
        lines.append("  No storage observations.")

    context_counts = Counter(item.kind for item in snapshot.execution_contexts)
    capability_counts = Counter(item.kind for item in snapshot.capabilities)
    lines.extend(["", "Execution contexts:"])
    for kind, count in sorted(context_counts.items()):
        lines.append(f"  {kind}: {count}")
    if not context_counts:
        lines.append("  None observed.")
    lines.extend(["", "Capabilities:"])
    for kind, count in sorted(capability_counts.items()):
        lines.append(f"  {kind}: {count}")
    if not capability_counts:
        lines.append("  None observed.")

    lines.extend(["", "Providers:"])
    for result in snapshot.providers:
        suffix = f" — {result.diagnostic}" if result.diagnostic else ""
        lines.append(f"  {result.provider}: {result.status}{suffix}")
    return "\n".join(lines)


def format_capability_matches(name: str, matches: list[CapabilityMatch]) -> str:
    lines = [f"Capability: {name}"]
    if not matches:
        lines.append("No exact-name matches in this inventory.")
        return "\n".join(lines)
    for match in matches:
        capability = match.capability
        context = match.context
        evidence_state = (
            "historical only"
            if any(item.historical_only for item in match.evidence)
            else capability.observation_state
        )
        lines.extend(
            [
                "",
                f"{context.name} [{context.kind}]",
                f"  context: {context.id}",
                f"  locator: {_display(capability.locator)}",
                f"  evidence: {evidence_state}",
                f"  provider: {capability.provider}",
            ]
        )
    return "\n".join(lines)
