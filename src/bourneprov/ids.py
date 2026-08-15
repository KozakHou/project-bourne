"""Portable public identifiers for experiments."""

from __future__ import annotations

import secrets
import time

_CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_ulid(timestamp_ms: int | None = None) -> str:
    """Return a 26-character ULID without requiring a runtime dependency."""

    if timestamp_ms is None:
        timestamp_ms = time.time_ns() // 1_000_000
    if not 0 <= timestamp_ms < 2**48:
        raise ValueError("ULID timestamp must fit in 48 bits")

    value = (timestamp_ms << 80) | int.from_bytes(secrets.token_bytes(10), "big")
    encoded = ["0"] * 26
    for index in range(25, -1, -1):
        encoded[index] = _CROCKFORD_BASE32[value & 0x1F]
        value >>= 5
    return "".join(encoded)
