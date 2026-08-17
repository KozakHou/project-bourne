"""Trusted process identity for scheduler ownership decisions."""

from __future__ import annotations

import getpass
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessIdentity:
    username: str
    effective_uid: int | None
    source: str

    def evidence(self) -> dict[str, object]:
        return {
            "username": self.username,
            "effective_uid": self.effective_uid,
            "source": self.source,
        }


def current_process_identity() -> ProcessIdentity:
    """Return an OS-backed identity on POSIX and a documented fallback elsewhere."""

    if os.name == "posix" and hasattr(os, "geteuid"):
        effective_uid = int(os.geteuid())
        username = _username_for_uid(effective_uid)
        if username:
            return ProcessIdentity(
                username=username,
                effective_uid=effective_uid,
                source="posix_effective_uid_password_database",
            )
        return ProcessIdentity(
            username=str(effective_uid),
            effective_uid=effective_uid,
            source="posix_effective_uid_numeric",
        )

    try:
        username = getpass.getuser()
    except (OSError, KeyError):
        username = "unknown"
    return ProcessIdentity(
        username=username,
        effective_uid=None,
        source="platform_username_fallback",
    )


def _username_for_uid(effective_uid: int) -> str | None:
    try:
        import pwd
    except ImportError:
        return None
    try:
        value = pwd.getpwuid(effective_uid).pw_name
    except (KeyError, OSError):
        return None
    return value if isinstance(value, str) and value else None
