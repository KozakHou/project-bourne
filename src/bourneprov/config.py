"""Local configuration for Bourne."""

from __future__ import annotations

import os
from pathlib import Path


def default_database_path() -> Path:
    """Resolve the local SQLite path without requiring a configuration file."""

    explicit = os.environ.get("BOURNE_DB")
    if explicit:
        return Path(explicit).expanduser()

    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        return Path(xdg_data_home).expanduser() / "bourne" / "experiments.sqlite3"

    local_app_data = os.environ.get("LOCALAPPDATA")
    if os.name == "nt" and local_app_data:
        return Path(local_app_data) / "Bourne" / "experiments.sqlite3"

    return Path.home() / ".local" / "share" / "bourne" / "experiments.sqlite3"
