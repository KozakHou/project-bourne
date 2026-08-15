"""Narrow metadata collectors used by the Bourne lifecycle."""

from .git import collect_git
from .system import collect_system

__all__ = ["collect_git", "collect_system"]
