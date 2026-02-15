"""
Shared utility functions for service modules.
"""
from typing import Any, Optional


def normalize_title(value: Any) -> str:
    """Normalize a media title for case-insensitive comparison.

    Strips whitespace, lowercases, and collapses internal whitespace runs.
    """
    if not value:
        return ''
    return ' '.join(str(value).strip().lower().split())


def to_int(value: Any) -> Optional[int]:
    """Safely convert a value to int, returning None on failure."""
    if value in (None, ''):
        return None
    if isinstance(value, str):
        value = value.replace(',', '').strip()
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
