"""Helpers for recovering stale sync rows after a process restart."""
from __future__ import annotations

from datetime import UTC, datetime


PROCESS_STARTED_AT_UTC = datetime.now(UTC)


def coerce_utc(value: datetime | None) -> datetime | None:
    """Treat naive datetimes from SQLite as UTC and normalize aware values."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def is_recoverable_stale_run(
    status: str | None,
    started_at: datetime | None,
    progress_items: list[dict] | None,
) -> bool:
    """Return True when a sync row is still marked running after a restart."""
    if status != 'running':
        return False

    started_at_utc = coerce_utc(started_at)
    if started_at_utc is None or started_at_utc >= PROCESS_STARTED_AT_UTC:
        return False

    return not any(item.get('status') == 'running' for item in (progress_items or []))
