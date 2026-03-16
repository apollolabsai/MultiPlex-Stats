"""Shared helpers for tracking structured sync progress steps."""
from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import threading


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _duration_seconds(started_at: str | None, completed_at: str | None) -> int | None:
    if not started_at or not completed_at:
        return None
    try:
        start_dt = datetime.fromisoformat(started_at)
        end_dt = datetime.fromisoformat(completed_at)
    except ValueError:
        return None
    return max(int((end_dt - start_dt).total_seconds()), 0)


class SyncProgressTracker:
    """Thread-safe in-memory tracker for ordered sync progress steps."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._steps: list[dict] = []
        self._step_map: dict[str, dict] = {}

    def reset(self, step_definitions: list[dict]) -> None:
        with self._lock:
            self._steps = []
            self._step_map = {}
            for order, definition in enumerate(step_definitions):
                step = {
                    'id': definition['id'],
                    'label': definition['label'],
                    'stage': definition.get('stage', ''),
                    'server_key': definition.get('server_key'),
                    'server_name': definition.get('server_name'),
                    'status': definition.get('status', 'pending'),
                    'detail': definition.get('detail', ''),
                    'current': definition.get('current'),
                    'total': definition.get('total'),
                    'unit': definition.get('unit', ''),
                    'started_at': definition.get('started_at'),
                    'completed_at': definition.get('completed_at'),
                    'duration_seconds': definition.get('duration_seconds'),
                    'error': definition.get('error'),
                    'order': order,
                }
                self._steps.append(step)
                self._step_map[step['id']] = step

    def snapshot(self) -> list[dict]:
        with self._lock:
            return deepcopy(self._steps)

    def get_step(self, step_id: str) -> dict | None:
        with self._lock:
            step = self._step_map.get(step_id)
            return deepcopy(step) if step else None

    def start(
        self,
        step_id: str,
        *,
        detail: str | None = None,
        current: int | None = None,
        total: int | None = None,
    ) -> None:
        with self._lock:
            step = self._step_map.get(step_id)
            if not step:
                return
            if not step.get('started_at'):
                step['started_at'] = _utc_now_iso()
            step['completed_at'] = None
            step['duration_seconds'] = None
            step['status'] = 'running'
            if detail is not None:
                step['detail'] = detail
            if current is not None:
                step['current'] = current
            if total is not None:
                step['total'] = total
            step['error'] = None

    def update(
        self,
        step_id: str,
        *,
        status: str | None = None,
        detail: str | None = None,
        current: int | None = None,
        total: int | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            step = self._step_map.get(step_id)
            if not step:
                return
            if status is not None:
                step['status'] = status
                if status == 'running' and not step.get('started_at'):
                    step['started_at'] = _utc_now_iso()
            if detail is not None:
                step['detail'] = detail
            if current is not None:
                step['current'] = current
            if total is not None:
                step['total'] = total
            if error is not None:
                step['error'] = error

    def complete(
        self,
        step_id: str,
        *,
        detail: str | None = None,
        current: int | None = None,
        total: int | None = None,
    ) -> None:
        with self._lock:
            step = self._step_map.get(step_id)
            if not step:
                return
            if not step.get('started_at'):
                step['started_at'] = _utc_now_iso()
            completed_at = _utc_now_iso()
            step['status'] = 'success'
            step['completed_at'] = completed_at
            step['duration_seconds'] = _duration_seconds(step.get('started_at'), completed_at)
            if detail is not None:
                step['detail'] = detail
            if current is not None:
                step['current'] = current
            elif step.get('total') is not None:
                step['current'] = step['total']
            if total is not None:
                step['total'] = total
                if step.get('current') is None:
                    step['current'] = total
            step['error'] = None

    def fail(self, step_id: str, *, detail: str | None = None, error: str | None = None) -> None:
        with self._lock:
            step = self._step_map.get(step_id)
            if not step:
                return
            if not step.get('started_at'):
                step['started_at'] = _utc_now_iso()
            completed_at = _utc_now_iso()
            step['status'] = 'failed'
            step['completed_at'] = completed_at
            step['duration_seconds'] = _duration_seconds(step.get('started_at'), completed_at)
            if detail is not None:
                step['detail'] = detail
            if error is not None:
                step['error'] = error

    def fail_first_running_for_server(self, server_key: str, *, stage: str | None = None, error: str | None = None) -> None:
        with self._lock:
            for step in self._steps:
                if step.get('server_key') != server_key:
                    continue
                if stage is not None and step.get('stage') != stage:
                    continue
                if step.get('status') != 'running':
                    continue
                completed_at = _utc_now_iso()
                if not step.get('started_at'):
                    step['started_at'] = completed_at
                step['status'] = 'failed'
                step['completed_at'] = completed_at
                step['duration_seconds'] = _duration_seconds(step.get('started_at'), completed_at)
                if error is not None:
                    step['error'] = error
                    step['detail'] = error
                return
