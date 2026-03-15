"""
Background scheduler for automatic daily media sync.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timedelta

from flask import Flask

from flask_app.services.config_service import ConfigService
from flask_app.services.history_sync_service import HistorySyncService
from flask_app.services.media_lifetime_stats_service import MediaLifetimeStatsService
from flask_app.services.media_service import MediaService
from multiplex_stats.timezone_utils import get_local_timezone

logger = logging.getLogger('multiplex.scheduler')

_scheduler_lock = threading.Lock()
_scheduler_thread: threading.Thread | None = None


def _seconds_until_next_run(now_local: datetime, hour: int = 1, minute: int = 0) -> tuple[float, datetime]:
    """Return seconds until the next scheduled local run and the run timestamp."""
    next_run = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now_local >= next_run:
        next_run += timedelta(days=1)
    return max((next_run - now_local).total_seconds(), 0.0), next_run


def _is_any_sync_running() -> bool:
    """Return True if any sync pipeline stage is already running."""
    return any(
        service().get_sync_status().get('status') == 'running'
        for service in (MediaService, HistorySyncService, MediaLifetimeStatsService)
    )


def _run_scheduled_media_sync_once(app: Flask) -> tuple[bool, str]:
    """Attempt one scheduled media-only sync run."""
    with app.app_context():
        if not ConfigService.has_valid_config():
            logger.info('Auto media sync skipped: no server configuration found.')
            return False, 'no_config'

        if _is_any_sync_running():
            logger.info('Auto media sync skipped: another sync is already running.')
            return False, 'busy'

        started = MediaService().start_media_load(run_mode=MediaService.RUN_MODE_MEDIA_ONLY)
        if started:
            logger.info('Auto media sync started.')
            return True, 'started'

        logger.info('Auto media sync skipped: media sync already running.')
        return False, 'already_running'


def _scheduler_loop(app: Flask, hour: int = 1, minute: int = 0) -> None:
    """Run the daily scheduler loop in a daemon thread."""
    local_tz = get_local_timezone()
    logger.info(
        'Auto media sync scheduler started: daily at %02d:%02d %s.',
        hour,
        minute,
        getattr(local_tz, 'key', 'local'),
    )

    while True:
        now_local = datetime.now(local_tz)
        sleep_seconds, next_run = _seconds_until_next_run(now_local, hour=hour, minute=minute)
        logger.info('Next auto media sync scheduled for %s.', next_run.isoformat())
        time.sleep(sleep_seconds)
        try:
            _run_scheduled_media_sync_once(app)
        except Exception:
            logger.exception('Auto media sync failed unexpectedly.')
            time.sleep(5)


def start_auto_media_sync_scheduler(app: Flask, hour: int = 1, minute: int = 0) -> bool:
    """Start the automatic media sync scheduler once per process."""
    global _scheduler_thread

    if app.config.get('TESTING'):
        return False

    if app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        return False

    with _scheduler_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return False

        _scheduler_thread = threading.Thread(
            target=_scheduler_loop,
            args=(app, hour, minute),
            name='auto-media-sync-scheduler',
            daemon=True,
        )
        _scheduler_thread.start()
        return True
