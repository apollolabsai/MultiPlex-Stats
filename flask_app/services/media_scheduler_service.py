"""
Background scheduler for automatic daily media sync.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone

from flask import Flask

from flask_app.services.config_service import ConfigService
from flask_app.services.history_sync_service import HistorySyncService
from flask_app.services.media_lifetime_stats_service import MediaLifetimeStatsService
from flask_app.services.media_service import MediaService
from multiplex_stats.timezone_utils import get_local_timezone

logger = logging.getLogger('multiplex.scheduler')

_scheduler_lock = threading.Lock()
_scheduler_thread: threading.Thread | None = None


def get_auto_media_sync_schedule() -> tuple[int, int]:
    """Return the configured local run time, defaulting to 05:00."""
    raw_value = (os.environ.get('AUTO_MEDIA_SYNC_TIME') or '').strip()
    if not raw_value:
        return 5, 0

    try:
        hour_str, minute_str = raw_value.split(':', 1)
        hour = int(hour_str)
        minute = int(minute_str)
    except ValueError:
        logger.warning(
            'Invalid AUTO_MEDIA_SYNC_TIME=%r; expected HH:MM, defaulting to 05:00.',
            raw_value,
        )
        return 5, 0

    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return hour, minute

    logger.warning(
        'Out-of-range AUTO_MEDIA_SYNC_TIME=%r; expected HH:MM, defaulting to 05:00.',
        raw_value,
    )
    return 5, 0


def _seconds_until_next_run(now_local: datetime, hour: int = 1, minute: int = 0) -> tuple[float, datetime]:
    """Return seconds until the next scheduled local run and the run timestamp."""
    next_run = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now_local >= next_run:
        next_run += timedelta(days=1)
    return max((next_run - now_local).total_seconds(), 0.0), next_run


def _should_run_startup_catchup(
    now_local: datetime,
    last_sync_local_date: date | None,
    hour: int = 1,
    minute: int = 0,
) -> tuple[bool, str]:
    """Return whether startup should catch up a missed daily run."""
    scheduled_today = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now_local < scheduled_today:
        return False, 'before_window'
    if last_sync_local_date == now_local.date():
        return False, 'already_synced_today'
    return True, 'missed_window'


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


def maybe_run_startup_media_sync_catchup(
    app: Flask,
    hour: int = 1,
    minute: int = 0,
    startup_source: str = 'startup',
) -> tuple[bool, str]:
    """Run one catch-up sync at startup if today's scheduled window was missed."""
    local_tz = get_local_timezone()
    now_local = datetime.now(local_tz)

    with app.app_context():
        status = MediaService().get_or_create_status()
        last_sync_local_date = None
        if status.last_sync_date:
            last_sync_local_date = (
                status.last_sync_date
                .replace(tzinfo=timezone.utc)
                .astimezone(local_tz)
                .date()
            )

    should_run, reason = _should_run_startup_catchup(
        now_local,
        last_sync_local_date,
        hour=hour,
        minute=minute,
    )
    if not should_run:
        logger.info(
            'Auto media sync startup catch-up not needed on %s (%s).',
            startup_source,
            reason,
        )
        return False, reason

    logger.info(
        'Auto media sync startup catch-up attempting on %s after missing the %02d:%02d window.',
        startup_source,
        hour,
        minute,
    )
    started, reason = _run_scheduled_media_sync_once(app)
    if started:
        logger.info('Auto media sync startup catch-up started on %s.', startup_source)
    else:
        logger.info(
            'Auto media sync startup catch-up did not start on %s (%s).',
            startup_source,
            reason,
        )
    return started, reason


def start_auto_media_sync_scheduler(app: Flask, hour: int = 1, minute: int = 0) -> tuple[bool, str]:
    """Start the automatic media sync scheduler once per process."""
    global _scheduler_thread

    if app.config.get('TESTING'):
        return False, 'testing'

    if app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        return False, 'werkzeug_parent'

    with _scheduler_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return False, 'already_running'

        _scheduler_thread = threading.Thread(
            target=_scheduler_loop,
            args=(app, hour, minute),
            name='auto-media-sync-scheduler',
            daemon=True,
        )
        _scheduler_thread.start()
        return True, 'started'


def configure_auto_media_sync(
    app: Flask,
    hour: int = 1,
    minute: int = 0,
    startup_source: str = 'startup',
) -> tuple[str, str]:
    """Start the scheduler and run startup catch-up with explicit logging."""
    local_tz = get_local_timezone()
    logger.info(
        'Configuring auto media sync from %s for %02d:%02d %s.',
        startup_source,
        hour,
        minute,
        getattr(local_tz, 'key', 'local'),
    )

    started, start_reason = start_auto_media_sync_scheduler(app, hour=hour, minute=minute)
    if started:
        logger.info('Auto media sync scheduler thread launched from %s.', startup_source)
    elif start_reason == 'already_running':
        logger.info('Auto media sync scheduler thread already running on %s.', startup_source)
    else:
        logger.warning(
            'Auto media sync scheduler thread not launched from %s (%s).',
            startup_source,
            start_reason,
        )
        return start_reason, 'scheduler_not_running'

    _, catchup_reason = maybe_run_startup_media_sync_catchup(
        app,
        hour=hour,
        minute=minute,
        startup_source=startup_source,
    )
    return start_reason, catchup_reason
