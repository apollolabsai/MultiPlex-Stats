"""Gunicorn hooks for MultiPlex Stats."""

from __future__ import annotations

import logging

from flask_app.services.media_scheduler_service import (
    configure_auto_media_sync,
    get_auto_media_sync_schedule,
)

logger = logging.getLogger('multiplex.scheduler')


def post_worker_init(worker):
    """Start the scheduler in the actual Gunicorn worker process."""
    app = getattr(worker, 'wsgi', None)
    if app is None:
        logger.error('Auto media sync scheduler not configured: Gunicorn worker.wsgi was unavailable.')
        return

    hour, minute = get_auto_media_sync_schedule()
    try:
        configure_auto_media_sync(
            app,
            hour=hour,
            minute=minute,
            startup_source='gunicorn_worker',
        )
    except Exception:
        logger.exception('Auto media sync scheduler configuration failed in Gunicorn worker.')
