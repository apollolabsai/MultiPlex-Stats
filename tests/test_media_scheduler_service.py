import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from flask import Flask

from flask_app.services.media_scheduler_service import (
    _run_scheduled_media_sync_once,
    _seconds_until_next_run,
)


class MediaSchedulerServiceTests(unittest.TestCase):
    def test_seconds_until_next_run_before_daily_window(self):
        now = datetime(2026, 3, 15, 0, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        seconds, next_run = _seconds_until_next_run(now, hour=1, minute=0)

        self.assertEqual(int(seconds), 1800)
        self.assertEqual(next_run.hour, 1)
        self.assertEqual(next_run.minute, 0)
        self.assertEqual(next_run.date(), now.date())

    def test_seconds_until_next_run_after_daily_window(self):
        now = datetime(2026, 3, 15, 1, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        seconds, next_run = _seconds_until_next_run(now, hour=1, minute=0)

        self.assertEqual(int(seconds), 84600)
        self.assertEqual(next_run.hour, 1)
        self.assertEqual(next_run.minute, 0)
        self.assertEqual(next_run.day, 16)

    @patch('flask_app.services.media_scheduler_service.ConfigService.has_valid_config')
    @patch('flask_app.services.media_scheduler_service._is_any_sync_running')
    @patch('flask_app.services.media_scheduler_service.MediaService.start_media_load')
    def test_run_scheduled_media_sync_once_starts_media_only_mode(
        self,
        mock_start_media_load,
        mock_is_any_sync_running,
        mock_has_valid_config,
    ):
        mock_has_valid_config.return_value = True
        mock_is_any_sync_running.return_value = False
        mock_start_media_load.return_value = True
        app = Flask(__name__)

        started, reason = _run_scheduled_media_sync_once(app)

        self.assertTrue(started)
        self.assertEqual(reason, 'started')
        mock_start_media_load.assert_called_once_with(run_mode='media_only')

    @patch('flask_app.services.media_scheduler_service.ConfigService.has_valid_config')
    @patch('flask_app.services.media_scheduler_service._is_any_sync_running')
    @patch('flask_app.services.media_scheduler_service.MediaService.start_media_load')
    def test_run_scheduled_media_sync_once_skips_when_other_sync_running(
        self,
        mock_start_media_load,
        mock_is_any_sync_running,
        mock_has_valid_config,
    ):
        mock_has_valid_config.return_value = True
        mock_is_any_sync_running.return_value = True
        app = Flask(__name__)

        started, reason = _run_scheduled_media_sync_once(app)

        self.assertFalse(started)
        self.assertEqual(reason, 'busy')
        mock_start_media_load.assert_not_called()
