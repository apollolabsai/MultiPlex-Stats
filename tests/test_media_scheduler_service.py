import unittest
from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from flask import Flask

from flask_app.services.media_scheduler_service import (
    get_auto_media_sync_schedule,
    _run_scheduled_media_sync_once,
    _should_run_startup_catchup,
    _seconds_until_next_run,
    start_auto_media_sync_scheduler,
)


class MediaSchedulerServiceTests(unittest.TestCase):
    def tearDown(self):
        import flask_app.services.media_scheduler_service as scheduler_service
        scheduler_service._scheduler_thread = None

    def test_get_auto_media_sync_schedule_defaults_to_5am(self):
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(get_auto_media_sync_schedule(), (5, 0))

    def test_get_auto_media_sync_schedule_uses_env_override(self):
        with patch.dict('os.environ', {'AUTO_MEDIA_SYNC_TIME': '06:42'}, clear=True):
            self.assertEqual(get_auto_media_sync_schedule(), (6, 42))

    def test_get_auto_media_sync_schedule_rejects_invalid_env_override(self):
        with patch.dict('os.environ', {'AUTO_MEDIA_SYNC_TIME': '25:00'}, clear=True):
            self.assertEqual(get_auto_media_sync_schedule(), (5, 0))

    def test_should_run_startup_catchup_before_daily_window(self):
        now = datetime(2026, 3, 18, 4, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        should_run, reason = _should_run_startup_catchup(now, None, hour=5, minute=0)

        self.assertFalse(should_run)
        self.assertEqual(reason, 'before_window')

    def test_should_run_startup_catchup_skips_when_already_synced_today(self):
        now = datetime(2026, 3, 18, 8, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        should_run, reason = _should_run_startup_catchup(
            now,
            date(2026, 3, 18),
            hour=5,
            minute=0,
        )

        self.assertFalse(should_run)
        self.assertEqual(reason, 'already_synced_today')

    def test_should_run_startup_catchup_after_missed_window(self):
        now = datetime(2026, 3, 18, 8, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        should_run, reason = _should_run_startup_catchup(
            now,
            date(2026, 3, 17),
            hour=5,
            minute=0,
        )

        self.assertTrue(should_run)
        self.assertEqual(reason, 'missed_window')

    @patch('flask_app.services.media_scheduler_service.threading.Thread')
    def test_start_scheduler_allows_gunicorn_worker_when_debug_enabled(self, mock_thread):
        thread = mock_thread.return_value
        thread.is_alive.return_value = False
        app = Flask(__name__)
        app.debug = True

        with patch.dict('os.environ', {}, clear=True):
            started, reason = start_auto_media_sync_scheduler(
                app,
                hour=5,
                minute=0,
                startup_source='gunicorn_worker',
            )

        self.assertTrue(started)
        self.assertEqual(reason, 'started')
        mock_thread.assert_called_once()

    @patch('flask_app.services.media_scheduler_service.threading.Thread')
    def test_start_scheduler_blocks_werkzeug_parent_in_dev_server_mode(self, mock_thread):
        app = Flask(__name__)
        app.debug = True

        with patch.dict('os.environ', {}, clear=True):
            started, reason = start_auto_media_sync_scheduler(
                app,
                hour=5,
                minute=0,
                startup_source='flask_dev_server',
            )

        self.assertFalse(started)
        self.assertEqual(reason, 'werkzeug_parent')
        mock_thread.assert_not_called()

    def test_seconds_until_next_run_before_daily_window(self):
        now = datetime(2026, 3, 15, 4, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        seconds, next_run = _seconds_until_next_run(now, hour=5, minute=0)

        self.assertEqual(int(seconds), 1800)
        self.assertEqual(next_run.hour, 5)
        self.assertEqual(next_run.minute, 0)
        self.assertEqual(next_run.date(), now.date())

    def test_seconds_until_next_run_after_daily_window(self):
        now = datetime(2026, 3, 15, 5, 30, tzinfo=ZoneInfo('America/Los_Angeles'))

        seconds, next_run = _seconds_until_next_run(now, hour=5, minute=0)

        self.assertEqual(int(seconds), 84600)
        self.assertEqual(next_run.hour, 5)
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
