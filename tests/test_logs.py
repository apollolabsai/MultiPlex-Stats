import itertools
import os
import unittest

from flask import Flask

import flask_app.routes.logs as log_routes
from flask_app.routes.logs import logs_bp
from flask_app.services import log_service


class LogServiceTests(unittest.TestCase):
    def setUp(self):
        self.original_entries = list(log_service._log_buffer)
        self.original_counter = log_service._log_id_counter
        log_service._log_buffer.clear()
        log_service._log_id_counter = itertools.count(1)

    def tearDown(self):
        log_service._log_buffer.clear()
        log_service._log_buffer.extend(self.original_entries)
        log_service._log_id_counter = self.original_counter

    def test_get_logs_without_cursor_returns_newest_matching_entries(self):
        for idx in range(1, 6):
            log_service._log_buffer.append({
                'id': idx,
                'timestamp': f'2026-03-14 00:00:0{idx}',
                'level': 'INFO',
                'logger': 'test',
                'message': f'entry {idx}',
            })

        entries = log_service.get_logs(limit=3)

        self.assertEqual([entry['id'] for entry in entries], [3, 4, 5])

    def test_get_logs_with_cursor_preserves_forward_stream_order(self):
        for idx in range(1, 6):
            log_service._log_buffer.append({
                'id': idx,
                'timestamp': f'2026-03-14 00:00:0{idx}',
                'level': 'INFO',
                'logger': 'test',
                'message': f'entry {idx}',
            })

        entries = log_service.get_logs(since_id=2, limit=2)

        self.assertEqual([entry['id'] for entry in entries], [3, 4])


class LogRoutesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        template_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'flask_app', 'templates'))
        cls.app = Flask(__name__, template_folder=template_root)
        cls.app.config['SECRET_KEY'] = 'test-secret'
        cls.app.register_blueprint(logs_bp, url_prefix='/logs')
        cls.client = cls.app.test_client()

    def test_logs_api_defaults_to_2000_entries(self):
        captured = {}

        def fake_get_logs(min_level='DEBUG', since_id=0, limit=2000):
            captured['min_level'] = min_level
            captured['since_id'] = since_id
            captured['limit'] = limit
            return []

        original_get_logs = log_routes.get_logs
        log_routes.get_logs = fake_get_logs
        try:
            response = self.client.get('/logs/api')
        finally:
            log_routes.get_logs = original_get_logs

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured['min_level'], 'DEBUG')
        self.assertEqual(captured['since_id'], 0)
        self.assertEqual(captured['limit'], 2000)
