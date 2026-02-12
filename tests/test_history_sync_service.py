import unittest
from unittest.mock import patch

from flask import Flask

from flask_app.models import HistorySyncStatus, ServerConfig, ViewingHistory, db
from flask_app.services.history_sync_service import HistorySyncService


def _history_response(records, total):
    return {
        'response': {
            'data': {
                'data': records,
                'recordsFiltered': total,
            }
        }
    }


class _FakeClient:
    def __init__(self, responses):
        self._responses = responses
        self._idx = 0

    def get_history_paginated(self, start=0, length=1000, after=None):
        if self._idx >= len(self._responses):
            return _history_response([], 0)
        response = self._responses[self._idx]
        self._idx += 1
        return response


class HistorySyncServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__)
        cls.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        cls.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        db.init_app(cls.app)
        with cls.app.app_context():
            db.create_all()

    @classmethod
    def tearDownClass(cls):
        with cls.app.app_context():
            db.drop_all()

    def setUp(self):
        self.ctx = self.app.app_context()
        self.ctx.push()
        ViewingHistory.query.delete()
        ServerConfig.query.delete()
        HistorySyncStatus.query.delete()
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()

    def _add_server(self, name: str, order: int):
        db.session.add(ServerConfig(
            name=name,
            ip_address='127.0.0.1:8181',
            api_key='key',
            server_order=order,
            is_active=True,
        ))
        db.session.commit()

    def test_backfill_aggregates_totals_from_both_servers(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)

        records_a = [
            {'row_id': 1, 'started': 1700000000, 'media_type': 'movie', 'title': 'A1'},
            {'row_id': 2, 'started': 1700000100, 'media_type': 'movie', 'title': 'A2'},
        ]
        records_b = [
            {'row_id': 3, 'started': 1700000200, 'media_type': 'movie', 'title': 'B1'},
            {'row_id': 4, 'started': 1700000300, 'media_type': 'movie', 'title': 'B2'},
        ]

        client_map = {
            'Server A': _FakeClient([_history_response(records_a, 2)]),
            'Server B': _FakeClient([_history_response(records_b, 2)]),
        }

        with patch('flask_app.services.history_sync_service.TautulliClient',
                   side_effect=lambda config: client_map[config.name]):
            started = HistorySyncService().start_backfill(30)

        self.assertTrue(started)
        status = HistorySyncService().get_or_create_status()
        self.assertEqual(status.status, 'success')
        self.assertEqual(status.records_total, 4)
        self.assertEqual(status.records_fetched, 4)
        self.assertEqual(status.records_inserted, 4)
        self.assertEqual(status.records_skipped, 0)
        self.assertEqual(ViewingHistory.query.count(), 4)

    def test_backfill_skips_duplicate_row_ids_between_servers(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)

        duplicate_row = {'row_id': 99, 'started': 1700000000, 'media_type': 'episode', 'title': 'Dup'}
        client_map = {
            'Server A': _FakeClient([_history_response([duplicate_row], 1)]),
            'Server B': _FakeClient([_history_response([duplicate_row], 1)]),
        }

        with patch('flask_app.services.history_sync_service.TautulliClient',
                   side_effect=lambda config: client_map[config.name]):
            started = HistorySyncService().start_backfill(30)

        self.assertTrue(started)
        status = HistorySyncService().get_or_create_status()
        self.assertEqual(status.status, 'success')
        self.assertEqual(status.records_total, 2)
        self.assertEqual(status.records_fetched, 2)
        self.assertEqual(status.records_inserted, 1)
        self.assertEqual(status.records_skipped, 1)
        self.assertEqual(ViewingHistory.query.count(), 1)
