import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from flask import Flask

from flask_app.models import ViewingHistory, db
from flask_app.services.media_service import MediaService


class MediaServiceTests(unittest.TestCase):
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
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()

    def _add_history(self, **kwargs) -> ViewingHistory:
        record = ViewingHistory(**kwargs)
        db.session.add(record)
        db.session.commit()
        return record

    @patch('flask_app.services.media_service.TautulliClient')
    @patch('flask_app.services.media_service.ContentService')
    def test_apply_endpoint_play_counts_uses_content_detail_methodology(self, mock_content_service_cls, mock_client_cls):
        service = MediaService()

        server_a = SimpleNamespace(name='Server A')
        server_b = SimpleNamespace(name='Server B')

        mock_client_cls.side_effect = lambda config: f"client:{config.name}"
        content_service = MagicMock()
        mock_content_service_cls.return_value = content_service

        watch_totals = {
            ('client:Server A', 111, 'movie'): 10,
            ('client:Server B', 222, 'movie'): None,
            ('client:Server A', 333, 'show'): 3,
        }
        user_totals = {
            ('client:Server B', 222, 'movie'): {'total_plays': 7},
        }

        def watch_side_effect(client, rating_key, media_type):
            return watch_totals.get((client, rating_key, media_type))

        def user_side_effect(client, rating_key, media_type):
            return user_totals.get((client, rating_key, media_type))

        content_service._fetch_watch_total_plays.side_effect = watch_side_effect
        content_service._fetch_item_user_stats.side_effect = user_side_effect

        movies_data = {
            ('Inception', 2010): {
                'library_play_count': 9,
                'server_rating_keys': {
                    'Server A': {111},
                    'Server B': {222},
                },
                'play_count': 0,
            }
        }
        tv_data = {
            'Family Guy': {
                'library_play_count': 5,
                'server_rating_keys': {
                    'Server A': {333},
                },
                'play_count': 0,
            }
        }

        service._apply_endpoint_play_counts(
            movies_data=movies_data,
            tv_data=tv_data,
            server_a_config=server_a,
            server_b_config=server_b,
        )

        self.assertEqual(movies_data[('Inception', 2010)]['play_count'], 17)
        self.assertEqual(tv_data['Family Guy']['play_count'], 3)

    @patch('flask_app.services.media_service.TautulliClient')
    @patch('flask_app.services.media_service.ContentService')
    def test_apply_endpoint_play_counts_falls_back_to_library_counts(self, mock_content_service_cls, mock_client_cls):
        service = MediaService()
        server_a = SimpleNamespace(name='Server A')
        mock_client_cls.return_value = 'client:Server A'

        content_service = MagicMock()
        content_service._fetch_watch_total_plays.return_value = None
        content_service._fetch_item_user_stats.return_value = None
        mock_content_service_cls.return_value = content_service

        movies_data = {
            ('Arrival', 2016): {
                'library_play_count': 12,
                'server_rating_keys': {'Server A': {701}},
                'play_count': 0,
            }
        }

        service._apply_endpoint_play_counts(
            movies_data=movies_data,
            tv_data={},
            server_a_config=server_a,
            server_b_config=None,
        )

        self.assertEqual(movies_data[('Arrival', 2016)]['play_count'], 12)

    def test_find_movie_history_id_returns_latest_match(self):
        older = self._add_history(
            row_id=1001,
            server_name='Server A',
            media_type='movie',
            title='Inception',
            full_title='Inception',
            year=2010,
            started=100,
        )
        newer = self._add_history(
            row_id=1002,
            server_name='Server B',
            media_type='movie',
            title='Inception',
            full_title='Inception',
            year=2010,
            started=200,
        )

        history_id = MediaService()._find_movie_history_id('Inception', 2010)
        self.assertEqual(history_id, newer.id)
        self.assertNotEqual(history_id, older.id)

    def test_find_show_history_id_returns_latest_match(self):
        older = self._add_history(
            row_id=2001,
            server_name='Server A',
            media_type='episode',
            grandparent_title='Family Guy',
            title='Road to the North Pole',
            started=100,
        )
        newer = self._add_history(
            row_id=2002,
            server_name='Server B',
            media_type='episode',
            grandparent_title='Family Guy',
            title='And Then There Were Fewer',
            started=200,
        )

        history_id = MediaService()._find_show_history_id('Family Guy')
        self.assertEqual(history_id, newer.id)
        self.assertNotEqual(history_id, older.id)

