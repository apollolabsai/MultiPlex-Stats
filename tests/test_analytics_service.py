import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from flask_app.models import CachedMedia, ViewingHistory, db
from flask_app.services.analytics_service import AnalyticsService


class AnalyticsServiceCurrentActivityLinkTests(unittest.TestCase):
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
        CachedMedia.query.delete()
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

    def _add_media(self, **kwargs) -> CachedMedia:
        record = CachedMedia(**kwargs)
        db.session.add(record)
        db.session.commit()
        return record

    def test_resolve_episode_history_id_prefers_title_with_colliding_key(self):
        newer = self._add_history(
            row_id=1001,
            server_name='Apollo',
            media_type='episode',
            grandparent_title='Game of Thrones',
            grandparent_rating_key=519192,
            started=200,
        )
        expected = self._add_history(
            row_id=1002,
            server_name='Apollo',
            media_type='episode',
            grandparent_title='A Knight of the Seven Kingdoms',
            grandparent_rating_key=519192,
            started=100,
        )

        resolved = AnalyticsService()._resolve_history_id_for_stream(
            server_name='Apollo',
            media_type='episode',
            rating_key=None,
            grandparent_rating_key='519192',
            title='A Knight of the Seven Kingdoms',
            grandparent_title='A Knight of the Seven Kingdoms',
        )

        self.assertEqual(resolved, expected.id)
        self.assertNotEqual(resolved, newer.id)

    def test_resolve_episode_history_id_falls_back_to_latest_when_no_title(self):
        expected = self._add_history(
            row_id=2001,
            server_name='Apollo',
            media_type='episode',
            grandparent_title='Game of Thrones',
            grandparent_rating_key=519192,
            started=200,
        )
        self._add_history(
            row_id=2002,
            server_name='Apollo',
            media_type='episode',
            grandparent_title='A Knight of the Seven Kingdoms',
            grandparent_rating_key=519192,
            started=100,
        )

        resolved = AnalyticsService()._resolve_history_id_for_stream(
            server_name='Apollo',
            media_type='episode',
            rating_key=None,
            grandparent_rating_key='519192',
            title='',
            grandparent_title='',
        )

        self.assertEqual(resolved, expected.id)

    def test_resolve_movie_history_id_by_title_when_key_missing(self):
        self._add_history(
            row_id=3001,
            server_name='Apollo',
            media_type='movie',
            title='Inception',
            full_title='Inception',
            rating_key=111,
            started=100,
        )
        expected = self._add_history(
            row_id=3002,
            server_name='Apollo',
            media_type='movie',
            title='Inception',
            full_title='Inception',
            rating_key=222,
            started=200,
        )

        resolved = AnalyticsService()._resolve_history_id_for_stream(
            server_name='Apollo',
            media_type='movie',
            rating_key=None,
            grandparent_rating_key=None,
            title='Inception',
            grandparent_title='',
        )

        self.assertEqual(resolved, expected.id)

    def test_resolve_episode_media_id_by_show_title(self):
        expected = self._add_media(
            media_type='show',
            title='Family Guy',
            play_count=0,
        )

        resolved = AnalyticsService()._resolve_media_id_for_stream(
            media_type='episode',
            title='Road to the North Pole',
            grandparent_title='Family Guy',
            year=None,
        )

        self.assertEqual(resolved, expected.id)

    def test_resolve_movie_media_id_prefers_exact_year(self):
        self._add_media(
            media_type='movie',
            title='Dune',
            year=1984,
            play_count=0,
            added_at=100,
        )
        expected = self._add_media(
            media_type='movie',
            title='Dune',
            year=2021,
            play_count=0,
            added_at=200,
        )

        resolved = AnalyticsService()._resolve_media_id_for_stream(
            media_type='movie',
            title='Dune',
            grandparent_title='',
            year='2021',
        )

        self.assertEqual(resolved, expected.id)

    def test_resolve_movie_media_id_falls_back_to_latest_added(self):
        older = self._add_media(
            media_type='movie',
            title='King Kong',
            year=1933,
            play_count=0,
            added_at=100,
        )
        newer = self._add_media(
            media_type='movie',
            title='King Kong',
            year=2005,
            play_count=0,
            added_at=200,
        )

        resolved = AnalyticsService()._resolve_media_id_for_stream(
            media_type='movie',
            title='King Kong',
            grandparent_title='',
            year=None,
        )

        self.assertEqual(resolved, newer.id)
        self.assertNotEqual(resolved, older.id)

    def test_parse_session_includes_geo_coordinates_and_location_label(self):
        session = {
            'ip_address': '8.8.8.8',
            'media_type': 'movie',
            'title': 'Heat',
            'full_title': 'Heat',
            'year': '1995',
            'friendly_name': 'Alice',
            'state': 'playing',
            'progress_percent': 42,
            'platform': 'Web',
            'product': 'Chrome',
            'transcode_decision': 'direct play',
            'stream_video_full_resolution': '1080p',
            'bandwidth': '8000',
        }
        geo_service = SimpleNamespace(
            lookup_ip=lambda ip: {
                'city': 'Los Angeles',
                'region': 'California',
                'country': 'United States',
                'isp': 'Example ISP',
                'latitude': 34.0522,
                'longitude': -118.2437,
            }
        )

        parsed = AnalyticsService()._parse_session(
            session,
            SimpleNamespace(name='Apollo', ip_address='192.168.1.228:8181'),
            'server-a',
            geo_service=geo_service,
        )

        self.assertEqual(parsed['location'], 'Los Angeles, California')
        self.assertEqual(parsed['geo_city'], 'Los Angeles')
        self.assertEqual(parsed['geo_region'], 'California')
        self.assertEqual(parsed['geo_country'], 'United States')
        self.assertEqual(parsed['geo_lat'], 34.0522)
        self.assertEqual(parsed['geo_lon'], -118.2437)
        self.assertTrue(parsed['is_mappable'])

    @patch('flask_app.services.analytics_service.ConfigService.get_server_configs')
    @patch('multiplex_stats.TautulliClient')
    def test_get_all_users_groups_by_username_and_uses_latest_friendly_name(
        self,
        mock_client_cls,
        mock_get_server_configs,
    ):
        server_a = SimpleNamespace(name='Apollo', ip_address='192.168.1.228:8181')
        server_b = SimpleNamespace(name='ApolloSS', ip_address='192.168.1.214:8181')
        mock_get_server_configs.return_value = (server_a, server_b)

        self._add_history(
            row_id=101,
            server_name='Apollo',
            server_order=0,
            user='pdti7',
            started=100,
        )
        self._add_history(
            row_id=1_000_000_101,
            server_name='ApolloSS',
            server_order=1,
            user='pdti7',
            started=200,
        )

        def build_response(items):
            return {'response': {'data': items}}

        class FakeClient:
            def __init__(self, config):
                self.name = config.name

            def get_users(self):
                if self.name == 'Apollo':
                    return build_response([{
                        'user_id': 42,
                        'username': 'pdti7',
                        'friendly_name': 'PDTI Old',
                        'email': 'pdti7@example.com',
                        'shared_libraries': ['Movies'],
                        'is_active': 1,
                    }])
                return build_response([{
                    'user_id': 42,
                    'username': 'pdti7',
                    'friendly_name': 'PDTI New',
                    'email': 'pdti7@example.com',
                    'shared_libraries': ['Movies', 'TV Shows'],
                    'is_active': 1,
                }])

            def get_library_user_stats(self, section_id):
                if section_id != 1:
                    return build_response([])

                if self.name == 'Apollo':
                    return build_response([{
                        'user_id': 42,
                        'friendly_name': 'PDTI Old',
                        'total_plays': 5,
                    }])
                return build_response([{
                    'user_id': 42,
                    'friendly_name': 'PDTI New',
                    'total_plays': 7,
                }])

        mock_client_cls.side_effect = lambda config: FakeClient(config)

        users = AnalyticsService().get_all_users()
        self.assertEqual(len(users), 1)

        user = users[0]
        self.assertEqual(user['username'], 'pdti7')
        self.assertEqual(user['friendly_name'], 'PDTI New')
        self.assertEqual(user['total_plays'], 12)
        self.assertEqual(user['server_a_plays'], 5)
        self.assertEqual(user['server_b_plays'], 7)
        self.assertEqual(user['first_play'], 100)
        self.assertEqual(user['last_play'], 200)

    def test_normalize_cached_charts_replaces_invalid_payloads(self):
        cached = {
            'daily': '<div>unexpected cached markup</div>',
            'monthly': {'title': 'Monthly', 'categories': [], 'series': []},
            'category': '<script>unexpected renderer output</script>',
            'movies': {'title': 'Movies', 'categories': [], 'data': []},
        }

        sanitized = AnalyticsService._normalize_cached_charts(cached)

        self.assertIsNone(sanitized['daily'])
        self.assertIsNone(sanitized['category'])
        self.assertEqual(sanitized['monthly']['title'], 'Monthly')
        self.assertEqual(sanitized['movies']['title'], 'Movies')
