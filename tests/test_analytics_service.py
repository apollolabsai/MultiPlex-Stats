import unittest

from flask import Flask

from flask_app.models import ViewingHistory, db
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

