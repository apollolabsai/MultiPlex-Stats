import unittest

from flask import Flask

from flask_app.models import LifetimeMediaPlayCount, ViewingHistory, db
from flask_app.services.media_lifetime_stats_service import MediaLifetimeStatsService


class MediaLifetimeStatsServiceTests(unittest.TestCase):
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
        LifetimeMediaPlayCount.query.delete()
        ViewingHistory.query.delete()
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()

    def test_apply_cached_play_counts_overrides_movie_and_show(self):
        db.session.add(LifetimeMediaPlayCount(
            media_type='movie',
            title_normalized='inception',
            year=2010,
            total_plays=42,
        ))
        db.session.add(LifetimeMediaPlayCount(
            media_type='show',
            title_normalized='family guy',
            year=None,
            total_plays=503,
        ))
        db.session.commit()

        movies = [{'content_title': 'Inception', 'content_year': 2010, 'play_count': 10}]
        shows = [{'content_title': 'Family Guy', 'content_year': None, 'play_count': 275}]

        out_movies, out_shows = MediaLifetimeStatsService().apply_cached_play_counts(movies, shows)

        self.assertEqual(out_movies[0]['play_count'], 42)
        self.assertEqual(out_shows[0]['play_count'], 503)

    def test_apply_cached_play_counts_sums_movie_variants_when_year_missing(self):
        db.session.add(LifetimeMediaPlayCount(
            media_type='movie',
            title_normalized='it',
            year=1990,
            total_plays=12,
        ))
        db.session.add(LifetimeMediaPlayCount(
            media_type='movie',
            title_normalized='it',
            year=2017,
            total_plays=34,
        ))
        db.session.commit()

        movies = [{'content_title': 'It', 'content_year': None, 'play_count': 2}]
        out_movies, _ = MediaLifetimeStatsService().apply_cached_play_counts(movies, [])
        self.assertEqual(out_movies[0]['play_count'], 46)

    def test_extract_content_key_handles_movie_and_episode(self):
        movie_key = MediaLifetimeStatsService._extract_content_key({
            'media_type': 'movie',
            'title': 'Inception',
            'year': '2010',
        })
        show_key = MediaLifetimeStatsService._extract_content_key({
            'media_type': 'episode',
            'grandparent_title': 'Family Guy',
        })

        self.assertEqual(movie_key, ('movie', 'inception', 2010))
        self.assertEqual(show_key, ('show', 'family guy', None))

    def test_scan_server_history_uses_local_viewing_history_rows(self):
        db.session.add(ViewingHistory(
            row_id=1,
            server_name='Server A',
            server_order=0,
            media_type='movie',
            title='Inception',
            year=2010,
        ))
        db.session.add(ViewingHistory(
            row_id=2,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            grandparent_title='Family Guy',
        ))
        db.session.add(ViewingHistory(
            row_id=3,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            grandparent_title='Family Guy',
        ))
        db.session.add(ViewingHistory(
            row_id=4,
            server_name='Server B',
            server_order=1,
            media_type='movie',
            title='Alien',
            year=1979,
        ))
        db.session.commit()

        counts = MediaLifetimeStatsService()._scan_server_history('Server A', 'a')

        self.assertEqual(counts.get(('movie', 'inception', 2010)), 1)
        self.assertEqual(counts.get(('show', 'family guy', None)), 2)
        self.assertNotIn(('movie', 'alien', 1979), counts)
