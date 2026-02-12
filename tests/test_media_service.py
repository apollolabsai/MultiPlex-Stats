import unittest

from flask import Flask

from flask_app.models import CachedMedia, ViewingHistory, db
from flask_app.services.media_service import MediaService


class MediaServiceLinkTests(unittest.TestCase):
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

    def _add_movie(self, title: str, year: int | None = None):
        media = CachedMedia(
            media_type='movie',
            title=title,
            year=year,
            play_count=0,
        )
        db.session.add(media)
        db.session.commit()
        return media

    def _add_show(self, title: str):
        media = CachedMedia(
            media_type='show',
            title=title,
            play_count=0,
        )
        db.session.add(media)
        db.session.commit()
        return media

    def _add_history(self, **kwargs) -> ViewingHistory:
        kwargs.setdefault('server_name', 'Server A')
        kwargs.setdefault('server_order', 0)
        item = ViewingHistory(**kwargs)
        db.session.add(item)
        db.session.commit()
        return item

    def test_get_movies_includes_history_id_for_matching_title_and_year(self):
        self._add_movie('Inception', 2010)
        older = self._add_history(
            row_id=1001,
            media_type='movie',
            title='Inception',
            full_title='Inception',
            year=2010,
            started=100,
        )
        newer = self._add_history(
            row_id=1002,
            media_type='movie',
            title='Inception',
            full_title='Inception',
            year=2010,
            started=200,
        )

        rows = MediaService().get_movies()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['history_id'], newer.id)
        self.assertNotEqual(rows[0]['history_id'], older.id)

    def test_get_movies_falls_back_to_title_when_year_missing(self):
        self._add_movie('Alien', None)
        expected = self._add_history(
            row_id=1101,
            media_type='movie',
            title='Alien',
            full_title='Alien',
            year=1979,
            started=300,
        )

        rows = MediaService().get_movies()

        self.assertEqual(rows[0]['history_id'], expected.id)

    def test_get_tv_shows_includes_history_id_for_matching_show(self):
        self._add_show('Family Guy')
        older = self._add_history(
            row_id=2001,
            media_type='episode',
            grandparent_title='Family Guy',
            title='Episode One',
            started=100,
        )
        newer = self._add_history(
            row_id=2002,
            media_type='episode',
            grandparent_title='Family Guy',
            title='Episode Two',
            started=200,
        )

        rows = MediaService().get_tv_shows()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['history_id'], newer.id)
        self.assertNotEqual(rows[0]['history_id'], older.id)

    def test_get_tv_shows_history_id_none_when_no_match(self):
        show = self._add_show('Unknown Show')
        rows = MediaService().get_tv_shows()
        self.assertIsNone(rows[0]['history_id'])
        self.assertEqual(rows[0]['media_id'], show.id)

    def test_get_movies_always_includes_media_id(self):
        movie = self._add_movie('No History Movie', 2024)
        rows = MediaService().get_movies()
        self.assertEqual(rows[0]['media_id'], movie.id)
