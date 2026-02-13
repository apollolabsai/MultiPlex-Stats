import unittest

from flask import Flask

from flask_app.models import CachedMedia, db
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

    def test_get_movies_includes_media_id_and_title_format(self):
        movie = self._add_movie('Inception', 2010)
        rows = MediaService().get_movies()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['media_id'], movie.id)
        self.assertEqual(rows[0]['title'], 'Inception (2010)')
        self.assertNotIn('history_id', rows[0])

    def test_get_movies_without_year_preserves_title(self):
        movie = self._add_movie('Alien', None)
        rows = MediaService().get_movies()
        self.assertEqual(rows[0]['media_id'], movie.id)
        self.assertEqual(rows[0]['title'], 'Alien')
        self.assertNotIn('history_id', rows[0])

    def test_get_tv_shows_includes_media_id(self):
        show = self._add_show('Family Guy')
        rows = MediaService().get_tv_shows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['media_id'], show.id)
        self.assertEqual(rows[0]['title'], 'Family Guy')
        self.assertNotIn('history_id', rows[0])

    def test_get_movies_always_includes_media_id(self):
        movie = self._add_movie('No History Movie', 2024)
        rows = MediaService().get_movies()
        self.assertEqual(rows[0]['media_id'], movie.id)
