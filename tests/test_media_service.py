import unittest
import threading
from types import SimpleNamespace
from unittest.mock import patch

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

    def _add_show(self, title: str, season_count: int = 0, episode_count: int = 0):
        media = CachedMedia(
            media_type='show',
            title=title,
            play_count=0,
            season_count=season_count,
            episode_count=episode_count,
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

    def test_get_tv_shows_includes_season_and_episode_counts(self):
        self._add_show('Family Guy', season_count=23, episode_count=432)
        rows = MediaService().get_tv_shows()
        self.assertEqual(rows[0]['season_count'], 23)
        self.assertEqual(rows[0]['episode_count'], 432)

    def test_process_export_data_derives_show_counts_and_size(self):
        export_data = [{
            'title': 'Sample Show',
            'addedAt': '2024-01-01T00:00:00Z',
            'seasons': [
                {
                    'episodes': [
                        {'media': [{'parts': [{'size': 100}, {'size': 50}]}]},
                        {'media': [{'parts': [{'size': 25}]}]},
                    ],
                },
                {
                    'episodes': [
                        {'media': [{'parts': [{'size': 75}]}]},
                    ],
                },
            ],
        }]

        data_dict = {}
        MediaService()._process_export_data_parallel(
            export_data=export_data,
            media_type='show',
            data_dict=data_dict,
            data_lock=threading.Lock(),
            is_primary=True,
            server_key='a',
        )

        self.assertIn('Sample Show', data_dict)
        show = data_dict['Sample Show']
        self.assertEqual(show['season_count'], 2)
        self.assertEqual(show['episode_count'], 3)
        self.assertEqual(show['file_size'], 250)

    def test_process_export_data_keeps_largest_show_size_across_servers(self):
        service = MediaService()
        data_dict = {}
        data_lock = threading.Lock()

        smaller_export = [{
            'title': 'Sample Show',
            'seasons': [
                {
                    'episodes': [
                        {'media': [{'parts': [{'size': 100}]}]},
                    ],
                },
            ],
        }]
        larger_export = [{
            'title': 'Sample Show',
            'seasons': [
                {
                    'episodes': [
                        {'media': [{'parts': [{'size': 250}]}]},
                    ],
                },
            ],
        }]

        service._process_export_data_parallel(
            export_data=smaller_export,
            media_type='show',
            data_dict=data_dict,
            data_lock=data_lock,
            is_primary=True,
            server_key='a',
        )
        service._process_export_data_parallel(
            export_data=larger_export,
            media_type='show',
            data_dict=data_dict,
            data_lock=data_lock,
            is_primary=False,
            server_key='b',
        )

        show = data_dict['Sample Show']
        self.assertEqual(show['file_size'], 250)
        self.assertEqual(show['season_count'], 1)
        self.assertEqual(show['episode_count'], 1)

    def test_tv_play_stats_do_not_overwrite_export_file_size(self):
        class StubClient:
            @staticmethod
            def get_library_media_info(section_id, length=25000, refresh=False):
                return {
                    'response': {
                        'data': {
                            'data': [
                                {
                                    'title': 'Sample Show',
                                    'file_size': 999,
                                    'play_count': 4,
                                    'last_played': 12345,
                                }
                            ]
                        }
                    }
                }

        data_dict = {
            'Sample Show': {
                'title': 'Sample Show',
                'year': None,
                'file_size': 250,
                'play_count': 1,
                'season_count': 2,
                'episode_count': 3,
                'added_at': 0,
                'last_played': 0,
                'video_codecs': set(),
                'video_resolutions': set(),
                'file_sizes': {250},
                'rating': None,
                'rating_image': None,
                'audience_rating': None,
                'audience_rating_image': None,
                'imdb_id': None,
                'tmdb_id': None,
            }
        }

        MediaService()._fetch_library_play_stats_parallel(
            client=StubClient(),
            section_id=1,
            media_type='show',
            data_dict=data_dict,
            data_lock=threading.Lock(),
        )

        show = data_dict['Sample Show']
        self.assertEqual(show['file_size'], 250)
        self.assertEqual(show['play_count'], 5)
        self.assertEqual(show['last_played'], 12345)

    def test_build_progress_steps_always_include_mdblist_step(self):
        steps = MediaService._build_progress_steps(
            SimpleNamespace(name='Server A'),
            None,
        )
        step_ids = [step['id'] for step in steps]
        self.assertIn('media-mdblist', step_ids)

    def test_export_progress_detail_includes_counts_and_elapsed(self):
        detail = MediaService._export_progress_detail('TV Shows', 800, 2450, 604)
        self.assertEqual(detail, 'Current library TV Shows: 800 / 2,450 items (604s)')

    def test_tv_export_uses_reduced_metadata_payload(self):
        service = MediaService()
        progress_step_id = service._step_id('a', 'tv-export')

        class StubClient:
            def __init__(self):
                self.export_kwargs = None

            def export_metadata(self, **kwargs):
                self.export_kwargs = kwargs
                return {'response': {'data': {'export_id': 123}}}

        client = StubClient()

        with patch.object(MediaService, '_wait_for_export_parallel', return_value=[]), patch.object(
            MediaService,
            '_process_export_data_parallel',
            return_value=None,
        ):
            service._fetch_library_via_export_parallel(
                client=client,
                server_name='Apollo',
                section_id=2,
                section_name='TV Shows',
                media_type='show',
                movies_data={},
                tv_data={},
                data_lock=threading.Lock(),
                is_primary=True,
                server_key='a',
                progress_step_id=progress_step_id,
                completed_step_items=0,
                total_step_items=1415,
                library_item_count=1415,
            )

        self.assertIsNotNone(client.export_kwargs)
        self.assertEqual(client.export_kwargs['metadata_level'], 1)
        self.assertEqual(client.export_kwargs['media_info_level'], 2)

    def test_get_movies_always_includes_media_id(self):
        movie = self._add_movie('No History Movie', 2024)
        rows = MediaService().get_movies()
        self.assertEqual(rows[0]['media_id'], movie.id)
