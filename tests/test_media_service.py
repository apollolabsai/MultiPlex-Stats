import unittest
import threading
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import flask_app.services.media_service as media_service_module
from flask_app.models import CachedMedia, MediaSyncStatus, MediaTechnicalCache, db
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
        MediaTechnicalCache.query.delete()
        MediaSyncStatus.query.delete()
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

    def test_process_export_data_reads_top_level_show_counters(self):
        export_data = [{
            'title': 'Counter Show',
            'seasonCount': 5,
            'leafCount': 62,
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

        show = data_dict['Counter Show']
        self.assertEqual(show['season_count'], 5)
        self.assertEqual(show['episode_count'], 62)
        self.assertEqual(show['file_size'], 0)

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

    def test_process_export_data_sums_movie_versions_after_source_enrichment(self):
        service = MediaService()
        data_dict = {}
        data_lock = threading.Lock()

        for size, codec, resolution, server_key in (
            (100, 'h264', '1080', 'a'),
            (250, 'hevc', '4k', 'b'),
        ):
            service._process_export_data_parallel(
                export_data=[{
                    'title': 'Versioned Movie',
                    'year': 2026,
                    '_technical_file_size': size,
                    '_technical_file_sizes': [size],
                    '_technical_video_codecs': [codec],
                    '_technical_video_resolutions': [resolution],
                }],
                media_type='movie',
                data_dict=data_dict,
                data_lock=data_lock,
                is_primary=server_key == 'a',
                server_key=server_key,
            )

        movie = data_dict[('Versioned Movie', 2026)]
        self.assertEqual(movie['file_size'], 350)
        self.assertEqual(movie['file_sizes'], {100, 250})
        self.assertEqual(movie['video_codecs'], {'h264', 'hevc'})
        self.assertEqual(movie['video_resolutions'], {'1080', '4k'})

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

    def test_movie_play_stats_do_not_double_count_enriched_technical_values(self):
        movie_data = {
            ('Sample Movie', 2026): {
                'title': 'Sample Movie',
                'year': 2026,
                'file_size': 250,
                'play_count': 1,
                'season_count': 0,
                'episode_count': 0,
                'added_at': 0,
                'last_played': 0,
                'video_codecs': {'h264'},
                'video_resolutions': {'1080'},
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
            client=SimpleNamespace(),
            section_id=1,
            media_type='movie',
            data_dict=movie_data,
            data_lock=threading.Lock(),
            items=[{
                'title': 'Sample Movie',
                'year': '2026',
                'file_size': 999,
                'video_codec': 'hevc',
                'video_resolution': '4k',
                'play_count': 4,
                'last_played': 12345,
            }],
            include_movie_technical=False,
        )

        movie = movie_data[('Sample Movie', 2026)]
        self.assertEqual(movie['file_size'], 250)
        self.assertEqual(movie['file_sizes'], {250})
        self.assertEqual(movie['video_codecs'], {'h264'})
        self.assertEqual(movie['video_resolutions'], {'1080'})
        self.assertEqual(movie['play_count'], 5)

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

    def test_tv_export_uses_custom_fields_payload(self):
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
        self.assertEqual(client.export_kwargs['metadata_level'], 0)
        self.assertEqual(client.export_kwargs['media_info_level'], 0)
        self.assertEqual(
            client.export_kwargs['custom_fields'],
            [
                'title',
                'addedAt',
                'rating',
                'audienceRating',
                'audienceRatingImage',
                'guid',
                'guids',
                'seasons.episodes.media.parts.size',
                'seasons.episodes.media.parts.sizeHuman',
            ],
        )

    def test_movie_export_includes_lightweight_change_marker(self):
        service = MediaService()

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
                section_id=1,
                section_name='Movies',
                media_type='movie',
                movies_data={},
                tv_data={},
                data_lock=threading.Lock(),
                is_primary=True,
                server_key='a',
                progress_step_id=service._step_id('a', 'movie-export'),
                completed_step_items=0,
                total_step_items=10,
                library_item_count=10,
            )

        self.assertIn('updatedAt', client.export_kwargs['custom_fields'])
        self.assertEqual(client.export_kwargs['media_info_level'], 0)

    def test_section_cache_seeds_scoped_technical_cache_without_targeted_request(self):
        service = MediaService()
        server = SimpleNamespace(
            name='Apollo',
            server_config_id=1,
        )

        class StubClient:
            @staticmethod
            def get_metadata(rating_key):
                raise AssertionError('targeted lookup should not be used')

        record = {
            'ratingKey': '10',
            'title': 'Cached Movie',
            'year': 2026,
            'guid': 'plex://movie/cached',
            'guids': [{'id': 'imdb://tt0000010'}],
            'updatedAt': '100',
        }
        state = {
            'started_at': None,
            'requests_used': 0,
            'section_cache_hits': 0,
            'local_cache_hits': 0,
            'fingerprint_mismatches': 0,
            'targeted_successes': 0,
            'targeted_failures': 0,
            'budget_skips': 0,
        }
        seen = set()

        service._enrich_movie_export_records(
            client=StubClient(),
            server_config=server,
            server_identifier='machine-a',
            section_id=1,
            export_data=[record],
            section_cache_items=[{
                'rating_key': '10',
                'title': 'Cached Movie',
                'year': '2026',
                'file_size': 1234,
                'video_codec': 'h264',
                'video_resolution': '1080',
            }],
            enrichment_state=state,
            seen_technical_locators=seen,
            technical_locator_lock=threading.Lock(),
        )

        self.assertEqual(record['_technical_file_size'], 1234)
        self.assertEqual(state['section_cache_hits'], 1)
        self.assertEqual(state['requests_used'], 0)
        self.assertEqual(seen, {(1, '1', 'movie', '10')})
        cached = MediaTechnicalCache.query.one()
        self.assertEqual(cached.plex_guid, 'plex://movie/cached')
        self.assertEqual(cached.server_identifier, 'machine-a')

        second_record = dict(record)
        for key in list(second_record):
            if key.startswith('_technical_'):
                second_record.pop(key)
        service._enrich_movie_export_records(
            client=StubClient(),
            server_config=server,
            server_identifier='machine-a',
            section_id=1,
            export_data=[second_record],
            section_cache_items=[],
            enrichment_state=state,
            seen_technical_locators=seen,
            technical_locator_lock=threading.Lock(),
        )
        self.assertEqual(second_record['_technical_file_size'], 1234)
        self.assertEqual(state['local_cache_hits'], 1)

    def test_title_change_uses_targeted_metadata_and_persists_result(self):
        service = MediaService()
        server = SimpleNamespace(name='ApolloSS', server_config_id=2)

        class StubClient:
            calls = []

            @classmethod
            def get_metadata(cls, rating_key):
                cls.calls.append(str(rating_key))
                return {
                    'response': {
                        'data': {
                            'media_info': [{
                                'video_codec': 'h264',
                                'video_resolution': '1080',
                                'parts': [{'file_size': 4321}],
                            }]
                        }
                    }
                }

        record = {
            'ratingKey': '20',
            'title': 'Current Title',
            'year': 2026,
            'guid': 'plex://movie/current',
            'updatedAt': '200',
        }
        state = {
            'started_at': None,
            'requests_used': 0,
            'section_cache_hits': 0,
            'local_cache_hits': 0,
            'fingerprint_mismatches': 0,
            'targeted_successes': 0,
            'targeted_failures': 0,
            'budget_skips': 0,
        }

        service._enrich_movie_export_records(
            client=StubClient(),
            server_config=server,
            server_identifier='machine-b',
            section_id=2,
            export_data=[record],
            section_cache_items=[{
                'rating_key': '20',
                'title': 'Old Title',
                'year': '2026',
                'file_size': 111,
                'video_codec': 'old',
                'video_resolution': 'sd',
            }],
            enrichment_state=state,
            seen_technical_locators=set(),
            technical_locator_lock=threading.Lock(),
        )

        self.assertEqual(StubClient.calls, ['20'])
        self.assertEqual(record['_technical_file_size'], 4321)
        self.assertEqual(state['targeted_successes'], 1)
        cached = MediaTechnicalCache.query.one()
        self.assertEqual(cached.title, 'Current Title')
        self.assertEqual(cached.file_size, 4321)

    def test_fingerprint_mismatch_replaces_stale_locator(self):
        db.session.add(MediaTechnicalCache(
            server_config_id=1,
            server_identifier='machine-a',
            server_name='Apollo',
            section_id='1',
            media_type='movie',
            rating_key='30',
            plex_guid='plex://movie/old',
            title='Old Movie',
            year=1990,
            plex_updated_at='100',
            file_size=999,
            file_size_versions='[999]',
            video_codecs='["old"]',
            video_resolutions='["sd"]',
        ))
        db.session.commit()

        class StubClient:
            @staticmethod
            def get_metadata(rating_key):
                return {
                    'response': {
                        'data': {
                            'media_info': [{
                                'video_codec': 'hevc',
                                'video_resolution': '4k',
                                'parts': [{'file_size': 5555}],
                            }]
                        }
                    }
                }

        record = {
            'ratingKey': '30',
            'title': 'New Movie',
            'year': 2026,
            'guid': 'plex://movie/new',
            'updatedAt': '300',
        }
        state = {
            'started_at': None,
            'requests_used': 0,
            'section_cache_hits': 0,
            'local_cache_hits': 0,
            'fingerprint_mismatches': 0,
            'targeted_successes': 0,
            'targeted_failures': 0,
            'budget_skips': 0,
        }
        MediaService()._enrich_movie_export_records(
            client=StubClient(),
            server_config=SimpleNamespace(name='Apollo', server_config_id=1),
            server_identifier='machine-a',
            section_id=1,
            export_data=[record],
            section_cache_items=[],
            enrichment_state=state,
            seen_technical_locators=set(),
            technical_locator_lock=threading.Lock(),
        )

        self.assertEqual(state['fingerprint_mismatches'], 1)
        self.assertEqual(record['_technical_file_size'], 5555)
        cached = MediaTechnicalCache.query.one()
        self.assertEqual(cached.plex_guid, 'plex://movie/new')
        self.assertEqual(cached.file_size, 5555)

    def test_failed_tv_export_is_logged_and_marked_failed(self):
        service = MediaService()
        service._progress_tracker.reset(
            service._build_progress_steps(
                SimpleNamespace(name='Apollo'),
                SimpleNamespace(name='ApolloSS'),
            )
        )

        status = service.get_or_create_status()
        status.server_b_name = 'ApolloSS'
        status.server_b_status = 'running'
        db.session.commit()

        class StubClient:
            config = SimpleNamespace(name='ApolloSS')

            @staticmethod
            def get_exports_table(section_id):
                return {
                    'response': {
                        'data': {
                            'data': [
                                {
                                    'export_id': 167,
                                    'complete': -1,
                                    'exported_items': 1413,
                                    'total_items': 1419,
                                }
                            ]
                        }
                    }
                }

        with patch.object(media_service_module.logger, 'error') as mock_log_error:
            with self.assertRaises(ValueError) as ctx:
                service._wait_for_export_parallel(
                    client=StubClient(),
                    section_id=1,
                    export_id=167,
                    section_name='TV Shows',
                    media_type='show',
                    server_key='b',
                    progress_step_id=service._step_id('b', 'tv-export'),
                    completed_step_items=0,
                    total_step_items=1499,
                    library_item_count=1419,
                )

        self.assertIn('processed 1,413/1,419 items', str(ctx.exception))
        failed_step = service._progress_tracker.get_step(service._step_id('b', 'tv-export'))
        self.assertEqual(failed_step['status'], 'failed')
        self.assertEqual(failed_step['detail'], 'TV Shows: export failed at 1,413 / 1,419 items')

        status = service.get_or_create_status()
        self.assertEqual(status.server_b_step, 'TV Shows: export failed at 1,413 / 1,419 items')
        mock_log_error.assert_called_once()

    def test_export_retries_after_failed_attempt(self):
        service = MediaService()
        service._progress_tracker.reset(
            service._build_progress_steps(SimpleNamespace(name='Apollo'), None)
        )

        class StubClient:
            def __init__(self):
                self.export_ids = []

            def export_metadata(self, **kwargs):
                export_id = len(self.export_ids) + 1
                self.export_ids.append(export_id)
                return {'response': {'data': {'export_id': export_id}}}

        client = StubClient()

        with patch.object(service, '_wait_for_export_parallel') as mock_wait, patch.object(
            service,
            '_process_export_data_parallel',
            return_value=None,
        ) as mock_process, patch.object(
            media_service_module.time,
            'sleep',
            return_value=None,
        ), patch.object(media_service_module.logger, 'warning'):
            mock_wait.side_effect = [
                ValueError("Export failed for Movies on Tautulli's side"),
                [{'title': 'Recovered Movie'}],
            ]

            service._fetch_library_via_export_parallel(
                client=client,
                server_name='Apollo',
                section_id=2,
                section_name='Movies',
                media_type='movie',
                movies_data={},
                tv_data={},
                data_lock=threading.Lock(),
                is_primary=True,
                server_key='a',
                progress_step_id=service._step_id('a', 'movie-export'),
                completed_step_items=0,
                total_step_items=10,
                library_item_count=10,
            )

        self.assertEqual(client.export_ids, [1, 2])
        self.assertEqual(mock_wait.call_count, 2)
        self.assertFalse(mock_wait.call_args_list[0].kwargs['mark_failed'])
        self.assertFalse(mock_wait.call_args_list[1].kwargs['mark_failed'])
        mock_process.assert_called_once()

    def test_secondary_server_failure_keeps_existing_cache(self):
        self._add_movie('Existing Movie', 1999)
        service = MediaService()

        server_a = SimpleNamespace(name='Apollo')
        server_b = SimpleNamespace(name='ApolloSS')

        def fake_fetch(
            server_config,
            movies_data,
            tv_data,
            data_lock,
            is_primary,
            server_key,
            seen_technical_locators=None,
            technical_locator_lock=None,
        ):
            if server_key == 'b':
                raise ValueError('ApolloSS export failed')
            movies_data[('New Movie', 2026)] = {
                'title': 'New Movie',
                'year': 2026,
                'file_size': 0,
                'play_count': 0,
                'season_count': 0,
                'episode_count': 0,
                'added_at': 0,
                'last_played': 0,
                'video_codecs': set(),
                'video_resolutions': set(),
                'file_sizes': set(),
                'rating': None,
                'rating_image': None,
                'audience_rating': None,
                'audience_rating_image': None,
                'imdb_id': None,
                'tmdb_id': None,
            }

        with patch.object(
            media_service_module.ConfigService,
            'get_server_configs',
            return_value=(server_a, server_b),
        ), patch.object(
            service,
            '_fetch_server_media_parallel',
            side_effect=fake_fetch,
        ), patch.object(media_service_module.logger, 'exception'):
            with self.assertRaises(ValueError) as ctx:
                service._run_media_sync_parallel(self.app)

        self.assertIn('Media refresh incomplete; keeping previous cached media', str(ctx.exception))
        titles = [row.title for row in CachedMedia.query.order_by(CachedMedia.title).all()]
        self.assertEqual(titles, ['Existing Movie'])

    def test_get_movies_always_includes_media_id(self):
        movie = self._add_movie('No History Movie', 2024)
        rows = MediaService().get_movies()
        self.assertEqual(rows[0]['media_id'], movie.id)

    def test_get_sync_status_recovers_stale_running_media_sync(self):
        db.session.add(MediaSyncStatus(
            status='running',
            started_at=media_service_module.datetime(2000, 1, 1),
            current_step='Fetching from servers...',
            server_a_name='Apollo',
            server_a_status='running',
            server_a_step='Discovering libraries...',
        ))
        db.session.commit()

        status = MediaService().get_sync_status()

        self.assertEqual(status['status'], 'failed')
        self.assertIn('Recovered stale media refresh', status['error_message'])
        self.assertEqual(status['pipeline_items'], [])

        refreshed = MediaSyncStatus.query.first()
        self.assertEqual(refreshed.status, 'failed')
        self.assertEqual(refreshed.server_a_status, 'failed')
