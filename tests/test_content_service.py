import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from flask import Flask

from flask_app.models import CachedMedia, LifetimeMediaPlayCount, db, ServerConfig, ViewingHistory
from flask_app.services.content_service import ContentService


class ContentServiceChartTests(unittest.TestCase):
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
        LifetimeMediaPlayCount.query.delete()
        ViewingHistory.query.delete()
        ServerConfig.query.delete()
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
            is_active=True
        ))
        db.session.commit()

    def _add_history(self, **kwargs) -> ViewingHistory:
        record = ViewingHistory(**kwargs)
        db.session.add(record)
        db.session.commit()
        return record

    def _add_cached_media(self, **kwargs) -> CachedMedia:
        record = CachedMedia(**kwargs)
        db.session.add(record)
        db.session.commit()
        return record

    def _add_lifetime_count(
        self,
        media_type: str,
        title_normalized: str,
        total_plays: int,
        year: int | None = None,
    ) -> LifetimeMediaPlayCount:
        row = LifetimeMediaPlayCount(
            media_type=media_type,
            title_normalized=title_normalized,
            year=year,
            total_plays=total_plays,
        )
        db.session.add(row)
        db.session.commit()
        return row

    def test_plays_by_year_chart_with_single_server(self):
        self._add_server('Server A', 0)

        plays = [
            ViewingHistory(server_name='Server A', date_played=date(2024, 1, 10)),
            ViewingHistory(server_name='Server A', date_played=date(2024, 5, 11)),
            ViewingHistory(server_name='Server A', date_played=date(2025, 2, 1)),
        ]

        service = ContentService()
        chart = service._build_plays_by_year_chart(plays, 'Example')

        self.assertEqual(chart['categories'], ['2024', '2025'])
        self.assertEqual(len(chart['series']), 1)
        self.assertEqual(chart['series'][0]['name'], 'Server A')
        self.assertEqual(chart['series'][0]['data'], [2, 1])
        self.assertEqual(chart['totals'], [2, 1])
        self.assertEqual(chart['overall_total'], 3)

    def test_plays_by_year_chart_with_two_servers(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)

        plays = [
            ViewingHistory(server_name='Server A', date_played=date(2024, 1, 10)),
            ViewingHistory(server_name='Server B', date_played=date(2024, 3, 10)),
            ViewingHistory(server_name='Server B', date_played=date(2025, 4, 10)),
        ]

        service = ContentService()
        chart = service._build_plays_by_year_chart(plays, 'Example')

        self.assertEqual(chart['categories'], ['2024', '2025'])
        self.assertEqual(len(chart['series']), 2)
        self.assertEqual(chart['series'][0]['name'], 'Server A')
        self.assertEqual(chart['series'][1]['name'], 'Server B')
        self.assertEqual(chart['series'][0]['data'], [1, 0])
        self.assertEqual(chart['series'][1]['data'], [1, 1])
        self.assertEqual(chart['totals'], [2, 1])
        self.assertEqual(chart['overall_total'], 3)

    def test_plays_by_user_chart(self):
        plays = [
            ViewingHistory(user='Alice'),
            ViewingHistory(user='alice'),
            ViewingHistory(user='Bob'),
            ViewingHistory(user='Bob'),
            ViewingHistory(user='Carol'),
            ViewingHistory(user=''),
            ViewingHistory(user=None),
        ]

        service = ContentService()
        chart = service._build_plays_by_user_chart(plays, 'Family Guy')

        self.assertEqual(chart['categories'], ['Alice', 'Bob', 'Carol'])
        self.assertEqual([point['y'] for point in chart['data']], [2, 2, 1])
        self.assertTrue(all('color' in point for point in chart['data']))
        self.assertEqual(chart['overall_total'], 5)

    def test_content_details_uses_local_lifetime_totals_across_servers(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)
        self._add_lifetime_count(
            media_type='movie',
            title_normalized='inception',
            year=2010,
            total_plays=17,
        )

        clicked = self._add_history(
            row_id=1,
            server_name='Server A',
            server_order=0,
            media_type='movie',
            title='Inception',
            full_title='Inception',
            year=2010,
            rating_key=111,
            user='Alice',
            started=1700000000,
            date_played=date(2024, 1, 10),
        )
        self._add_history(
            row_id=2,
            server_name='Server B',
            server_order=1,
            media_type='movie',
            title='Inception',
            full_title='Inception',
            year=2010,
            rating_key=222,
            user='Bob',
            started=1700100000,
            date_played=date(2024, 1, 11),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 17)
        self.assertEqual(details['unique_users'], 2)

    def test_content_details_uses_local_lifetime_totals_for_show(self):
        self._add_server('Server A', 0)
        self._add_lifetime_count(
            media_type='show',
            title_normalized='family guy',
            total_plays=25,
        )

        clicked = self._add_history(
            row_id=10,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='And Then There Were Fewer',
            full_title='Family Guy - And Then There Were Fewer',
            grandparent_title='Family Guy',
            rating_key=3001,
            grandparent_rating_key=9001,
            user='alice',
            started=1700000000,
            date_played=date(2024, 2, 1),
        )
        self._add_history(
            row_id=11,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='Road to the North Pole',
            full_title='Family Guy - Road to the North Pole',
            grandparent_title='Family Guy',
            rating_key=3002,
            grandparent_rating_key=9001,
            user='bob',
            started=1700100000,
            date_played=date(2024, 2, 2),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 25)
        self.assertEqual(details['unique_users'], 2)

    def test_get_metadata_for_tv_uses_children_fallback_for_counts(self):
        self._add_server('Server A', 0)

        record = self._add_history(
            row_id=14,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='Pilot',
            full_title='Family Guy - Pilot',
            grandparent_title='Family Guy',
            rating_key=3100,
            grandparent_rating_key=9100,
            started=1700000000,
            date_played=date(2024, 2, 1),
        )

        client = MagicMock()
        client.get_metadata.return_value = {
            'response': {'result': 'success', 'data': {'summary': 'animated show'}}
        }
        client.get_children_metadata.return_value = {
            'response': {
                'result': 'success',
                'data': [
                    {'media_type': 'season', 'children_count': 12},
                    {'media_type': 'season', 'children_count': 14},
                ],
            }
        }

        with patch('flask_app.services.content_service.TautulliClient', return_value=client):
            metadata = ContentService()._get_metadata_for_record(record, is_movie=False)

        self.assertEqual(metadata['season_count'], 2)
        self.assertEqual(metadata['episode_count'], 26)
        client.get_children_metadata.assert_called_with(9100)

    def test_get_metadata_for_tv_combines_season_children_and_metadata_episode_total(self):
        self._add_server('Server A', 0)

        record = self._add_history(
            row_id=15,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='Pilot',
            full_title='Family Guy - Pilot',
            grandparent_title='Family Guy',
            rating_key=3101,
            grandparent_rating_key=9101,
            started=1700000000,
            date_played=date(2024, 2, 1),
        )

        client = MagicMock()
        client.get_metadata.return_value = {
            'response': {
                'result': 'success',
                'data': {
                    'summary': 'animated show',
                    'children_count': 297,
                },
            }
        }
        client.get_children_metadata.return_value = {
            'response': {
                'result': 'success',
                'data': {
                    'children_count': 18,
                    'children_type': 'season',
                },
            }
        }

        with patch('flask_app.services.content_service.TautulliClient', return_value=client):
            metadata = ContentService()._get_metadata_for_record(record, is_movie=False)

        self.assertEqual(metadata['season_count'], 18)
        self.assertEqual(metadata['episode_count'], 297)

    def test_tv_user_chart_uses_local_user_counts(self):
        self._add_server('Server A', 0)

        clicked = self._add_history(
            row_id=12,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='Episode One',
            full_title='Family Guy - Episode One',
            grandparent_title='Family Guy',
            rating_key=3101,
            grandparent_rating_key=9101,
            user='alice',
            started=1700000000,
            date_played=date(2024, 2, 1),
        )
        self._add_history(
            row_id=13,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='Episode Two',
            full_title='Family Guy - Episode Two',
            grandparent_title='Family Guy',
            rating_key=3102,
            grandparent_rating_key=9101,
            user='alice',
            started=1700100000,
            date_played=date(2024, 2, 2),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['unique_users'], 1)
        self.assertEqual(details['plays_by_user_chart']['categories'], ['alice'])
        self.assertEqual([point['y'] for point in details['plays_by_user_chart']['data']], [2])

    def test_content_details_falls_back_to_local_counts_on_endpoint_failure(self):
        self._add_server('Server A', 0)

        clicked = self._add_history(
            row_id=20,
            server_name='Server A',
            server_order=0,
            media_type='movie',
            title='Arrival',
            full_title='Arrival',
            year=2016,
            rating_key=701,
            user='User One',
            started=1700200000,
            date_played=date(2024, 3, 1),
        )
        self._add_history(
            row_id=21,
            server_name='Server A',
            server_order=0,
            media_type='movie',
            title='Arrival',
            full_title='Arrival',
            year=2016,
            rating_key=701,
            user='User Two',
            started=1700300000,
            date_played=date(2024, 3, 2),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 2)
        self.assertEqual(details['unique_users'], 2)

    def test_content_details_prefers_lifetime_total_over_local_history_len(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)
        self._add_lifetime_count(
            media_type='movie',
            title_normalized='interstellar',
            year=2014,
            total_plays=20,
        )

        clicked = self._add_history(
            row_id=30,
            server_name='Server A',
            server_order=0,
            media_type='movie',
            title='Interstellar',
            full_title='Interstellar',
            year=2014,
            rating_key=123,
            user='Alice',
            started=1700400000,
            date_played=date(2024, 3, 3),
        )
        self._add_history(
            row_id=31,
            server_name='Server B',
            server_order=1,
            media_type='movie',
            title='Interstellar',
            full_title='Interstellar',
            year=2014,
            rating_key=456,
            user='Bob',
            started=1700500000,
            date_played=date(2024, 3, 4),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 20)
        self.assertEqual(details['unique_users'], 2)

    def test_content_details_uses_lifetime_total_when_present(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)
        self._add_lifetime_count(
            media_type='movie',
            title_normalized='the matrix',
            year=1999,
            total_plays=11,
        )

        clicked = self._add_history(
            row_id=40,
            server_name='Server A',
            server_order=0,
            media_type='movie',
            title='The Matrix',
            full_title='The Matrix',
            year=1999,
            rating_key=910,
            user='Neo',
            started=1700600000,
            date_played=date(2024, 4, 1),
        )
        self._add_history(
            row_id=41,
            server_name='Server B',
            server_order=1,
            media_type='movie',
            title='The Matrix',
            full_title='The Matrix',
            year=1999,
            rating_key=920,
            user='Trinity',
            started=1700700000,
            date_played=date(2024, 4, 2),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 11)
        self.assertEqual(details['unique_users'], 2)

    def test_content_details_uses_lifetime_total_for_show(self):
        self._add_server('Server A', 0)
        self._add_lifetime_count(
            media_type='show',
            title_normalized='family guy',
            total_plays=30,
        )

        clicked = self._add_history(
            row_id=50,
            server_name='Server A',
            server_order=0,
            media_type='episode',
            title='Pilot',
            full_title='Family Guy - Pilot',
            grandparent_title='Family Guy',
            rating_key=5001,
            grandparent_rating_key=9001,
            user='alice',
            started=1700800000,
            date_played=date(2024, 5, 1),
        )

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 30)
        self.assertEqual(details['unique_users'], 1)

    def test_media_content_details_works_without_local_history(self):
        self._add_server('Server A', 0)
        media = self._add_cached_media(
            media_type='movie',
            title='Fresh Movie',
            year=2024,
            play_count=9,
        )

        client = MagicMock()
        client.get_history_paginated.return_value = {
            'response': {
                'result': 'success',
                'data': {
                    'recordsFiltered': 1,
                    'data': [
                        {
                            'media_type': 'movie',
                            'title': 'Fresh Movie',
                            'year': 2024,
                            'rating_key': 555,
                        }
                    ],
                },
            }
        }
        client.get_metadata.return_value = {
            'response': {
                'result': 'success',
                'data': {
                    'summary': 'A brand new movie.',
                    'year': 2024,
                },
            }
        }
        with patch('flask_app.services.content_service.TautulliClient', return_value=client):
            details = ContentService().get_content_details_for_media(media.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['content_kind'], 'movie')
        self.assertEqual(details['total_plays'], 9)
        self.assertEqual(details['unique_users'], 0)
        self.assertEqual(details['watch_history'], [])
        self.assertIsNone(details['source_record_id'])

    def test_get_metadata_for_media_falls_back_to_cached_ratings(self):
        media = self._add_cached_media(
            media_type='movie',
            title='Rated Movie',
            year=2023,
            play_count=0,
            added_at=1704110400,
            rating='7.8',
            rating_image='imdb://image.rating',
            audience_rating='91',
            audience_rating_image='rottentomatoes://image.rating.upright',
        )

        metadata = ContentService()._get_metadata_for_media(
            media=media,
            source_record=None,
            is_movie=True,
            content_title='Rated Movie',
        )

        self.assertEqual(metadata['critic_rating'], '7.8')
        self.assertEqual(metadata['critic_rating_display'], '7.8')
        self.assertEqual(metadata['audience_rating'], '91')
        self.assertEqual(metadata['audience_rating_display'], '91%')
        self.assertEqual(metadata['date_added'], '2024-01-01')

    def test_format_rating_display_handles_rotten_tomatoes_fraction(self):
        formatted = ContentService._format_rating_display(
            0.93,
            'rottentomatoes://image.rating.upright',
        )
        self.assertEqual(formatted, '93%')


if __name__ == '__main__':
    unittest.main()
