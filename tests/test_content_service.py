import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from flask import Flask

from flask_app.models import CachedMedia, db, ServerConfig, ViewingHistory
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

    def test_content_details_uses_endpoint_totals_across_servers(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)

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

        client_by_server: dict[str, MagicMock] = {}

        def build_client(config):
            client = MagicMock()
            client_by_server[config.name] = client
            if config.name == 'Server A':
                client.get_item_watch_time_stats.return_value = {
                    'response': {'data': [{'query_days': 0, 'total_plays': 10}]}
                }
                client.get_item_user_stats.return_value = {
                    'response': {'data': [
                        {'friendly_name': 'Alice', 'total_plays': 6},
                        {'friendly_name': 'Bob', 'total_plays': 4},
                    ]}
                }
            else:
                client.get_item_watch_time_stats.return_value = {
                    'response': {'data': [{'query_days': 0, 'total_plays': 7}]}
                }
                client.get_item_user_stats.return_value = {
                    'response': {'data': [
                        {'friendly_name': 'Bob', 'total_plays': 3},
                        {'friendly_name': 'Charlie', 'total_plays': 4},
                    ]}
                }
            return client

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', side_effect=build_client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 17)
        self.assertEqual(details['unique_users'], 3)
        client_by_server['Server A'].get_item_user_stats.assert_called_with(111)
        client_by_server['Server B'].get_item_user_stats.assert_called_with(222)

    def test_content_details_uses_show_parent_rating_key_for_stats(self):
        self._add_server('Server A', 0)

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

        client = MagicMock()
        client.get_item_watch_time_stats.return_value = {
            'response': {'data': [{'query_days': 0, 'total_plays': 25}]}
        }
        client.get_item_user_stats.return_value = {
            'response': {'data': [
                {'friendly_name': 'alice', 'total_plays': 12},
                {'friendly_name': 'bob', 'total_plays': 13},
            ]}
        }

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', return_value=client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 25)
        self.assertEqual(details['unique_users'], 2)
        client.get_item_user_stats.assert_called_with(9001)

    def test_tv_user_chart_uses_endpoint_user_counts(self):
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

        client = MagicMock()
        client.get_item_watch_time_stats.return_value = {
            'response': {'result': 'success', 'data': [{'query_days': 0, 'total_plays': 12}]}
        }
        client.get_item_user_stats.return_value = {
            'response': {'result': 'success', 'data': [
                {'friendly_name': 'alice', 'total_plays': 5},
                {'friendly_name': 'bob', 'total_plays': 7},
            ]}
        }

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', return_value=client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['unique_users'], 2)
        self.assertEqual(details['plays_by_user_chart']['categories'], ['bob', 'alice'])
        self.assertEqual([point['y'] for point in details['plays_by_user_chart']['data']], [7, 5])

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

        failing_client = MagicMock()
        failing_client.get_item_watch_time_stats.side_effect = RuntimeError('tautulli unavailable')

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', return_value=failing_client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 2)
        self.assertEqual(details['unique_users'], 2)

    def test_content_details_uses_endpoint_totals_when_one_server_fails(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)

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

        def build_client(config):
            client = MagicMock()
            if config.name == 'Server A':
                client.get_item_watch_time_stats.return_value = {
                    'response': {'data': [{'query_days': 0, 'total_plays': 20}]}
                }
                client.get_item_user_stats.return_value = {
                    'response': {'data': [{'friendly_name': 'Alice', 'total_plays': 20}]}
                }
            else:
                client.get_item_watch_time_stats.side_effect = RuntimeError('server unavailable')
                client.get_item_user_stats.side_effect = RuntimeError('server unavailable')
            return client

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', side_effect=build_client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 20)
        self.assertEqual(details['unique_users'], 1)

    def test_content_details_total_plays_can_come_from_user_stats(self):
        self._add_server('Server A', 0)
        self._add_server('Server B', 1)

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

        def build_client(config):
            client = MagicMock()
            client.get_item_watch_time_stats.side_effect = RuntimeError('watch stats unavailable')
            if config.name == 'Server A':
                client.get_item_user_stats.return_value = {
                    'response': {'result': 'success', 'data': [
                        {'friendly_name': 'Neo', 'total_plays': '5'},
                        {'friendly_name': 'Morpheus', 'total_plays': '2'},
                    ]}
                }
            else:
                client.get_item_user_stats.return_value = {
                    'response': {'result': 'success', 'data': [
                        {'friendly_name': 'Trinity', 'total_plays': '4'},
                    ]}
                }
            return client

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', side_effect=build_client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 11)
        self.assertEqual(details['unique_users'], 3)

    def test_content_details_aggregates_multiple_show_keys_per_server(self):
        self._add_server('Server A', 0)

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

        client = MagicMock()

        def history_page(**kwargs):
            start = kwargs.get('start', 0)
            if start == 0:
                return {
                    'response': {
                        'result': 'success',
                        'data': {
                            'recordsFiltered': 2,
                            'data': [
                                {
                                    'media_type': 'episode',
                                    'grandparent_title': 'Family Guy',
                                    'grandparent_rating_key': 9001,
                                },
                                {
                                    'media_type': 'episode',
                                    'grandparent_title': 'Family Guy',
                                    'grandparent_rating_key': 9002,
                                },
                            ],
                        },
                    }
                }
            return {
                'response': {
                    'result': 'success',
                    'data': {
                        'recordsFiltered': 2,
                        'data': [],
                    },
                }
            }

        def watch_stats(rating_key, **kwargs):
            if rating_key == 9001:
                total = 10
            elif rating_key == 9002:
                total = 20
            else:
                total = 0
            return {'response': {'result': 'success', 'data': [{'query_days': 0, 'total_plays': total}]}}

        def user_stats(rating_key, **kwargs):
            if rating_key == 9001:
                rows = [
                    {'friendly_name': 'alice', 'total_plays': 6},
                    {'friendly_name': 'bob', 'total_plays': 4},
                ]
            elif rating_key == 9002:
                rows = [
                    {'friendly_name': 'alice', 'total_plays': 7},
                    {'friendly_name': 'charlie', 'total_plays': 13},
                ]
            else:
                rows = []
            return {'response': {'result': 'success', 'data': rows}}

        client.get_history_paginated.side_effect = history_page
        client.get_item_watch_time_stats.side_effect = watch_stats
        client.get_item_user_stats.side_effect = user_stats

        with patch.object(ContentService, '_get_metadata_for_record', return_value={'summary': 'ok'}):
            with patch('flask_app.services.content_service.TautulliClient', return_value=client):
                details = ContentService().get_content_details(clicked.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['total_plays'], 30)
        self.assertEqual(details['unique_users'], 3)
        watched_keys = {call.args[0] for call in client.get_item_watch_time_stats.call_args_list}
        self.assertEqual(watched_keys, {9001, 9002})

    def test_media_content_details_works_without_local_history(self):
        self._add_server('Server A', 0)
        media = self._add_cached_media(
            media_type='movie',
            title='Fresh Movie',
            year=2024,
            play_count=0,
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
        client.get_item_watch_time_stats.return_value = {
            'response': {'result': 'success', 'data': [{'query_days': 0, 'total_plays': 9}]}
        }
        client.get_item_user_stats.return_value = {
            'response': {'result': 'success', 'data': [
                {'friendly_name': 'alice', 'total_plays': 4},
                {'friendly_name': 'bob', 'total_plays': 5},
            ]}
        }

        with patch('flask_app.services.content_service.TautulliClient', return_value=client):
            details = ContentService().get_content_details_for_media(media.id)

        self.assertIsNotNone(details)
        self.assertEqual(details['content_kind'], 'movie')
        self.assertEqual(details['total_plays'], 9)
        self.assertEqual(details['unique_users'], 2)
        self.assertEqual(details['watch_history'], [])
        self.assertIsNone(details['source_record_id'])


if __name__ == '__main__':
    unittest.main()
