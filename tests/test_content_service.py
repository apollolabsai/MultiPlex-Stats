import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from flask import Flask

from flask_app.models import db, ServerConfig, ViewingHistory
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
        client_by_server['Server A'].get_item_watch_time_stats.assert_called_with(
            111,
            query_days=0,
        )
        client_by_server['Server B'].get_item_watch_time_stats.assert_called_with(
            222,
            query_days=0,
        )

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
        client.get_item_watch_time_stats.assert_called_with(
            9001,
            query_days=0,
        )

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


if __name__ == '__main__':
    unittest.main()
