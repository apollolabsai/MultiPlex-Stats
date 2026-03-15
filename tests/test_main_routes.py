import unittest
import os
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from flask_app.models import ViewingHistory, db
from flask_app.routes.logs import logs_bp
from flask_app.routes.main import main_bp
from flask_app.routes.settings import settings_bp


class MainRoutesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        template_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'flask_app', 'templates'))
        cls.app = Flask(__name__, template_folder=template_root)
        cls.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        cls.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        cls.app.config['SECRET_KEY'] = 'test-secret'
        db.init_app(cls.app)

        @cls.app.template_filter('timestamp_to_date')
        def _timestamp_to_date(value):
            return str(value)

        @cls.app.template_filter('timestamp_to_age_label')
        def _timestamp_to_age_label(value):
            return str(value)

        cls.app.register_blueprint(main_bp)
        cls.app.register_blueprint(settings_bp, url_prefix='/settings')
        cls.app.register_blueprint(logs_bp, url_prefix='/logs')
        with cls.app.app_context():
            db.create_all()
        cls.client = cls.app.test_client()

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

    @patch('flask_app.routes.main.AnalyticsService.get_current_activity')
    def test_api_current_activity_data_returns_html_and_streams(self, mock_get_current_activity):
        mock_get_current_activity.return_value = [{
            'server': 'Apollo',
            'server_order': 'server-a',
            'user': 'Alice',
            'title': 'Heat',
            'subtitle': '(1995)',
            'media_type': 'movie',
            'state': 'playing',
            'progress_percent': 42,
            'platform': 'Web',
            'product': 'Chrome',
            'quality': 'Direct Play - 1080p',
            'bandwidth_mbps': 8.0,
            'ip_address': '8.8.8.8',
            'location': 'Los Angeles, California',
            'geo_city': 'Los Angeles',
            'geo_region': 'California',
            'geo_country': 'United States',
            'geo_lat': 34.0522,
            'geo_lon': -118.2437,
            'is_mappable': True,
            'poster_url': '',
            'media_id': None,
        }]

        response = self.client.get('/api/current-activity-data')
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIn('Alice', payload['html'])
        self.assertIn('8.8.8.8', payload['html'])
        self.assertEqual(len(payload['streams']), 1)
        self.assertEqual(payload['streams'][0]['geo_lat'], 34.0522)

    @patch('flask_app.services.geolocation_service.GeolocationService.lookup_ip')
    def test_api_ip_lookup_preserves_existing_history_modal_fields(self, mock_lookup_ip):
        db.session.add(ViewingHistory(
            row_id=1,
            server_name='Apollo',
            ip_address='8.8.8.8',
            location='wan',
        ))
        db.session.commit()

        mock_lookup_ip.return_value = {
            'city': 'Los Angeles',
            'region': 'California',
            'country': 'United States',
            'isp': 'Example ISP',
            'latitude': 34.0522,
            'longitude': -118.2437,
        }

        response = self.client.get('/api/ip-lookup?ip=8.8.8.8')
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload['ip'], '8.8.8.8')
        self.assertEqual(payload['location'], 'wan')
        self.assertEqual(payload['city'], 'Los Angeles')
        self.assertEqual(payload['region'], 'California')
        self.assertEqual(payload['country'], 'United States')
        self.assertEqual(payload['isp'], 'Example ISP')
        self.assertEqual(payload['latitude'], 34.0522)
        self.assertEqual(payload['longitude'], -118.2437)

    @patch('flask_app.routes.main.ConfigService.has_valid_config')
    @patch('flask_app.routes.main.MediaService.start_media_load')
    def test_api_media_start_load_passes_requested_run_mode(self, mock_start_media_load, mock_has_valid_config):
        mock_has_valid_config.return_value = True
        mock_start_media_load.return_value = True

        response = self.client.post('/api/media/start-load?mode=full_pipeline')
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload['run_mode'], 'full_pipeline')
        mock_start_media_load.assert_called_once_with(run_mode='full_pipeline')

    @patch('flask_app.routes.main.ConfigService.has_valid_config')
    def test_api_media_start_load_rejects_invalid_run_mode(self, mock_has_valid_config):
        mock_has_valid_config.return_value = True

        response = self.client.post('/api/media/start-load?mode=unexpected')
        payload = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload['error'], 'Invalid media sync mode.')

    @patch('flask_app.routes.main.ConfigService.get_analytics_settings')
    @patch('flask_app.routes.main.AnalyticsService.get_monthly_chart_json')
    @patch('flask_app.routes.main.AnalyticsService.get_user_detail')
    @patch('flask_app.routes.main.ConfigService.has_valid_config')
    def test_user_detail_page_renders(
        self,
        mock_has_valid_config,
        mock_get_user_detail,
        mock_get_monthly_chart_json,
        mock_get_analytics_settings,
    ):
        mock_has_valid_config.return_value = True
        mock_get_analytics_settings.return_value = SimpleNamespace(monthly_trend_months=60)
        mock_get_user_detail.return_value = {
            'display_name': 'PDTI New',
            'friendly_name': 'PDTI New',
            'username': 'pdti7',
            'user_id': 42,
            'email': 'pdti7@example.com',
            'user_thumb': '',
            'total_plays': 12,
            'first_play': 100,
            'last_play': 200,
            'unique_devices': 2,
            'unique_ips': 2,
            'device_chart': {
                'categories': ['Living Room AppleTV'],
                'series': [{'name': 'Apollo', 'data': [12], 'color': '#E6B413'}],
                'title': 'Number of Plays by Device',
            },
            'ip_addresses': [],
        }
        mock_get_monthly_chart_json.return_value = {
            'chart_data': {
                'categories': ['2026-01'],
                'series': [{'name': 'Apollo Movies', 'data': [1], 'color': '#E6B413'}],
                'title': 'Monthly Play Counts by Server and Media Type',
            },
            'monthly_trend_months': 60,
            'user_id': 42,
        }

        response = self.client.get('/users/pdti7?user_id=42')

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'PDTI New', response.data)
        self.assertIn(b'Plays by Device', response.data)
