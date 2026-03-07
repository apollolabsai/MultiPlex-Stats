import unittest
import os
from unittest.mock import patch

from flask import Flask

from flask_app.models import ViewingHistory, db
from flask_app.routes.main import main_bp


class MainRoutesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        template_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'flask_app', 'templates'))
        cls.app = Flask(__name__, template_folder=template_root)
        cls.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        cls.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        cls.app.config['SECRET_KEY'] = 'test-secret'
        db.init_app(cls.app)
        cls.app.register_blueprint(main_bp)
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
