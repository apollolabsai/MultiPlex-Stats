import unittest
from unittest.mock import Mock, patch

from flask import Flask

from flask_app.models import IPGeolocation, db
from flask_app.services.geolocation_service import GeolocationService


class GeolocationServiceTests(unittest.TestCase):
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
        IPGeolocation.query.delete()
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()

    @patch('flask_app.services.geolocation_service.requests.get')
    def test_lookup_ip_caches_coordinates(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'status': 'success',
            'city': 'Paris',
            'regionName': 'Ile-de-France',
            'country': 'France',
            'isp': 'Example ISP',
            'lat': 48.8566,
            'lon': 2.3522,
        }
        mock_get.return_value = response

        service = GeolocationService()
        result = service.lookup_ip('8.8.8.8')
        cached = service.lookup_ip('8.8.8.8')

        self.assertEqual(result['city'], 'Paris')
        self.assertEqual(result['latitude'], 48.8566)
        self.assertEqual(result['longitude'], 2.3522)
        self.assertEqual(cached['latitude'], 48.8566)
        self.assertEqual(mock_get.call_count, 1)

        record = IPGeolocation.query.filter_by(ip_address='8.8.8.8').first()
        self.assertIsNotNone(record)
        self.assertEqual(record.latitude, 48.8566)
        self.assertEqual(record.longitude, 2.3522)

    def test_lookup_ip_returns_local_network_without_coordinates(self):
        result = GeolocationService().lookup_ip('192.168.1.50')

        self.assertEqual(result['city'], 'Local')
        self.assertEqual(result['isp'], 'Local Network')
        self.assertIsNone(result['latitude'])
        self.assertIsNone(result['longitude'])

    @patch('flask_app.services.geolocation_service.requests.get')
    def test_lookup_ip_refreshes_cached_rows_missing_coordinates(self, mock_get):
        stale_record = IPGeolocation(
            ip_address='8.8.4.4',
            city='Van Nuys',
            region='California',
            country='United States',
            isp='Charter Communications',
            latitude=None,
            longitude=None,
        )
        db.session.add(stale_record)
        db.session.commit()

        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'status': 'success',
            'city': 'Van Nuys',
            'regionName': 'California',
            'country': 'United States',
            'isp': 'Charter Communications',
            'lat': 34.1867,
            'lon': -118.448,
        }
        mock_get.return_value = response

        result = GeolocationService().lookup_ip('8.8.4.4')

        self.assertEqual(result['city'], 'Van Nuys')
        self.assertEqual(result['latitude'], 34.1867)
        self.assertEqual(result['longitude'], -118.448)
        self.assertEqual(mock_get.call_count, 1)

        record = IPGeolocation.query.filter_by(ip_address='8.8.4.4').first()
        self.assertEqual(record.latitude, 34.1867)
        self.assertEqual(record.longitude, -118.448)
