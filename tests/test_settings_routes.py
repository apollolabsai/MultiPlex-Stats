import os
import unittest

from flask import Flask

from flask_app.models import AnalyticsSettings, db
from flask_app.routes.settings import settings_bp
from flask_app.services.config_service import ConfigService


class SettingsRoutesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        template_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'flask_app', 'templates'))
        cls.app = Flask(__name__, template_folder=template_root)
        cls.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
        cls.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        cls.app.config['SECRET_KEY'] = 'test-secret'
        cls.app.register_blueprint(settings_bp, url_prefix='/settings')
        db.init_app(cls.app)
        with cls.app.app_context():
            db.create_all()
            if AnalyticsSettings.query.first() is None:
                db.session.add(AnalyticsSettings())
                db.session.commit()
        cls.client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        with cls.app.app_context():
            db.drop_all()

    def setUp(self):
        self.ctx = self.app.app_context()
        self.ctx.push()
        settings = AnalyticsSettings.query.first()
        settings.stadia_maps_api_key = None
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()

    def test_update_map_settings_saves_key(self):
        response = self.client.post('/settings/map', data={
            'stadia_maps_api_key': 'abc123'
        }, follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        settings = AnalyticsSettings.query.first()
        self.assertEqual(settings.stadia_maps_api_key, 'abc123')

    def test_update_map_settings_allows_clearing_key(self):
        settings = AnalyticsSettings.query.first()
        settings.stadia_maps_api_key = 'abc123'
        db.session.commit()

        response = self.client.post('/settings/map', data={
            'stadia_maps_api_key': ''
        }, follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        settings = AnalyticsSettings.query.first()
        self.assertIsNone(settings.stadia_maps_api_key)

    def test_db_stored_key_takes_precedence_over_env_default(self):
        settings = AnalyticsSettings.query.first()
        settings.stadia_maps_api_key = 'stored-key'
        db.session.commit()

        effective = ConfigService.get_effective_stadia_maps_api_key('env-key')

        self.assertEqual(effective, 'stored-key')

    def test_env_default_used_when_no_db_key(self):
        effective = ConfigService.get_effective_stadia_maps_api_key('env-key')
        self.assertEqual(effective, 'env-key')
