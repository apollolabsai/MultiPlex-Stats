import unittest
from unittest.mock import patch

from flask import Flask

from flask_app.models import ServerConfig, db
from flask_app.services.image_proxy_service import ImageProxyService


class ImageProxyServiceTests(unittest.TestCase):
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
        ServerConfig.query.delete()
        db.session.commit()
        db.session.add(ServerConfig(
            name='ApolloSS',
            ip_address='192.168.1.214:8181',
            api_key='test-key',
            is_active=True,
        ))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        self.ctx.pop()

    def test_build_fetch_candidates_prefers_canonical_rating_key_thumb(self):
        candidates = ImageProxyService._build_fetch_candidates(
            image_path='/library/metadata/3201/thumb/1700000000',
            rating_key=9201,
        )

        self.assertEqual(
            candidates,
            [
                ('/library/metadata/3201/thumb/1700000000', 9201),
                ('/library/metadata/9201/thumb', 9201),
                ('/library/metadata/3201/thumb/1700000000', None),
                ('/library/metadata/9201/thumb', None),
            ],
        )

    @patch('flask_app.services.image_proxy_service.TautulliClient')
    def test_fetch_image_retries_with_canonical_thumb_path(self, mock_client_cls):
        client = mock_client_cls.return_value

        def fake_proxy(**kwargs):
            if kwargs['img'] == '/library/metadata/3201/thumb/1700000000':
                raise RuntimeError('thumb mismatch')
            if kwargs['img'] == '/library/metadata/9201/thumb':
                return b'poster-bytes', 'image/png'
            raise AssertionError(f'Unexpected candidate: {kwargs}')

        client.pms_image_proxy.side_effect = fake_proxy

        image_bytes, content_type = ImageProxyService.fetch_image(
            server_name='ApolloSS',
            image_path='/library/metadata/3201/thumb/1700000000',
            width=220,
            height=330,
            fallback='poster',
            rating_key=9201,
        )

        self.assertEqual(image_bytes, b'poster-bytes')
        self.assertEqual(content_type, 'image/png')
        self.assertEqual(client.pms_image_proxy.call_count, 2)
        self.assertEqual(
            client.pms_image_proxy.call_args_list[1].kwargs,
            {
                'img': '/library/metadata/9201/thumb',
                'width': 220,
                'height': 330,
                'fallback': 'poster',
                'rating_key': 9201,
            },
        )


if __name__ == '__main__':
    unittest.main()
