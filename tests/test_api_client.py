import unittest
from unittest.mock import Mock, patch

from multiplex_stats.api_client import TautulliClient
from multiplex_stats.models import ServerConfig


class ApiClientTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.client = TautulliClient(
            ServerConfig(
                name='Apollo',
                ip_address='192.168.1.228:8181',
                api_key='abc123',
            )
        )

    @patch('multiplex_stats.api_client.requests.get')
    def test_get_history_uses_extended_read_timeout(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {'response': {'data': {}}}
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        self.client.get_history_paginated(start=0, length=1000)

        self.assertEqual(mock_get.call_args.kwargs['timeout'], (5, 90))

    @patch('multiplex_stats.api_client.requests.get')
    def test_library_media_info_uses_default_timeout(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {'response': {'data': {}}}
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        self.client.get_library_media_info(section_id=1, refresh=False)

        self.assertEqual(mock_get.call_args.kwargs['timeout'], (5, 30))

    @patch('multiplex_stats.api_client.requests.get')
    def test_pms_image_proxy_returns_content_and_content_type(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.content = b'image-bytes'
        response.headers = {'Content-Type': 'image/png'}
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        image_bytes, content_type = self.client.pms_image_proxy(
            img='/library/metadata/123/thumb/456',
            width=220,
            height=330,
            fallback='poster',
            rating_key=123,
        )

        self.assertEqual(image_bytes, b'image-bytes')
        self.assertEqual(content_type, 'image/png')
        self.assertEqual(mock_get.call_args.args[0], self.client.base_url)
        self.assertEqual(
            mock_get.call_args.kwargs['params'],
            {
                'apikey': 'abc123',
                'cmd': 'pms_image_proxy',
                'img': '/library/metadata/123/thumb/456',
                'width': 220,
                'height': 330,
                'fallback': 'poster',
                'rating_key': 123,
            },
        )
        self.assertEqual(mock_get.call_args.kwargs['timeout'], (5, 30))


if __name__ == '__main__':
    unittest.main()
