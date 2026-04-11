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


if __name__ == '__main__':
    unittest.main()
