import os
import unittest

from multiplex_stats.config_loader import ConfigLoader


class ConfigLoaderEnvTests(unittest.TestCase):
    def setUp(self):
        self.env_keys = [
            'TAUTULLI_SERVER_A_NAME',
            'TAUTULLI_SERVER_A_IP',
            'TAUTULLI_SERVER_A_KEY',
            'TAUTULLI_SERVER_A_SSL',
            'TAUTULLI_SERVER_A_VERIFY_SSL',
            'TAUTULLI_SERVER_B_NAME',
            'TAUTULLI_SERVER_B_IP',
            'TAUTULLI_SERVER_B_KEY',
            'TAUTULLI_SERVER_B_SSL',
            'TAUTULLI_SERVER_B_VERIFY_SSL',
        ]
        self.original_env = {k: os.environ.get(k) for k in self.env_keys}

    def tearDown(self):
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _set_minimum_server_a(self):
        os.environ['TAUTULLI_SERVER_A_NAME'] = 'ServerA'
        os.environ['TAUTULLI_SERVER_A_IP'] = '192.168.1.10:8181'
        os.environ['TAUTULLI_SERVER_A_KEY'] = 'abc123'

    def test_env_ssl_flags_are_parsed_for_both_servers(self):
        self._set_minimum_server_a()
        os.environ['TAUTULLI_SERVER_A_SSL'] = 'TRUE'
        os.environ['TAUTULLI_SERVER_A_VERIFY_SSL'] = '0'

        os.environ['TAUTULLI_SERVER_B_NAME'] = 'ServerB'
        os.environ['TAUTULLI_SERVER_B_IP'] = '192.168.1.11:8181'
        os.environ['TAUTULLI_SERVER_B_KEY'] = 'def456'
        os.environ['TAUTULLI_SERVER_B_SSL'] = '1'
        os.environ['TAUTULLI_SERVER_B_VERIFY_SSL'] = 'yes'

        loader = ConfigLoader(config_file='config-does-not-exist.ini')
        server_a, server_b = loader.get_server_configs()

        self.assertTrue(server_a.use_ssl)
        self.assertFalse(server_a.verify_ssl)
        self.assertTrue(server_b.use_ssl)
        self.assertTrue(server_b.verify_ssl)

    def test_env_ssl_flags_default_false_when_unset(self):
        self._set_minimum_server_a()
        os.environ.pop('TAUTULLI_SERVER_A_SSL', None)
        os.environ.pop('TAUTULLI_SERVER_A_VERIFY_SSL', None)

        loader = ConfigLoader(config_file='config-does-not-exist.ini')
        server_a, server_b = loader.get_server_configs()

        self.assertFalse(server_a.use_ssl)
        self.assertFalse(server_a.verify_ssl)
        self.assertIsNone(server_b)


if __name__ == '__main__':
    unittest.main()
