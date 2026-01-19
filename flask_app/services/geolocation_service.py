"""
IP geolocation lookup service.
"""

import requests


class GeolocationService:
    """Service to lookup IP geolocation details."""

    def __init__(self):
        self.api_url = "https://ipapi.co/{ip}/json/"

    def lookup_ip(self, ip_address):
        """
        Lookup geolocation for an IP address.

        Args:
            ip_address: IP address to lookup

        Returns:
            dict with keys: city, region, country (all may be None)
        """
        normalized_ip = self._normalize_ip(ip_address)
        if not normalized_ip:
            return {'city': None, 'region': None, 'country': None}

        if self._is_private_ip(normalized_ip):
            return {'city': 'Local', 'region': None, 'country': None}

        try:
            response = requests.get(
                self.api_url.format(ip=normalized_ip),
                timeout=5
            )
            if response.status_code != 200:
                return {'city': None, 'region': None, 'country': None}

            data = response.json()
            return {
                'city': data.get('city'),
                'region': data.get('region'),
                'country': data.get('country_name')
            }
        except Exception:
            return {'city': None, 'region': None, 'country': None}

    def _normalize_ip(self, ip_address):
        """Normalize IP string for lookup."""
        if not ip_address:
            return None

        ip = str(ip_address).strip().lower()
        if ip in ('', 'unknown'):
            return None

        # Strip port for IPv4/host:port format.
        if ip.count(':') == 1:
            host, port = ip.rsplit(':', 1)
            if port.isdigit():
                ip = host

        return ip

    def _is_private_ip(self, ip_address):
        """Check if IP is private/local."""
        if not ip_address:
            return True

        private_patterns = [
            '127.',
            '10.',
            '192.168.',
            '172.16.', '172.17.', '172.18.', '172.19.',
            '172.20.', '172.21.', '172.22.', '172.23.',
            '172.24.', '172.25.', '172.26.', '172.27.',
            '172.28.', '172.29.', '172.30.', '172.31.',
            'localhost',
            '::1',
            'fe80::',
        ]

        return any(ip_address.startswith(pattern) for pattern in private_patterns)
