"""
IP geolocation lookup service with database caching.
Uses ip-api.com (45 requests/minute free tier, no API key required).
"""
import requests
from flask_app.models import db, IPGeolocation


class GeolocationService:
    """Service to lookup and cache IP geolocations."""

    def __init__(self):
        # ip-api.com - free tier: 45 requests/minute, HTTP only
        self.api_url = "http://ip-api.com/json/{ip}?fields=status,message,country,regionName,city,isp"

    def lookup_ip(self, ip_address):
        """
        Lookup geolocation for an IP address.
        Returns cached result if available, otherwise performs API lookup.

        Args:
            ip_address: IP address to lookup

        Returns:
            dict with keys: city, region, country, isp (all may be None)
        """
        normalized_ip = self._normalize_ip(ip_address)
        if not normalized_ip:
            return {'city': None, 'region': None, 'country': None, 'isp': None}

        # Check if it's a private IP (don't lookup)
        if self._is_private_ip(normalized_ip):
            return {'city': 'Local', 'region': None, 'country': None, 'isp': 'Local Network'}

        # Check cache first
        cached = IPGeolocation.query.filter_by(ip_address=normalized_ip).first()
        if cached:
            return {
                'city': cached.city,
                'region': cached.region,
                'country': cached.country,
                'isp': cached.isp
            }

        # Perform API lookup
        try:
            response = requests.get(
                self.api_url.format(ip=normalized_ip),
                timeout=5
            )

            if response.status_code == 200:
                data = response.json()

                # Check for API error response
                if data.get('status') == 'fail':
                    return {'city': None, 'region': None, 'country': None, 'isp': None}

                # Extract location data (ip-api.com field names)
                city = data.get('city')
                region = data.get('regionName')
                country = data.get('country')
                isp = data.get('isp')

                # Cache the result
                geo_record = IPGeolocation(
                    ip_address=normalized_ip,
                    city=city,
                    region=region,
                    country=country,
                    isp=isp
                )
                db.session.add(geo_record)
                db.session.commit()

                return {'city': city, 'region': region, 'country': country, 'isp': isp}
            else:
                # API error - cache as unknown to prevent repeated failed requests
                geo_record = IPGeolocation(
                    ip_address=normalized_ip,
                    city=None,
                    region=None,
                    country=None,
                    isp=None
                )
                db.session.add(geo_record)
                db.session.commit()
                return {'city': None, 'region': None, 'country': None, 'isp': None}

        except Exception as e:
            print(f"Error looking up IP {normalized_ip}: {e}")
            # Don't cache failures - allow retry later
            return {'city': None, 'region': None, 'country': None, 'isp': None}

    def _normalize_ip(self, ip_address):
        """Normalize IP string for caching and lookup."""
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

        # Common private IP ranges
        private_patterns = [
            '127.',
            '10.',
            '192.168.',
            '172.16.', '172.17.', '172.18.', '172.19.',
            '172.20.', '172.21.', '172.22.', '172.23.',
            '172.24.', '172.25.', '172.26.', '172.27.',
            '172.28.', '172.29.', '172.30.', '172.31.',
            'localhost',
            '::1',  # IPv6 localhost
            'fe80::',  # IPv6 link-local
        ]

        for pattern in private_patterns:
            if ip_address.startswith(pattern):
                return True

        return False
