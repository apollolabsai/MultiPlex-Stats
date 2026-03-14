"""
IP geolocation lookup service with database caching.
Uses ip-api.com (45 requests/minute free tier, no API key required).
"""
import ipaddress
import logging
from datetime import UTC, datetime

from flask_app.utils.http import logged_session

logger = logging.getLogger('multiplex.geolocation')
from flask_app.models import db, IPGeolocation


class GeolocationService:
    """Service to lookup and cache IP geolocations."""

    def __init__(self):
        # ip-api.com - free tier: 45 requests/minute, HTTP only
        self.api_url = "http://ip-api.com/json/{ip}?fields=status,message,country,regionName,city,isp,lat,lon"

    def lookup_ip(self, ip_address):
        """
        Lookup geolocation for an IP address.
        Returns cached result if available, otherwise performs API lookup.

        Args:
            ip_address: IP address to lookup

        Returns:
            dict with keys: city, region, country, isp, latitude, longitude (all may be None)
        """
        normalized_ip = self._normalize_ip(ip_address)
        if not normalized_ip:
            return self._empty_result()

        # Check if it's a private IP (don't lookup)
        if self._is_private_ip(normalized_ip):
            return {
                'city': 'Local',
                'region': None,
                'country': None,
                'isp': 'Local Network',
                'latitude': None,
                'longitude': None,
            }

        # Check cache first
        cached = IPGeolocation.query.filter_by(ip_address=normalized_ip).first()
        if cached:
            cached_result = self._record_to_result(cached)

            # Legacy cache rows may have location text but no coordinates.
            if self._should_refresh_cached_coordinates(cached):
                refreshed_result = self._lookup_remote(normalized_ip, cached)
                if refreshed_result is not None:
                    return refreshed_result

            return cached_result

        # Perform API lookup
        fresh_result = self._lookup_remote(normalized_ip)
        if fresh_result is not None:
            return fresh_result

        return self._empty_result()

    def _lookup_remote(self, normalized_ip, cached_record=None):
        """Query the remote geolocation API and upsert the cache record."""
        logger.info('Geolocation Looking up IP %s', normalized_ip)
        try:
            response = logged_session.get(
                self.api_url.format(ip=normalized_ip),
                timeout=5,
            )

            if response.status_code == 200:
                data = response.json()

                # Check for API error response
                if data.get('status') == 'fail':
                    logger.warning('Geolocation Lookup failed for IP %s: %s', normalized_ip, data.get('message', 'unknown error'))
                    return None if cached_record else self._empty_result()

                # Extract location data (ip-api.com field names)
                city = data.get('city')
                region = data.get('regionName')
                country = data.get('country')
                isp = data.get('isp')
                latitude = data.get('lat')
                longitude = data.get('lon')

                # Cache the result
                geo_record = cached_record or IPGeolocation(ip_address=normalized_ip)
                geo_record.city = city
                geo_record.region = region
                geo_record.country = country
                geo_record.isp = isp
                geo_record.latitude = latitude
                geo_record.longitude = longitude
                geo_record.lookup_date = datetime.now(UTC)
                db.session.add(geo_record)
                db.session.commit()

                location_label = ', '.join(filter(None, [city, region, country])) or 'Unknown'
                logger.info('Geolocation %s -> %s (%s)', normalized_ip, location_label, isp or 'unknown ISP')

                return {
                    'city': city,
                    'region': region,
                    'country': country,
                    'isp': isp,
                    'latitude': latitude,
                    'longitude': longitude,
                }
            else:
                if cached_record:
                    return None

                # API error - cache as unknown to prevent repeated failed requests
                geo_record = IPGeolocation(
                    ip_address=normalized_ip,
                    city=None,
                    region=None,
                    country=None,
                    isp=None,
                    latitude=None,
                    longitude=None,
                    lookup_date=datetime.now(UTC),
                )
                db.session.add(geo_record)
                db.session.commit()
                return self._empty_result()

        except Exception as e:
            logger.error("Error looking up IP %s: %s", normalized_ip, e)
            # Don't overwrite cached records on refresh failure.
            return None if cached_record else self._empty_result()

    @staticmethod
    def format_location_label(geo_data):
        """Format a compact location label for dashboard display."""
        if not geo_data:
            return 'Unknown'

        city = geo_data.get('city')
        region = geo_data.get('region')
        country = geo_data.get('country')
        isp = geo_data.get('isp')

        if city == 'Local' and isp == 'Local Network':
            return 'Local Network'
        if city and region:
            return f"{city}, {region}"
        if city:
            return city
        if region:
            return region
        if country:
            return country
        return 'Unknown'

    @staticmethod
    def _empty_result():
        return {
            'city': None,
            'region': None,
            'country': None,
            'isp': None,
            'latitude': None,
            'longitude': None,
        }

    @staticmethod
    def _record_to_result(record):
        """Convert a cached ORM record into the public response shape."""
        return {
            'city': record.city,
            'region': record.region,
            'country': record.country,
            'isp': record.isp,
            'latitude': record.latitude,
            'longitude': record.longitude,
        }

    @staticmethod
    def _should_refresh_cached_coordinates(record):
        """Refresh cached rows that lack coordinates.

        - Legacy rows with location text but no coords: refresh immediately.
        - All-None rows (failed lookups): retry after 24 hours so transient
          API outages don't permanently blackhole an IP address.
        """
        if record.latitude is not None and record.longitude is not None:
            return False

        if any((record.city, record.region, record.country, record.isp)):
            return True

        # All-None record — only retry if the cached failure is old enough.
        if record.lookup_date is None:
            return True
        # Normalize to naive UTC for comparison — SQLite may return either
        # naive or aware datetimes depending on how the record was stored.
        lookup_date = record.lookup_date
        if lookup_date.tzinfo is not None:
            lookup_date = lookup_date.replace(tzinfo=None)
        now = datetime.now(UTC).replace(tzinfo=None)
        return (now - lookup_date).total_seconds() > 86400

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
        """Check if IP is private/local using proper CIDR evaluation."""
        if not ip_address or ip_address == 'localhost':
            return True
        try:
            return ipaddress.ip_address(ip_address).is_private
        except ValueError:
            return False
