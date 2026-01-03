"""
Tautulli API client for fetching server statistics.
"""

import requests
from typing import Any, Optional
from datetime import datetime, timedelta

from multiplex_stats.models import ServerConfig


class TautulliClient:
    """Client for interacting with Tautulli API."""

    def __init__(self, server_config: ServerConfig):
        """
        Initialize Tautulli client.

        Args:
            server_config: Server configuration containing API credentials
        """
        self.config = server_config
        self.base_url = server_config.base_url
        self.api_key = server_config.api_key

    def _make_request(self, command: str, **params: Any) -> dict[str, Any]:
        """
        Make a request to the Tautulli API.

        Args:
            command: API command to execute
            **params: Additional parameters for the API call

        Returns:
            JSON response from the API

        Raises:
            requests.RequestException: If the request fails
        """
        url = f"{self.base_url}?apikey={self.api_key}&cmd={command}"

        for key, value in params.items():
            url += f"&{key}={value}"

        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_plays_by_date(self, time_range: int = 60) -> dict[str, Any]:
        """
        Get play counts by date.

        Args:
            time_range: Number of days to fetch (default: 60)

        Returns:
            API response containing play counts by date
        """
        return self._make_request('get_plays_by_date', time_range=time_range)

    def get_plays_per_month(self, time_range: int = 60) -> dict[str, Any]:
        """
        Get play counts per month.

        Args:
            time_range: Number of months to fetch (default: 60)

        Returns:
            API response containing play counts per month
        """
        return self._make_request('get_plays_per_month', time_range=time_range)

    def get_history(
        self,
        days: int = 60,
        length: int = 4000,
        after: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Get play history.

        Args:
            days: Number of days back to fetch
            length: Maximum number of records to return
            after: Optional date string in YYYY-MM-DD format

        Returns:
            API response containing play history
        """
        if after is None:
            after_date = datetime.now() - timedelta(days=days)
            after = after_date.strftime("%Y-%m-%d")

        return self._make_request('get_history', after=after, length=length)

    def get_library_user_stats(self, section_id: int) -> dict[str, Any]:
        """
        Get library user statistics.

        Args:
            section_id: Library section ID (1=TV, 2=Movies typically)

        Returns:
            API response containing user statistics for the library
        """
        return self._make_request('get_library_user_stats', section_id=section_id)

    def get_users(self) -> dict[str, Any]:
        """
        Get list of users.

        Returns:
            API response containing user information
        """
        return self._make_request('get_users')

    def get_activity(self) -> dict[str, Any]:
        """
        Get current activity on the server.

        Returns:
            API response containing current streaming activity
        """
        return self._make_request('get_activity')
