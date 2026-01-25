"""
Tautulli API client for fetching server statistics.
"""

import requests
import urllib3
from typing import Any, Optional
from datetime import datetime, timedelta

from multiplex_stats.models import ServerConfig

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        self.verify_ssl = server_config.verify_ssl

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

        response = requests.get(url, verify=self.verify_ssl)
        response.raise_for_status()
        return response.json()

    def get_plays_by_date(
        self,
        time_range: int = 60,
        user_id: Optional[int] = None
    ) -> dict[str, Any]:
        """
        Get play counts by date.

        Args:
            time_range: Number of days to fetch (default: 60)
            user_id: Optional user ID to filter results for a specific user

        Returns:
            API response containing play counts by date
        """
        params = {'time_range': time_range}
        if user_id is not None:
            params['user_id'] = user_id
        return self._make_request('get_plays_by_date', **params)

    def get_plays_per_month(
        self,
        time_range: int = 60,
        user_id: Optional[int] = None
    ) -> dict[str, Any]:
        """
        Get play counts per month.

        Args:
            time_range: Number of months to fetch (default: 60)
            user_id: Optional user ID to filter results for a specific user

        Returns:
            API response containing play counts per month
        """
        params = {'time_range': time_range}
        if user_id is not None:
            params['user_id'] = user_id
        return self._make_request('get_plays_per_month', **params)

    def get_history(
        self,
        days: int = 60,
        length: int = 25000,
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

    def get_history_paginated(
        self,
        start: int = 0,
        length: int = 1000,
        after: Optional[str] = None
    ) -> dict[str, Any]:
        """
        Get play history with pagination support.

        Args:
            start: Row offset to start from (for pagination)
            length: Number of records to return per page (max 1000 recommended)
            after: Optional date string in YYYY-MM-DD format to filter records after this date

        Returns:
            API response containing:
            - response.data: List of history records
            - response.recordsTotal: Total records available
            - response.recordsFiltered: Records matching filter
        """
        params = {'start': start, 'length': length}
        if after:
            params['after'] = after
        return self._make_request('get_history', **params)

    def get_activity(self) -> dict[str, Any]:
        """
        Get current streaming activity.

        Returns:
            API response containing current active streaming sessions
        """
        return self._make_request('get_activity')

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

    def get_concurrent_streams_by_stream_type(
        self,
        time_range: int = 60
    ) -> dict[str, Any]:
        """
        Get concurrent streams data by stream type.

        Args:
            time_range: Number of days to fetch (default: 60)

        Returns:
            API response containing concurrent streams data with categories
            (dates) and series (Direct Play, Direct Stream, Transcode, Max)
        """
        return self._make_request(
            'get_concurrent_streams_by_stream_type',
            time_range=time_range
        )

    def get_library_media_info(
        self,
        section_id: int,
        start: int = 0,
        length: int = 25000,
        order_column: str = 'added_at',
        order_dir: str = 'desc',
        refresh: bool = True
    ) -> dict[str, Any]:
        """
        Get media info for a library section.

        Args:
            section_id: Library section ID (1=Movies, 2=TV typically)
            start: Row offset to start from (for pagination)
            length: Number of records to return
            order_column: Column to order by
            order_dir: Order direction (asc/desc)
            refresh: Refresh cache to get accurate counts (default: True)

        Returns:
            API response containing media info for the library section
        """
        return self._make_request(
            'get_library_media_info',
            section_id=section_id,
            start=start,
            length=length,
            order_column=order_column,
            order_dir=order_dir,
            refresh='true' if refresh else 'false'
        )

    def get_libraries(self) -> dict[str, Any]:
        """
        Get list of all libraries.

        Returns:
            API response containing library information
        """
        return self._make_request('get_libraries')

    def export_metadata(
        self,
        section_id: int,
        file_format: str = 'json',
        metadata_level: int = 1,
        media_info_level: int = 1
    ) -> dict[str, Any]:
        """
        Start an async export of library metadata.

        Args:
            section_id: Library section ID
            file_format: Export format ('json', 'csv', 'xml', 'm3u')
            metadata_level: Detail level for metadata (0=none, 1=basic, 2=full)
            media_info_level: Detail level for media info (0=none, 1=basic, 2=full)

        Returns:
            API response containing export_id for tracking
        """
        return self._make_request(
            'export_metadata',
            section_id=section_id,
            file_format=file_format,
            metadata_level=metadata_level,
            media_info_level=media_info_level
        )

    def get_exports_table(self, section_id: int) -> dict[str, Any]:
        """
        Get status of all exports for a library section.

        Args:
            section_id: Library section ID

        Returns:
            API response containing list of exports with their status.
            Each export has: export_id, complete (0/1), file_format, etc.
        """
        return self._make_request('get_exports_table', section_id=section_id)

    def download_export(self, export_id: int) -> dict[str, Any]:
        """
        Download a completed export.

        Args:
            export_id: Export ID from export_metadata response

        Returns:
            JSON content of the export directly (for json format)
        """
        return self._make_request('download_export', export_id=export_id)
