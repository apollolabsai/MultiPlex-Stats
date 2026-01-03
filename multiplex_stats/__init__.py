"""
Tautulli Analytics Package

A Python package for fetching and analyzing Plex server statistics via Tautulli API.
"""

from multiplex_stats.api_client import TautulliClient
from multiplex_stats.data_processing import (
    process_daily_data,
    process_monthly_data,
    process_history_data,
    process_library_stats,
)
from multiplex_stats.models import ServerConfig

__version__ = "0.1.0"
__all__ = [
    "TautulliClient",
    "ServerConfig",
    "process_daily_data",
    "process_monthly_data",
    "process_history_data",
    "process_library_stats",
]
