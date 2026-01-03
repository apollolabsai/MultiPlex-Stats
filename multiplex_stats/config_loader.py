"""
Configuration loader for MultiPlex Stats.

Supports loading configuration from:
1. config.ini file (recommended)
2. Environment variables (for automation/Docker)
"""

import configparser
import os
from pathlib import Path
from typing import Optional, Tuple
from dataclasses import dataclass

from multiplex_stats.models import ServerConfig


@dataclass
class AnalyticsSettings:
    """Settings for analytics processing."""

    # Time ranges for trend charts
    daily_trend_days: int = 60      # Number of days to show in daily trends chart
    monthly_trend_months: int = 60  # Number of months to show in monthly trends chart

    # History analysis
    history_days: int = 60

    # Top N items to show
    top_movies: int = 30
    top_tv_shows: int = 30
    top_users: int = 20


class ConfigLoader:
    """Load configuration from various sources."""

    def __init__(self, config_file: str = "config.ini"):
        """
        Initialize config loader.

        Args:
            config_file: Path to config file (default: config.ini)
        """
        self.config_file = config_file
        self.config = None

    def load_from_file(self) -> bool:
        """
        Load configuration from INI file.

        Returns:
            True if file was loaded successfully, False otherwise
        """
        if not os.path.exists(self.config_file):
            return False

        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)
        return True

    def get_server_configs(self) -> Tuple[Optional[ServerConfig], Optional[ServerConfig]]:
        """
        Get server configurations.

        Returns:
            Tuple of (ServerA config, ServerB config). ServerB may be None for single-server setups.

        Raises:
            ValueError: If configuration is missing or invalid
        """
        # Try config file first
        if self.config:
            try:
                server_a = ServerConfig(
                    name=self.config.get('ServerA', 'name'),
                    ip_address=self.config.get('ServerA', 'ip_address'),
                    api_key=self.config.get('ServerA', 'api_key')
                )

                # Validate Server A API key isn't a placeholder
                if 'YOUR_API_KEY' in server_a.api_key:
                    raise ValueError(
                        "Please update config.ini with your actual API key for ServerA!\n"
                        "Replace 'YOUR_API_KEY_HERE' with your Tautulli API key."
                    )

                # Try to load Server B, but it's optional
                server_b = None
                if self.config.has_section('ServerB'):
                    try:
                        server_b_name = self.config.get('ServerB', 'name', fallback='')
                        server_b_ip = self.config.get('ServerB', 'ip_address', fallback='')
                        server_b_key = self.config.get('ServerB', 'api_key', fallback='')

                        # Only create Server B config if all fields are provided and not placeholders
                        if server_b_name and server_b_ip and server_b_key and 'YOUR_API_KEY' not in server_b_key:
                            server_b = ServerConfig(
                                name=server_b_name,
                                ip_address=server_b_ip,
                                api_key=server_b_key
                            )
                    except (configparser.NoOptionError):
                        # Server B is incomplete, leave as None
                        pass

                return server_a, server_b

            except (configparser.NoSectionError, configparser.NoOptionError) as e:
                raise ValueError(f"Invalid config file: {e}")

        # Try environment variables
        env_a_name = os.getenv('TAUTULLI_SERVER_A_NAME')
        env_a_ip = os.getenv('TAUTULLI_SERVER_A_IP')
        env_a_key = os.getenv('TAUTULLI_SERVER_A_KEY')

        env_b_name = os.getenv('TAUTULLI_SERVER_B_NAME')
        env_b_ip = os.getenv('TAUTULLI_SERVER_B_IP')
        env_b_key = os.getenv('TAUTULLI_SERVER_B_KEY')

        if all([env_a_name, env_a_ip, env_a_key]):
            server_a = ServerConfig(name=env_a_name, ip_address=env_a_ip, api_key=env_a_key)

            # Server B is optional
            server_b = None
            if all([env_b_name, env_b_ip, env_b_key]):
                server_b = ServerConfig(name=env_b_name, ip_address=env_b_ip, api_key=env_b_key)

            return server_a, server_b

        raise ValueError(
            "No configuration found!\n\n"
            "Please create a config.ini file:\n"
            "  1. Copy config.ini.example to config.ini\n"
            "  2. Edit config.ini with your server information\n\n"
            "Or set environment variables:\n"
            "  TAUTULLI_SERVER_A_NAME, TAUTULLI_SERVER_A_IP, TAUTULLI_SERVER_A_KEY\n"
            "  (Optional) TAUTULLI_SERVER_B_NAME, TAUTULLI_SERVER_B_IP, TAUTULLI_SERVER_B_KEY"
        )

    def get_settings(self) -> AnalyticsSettings:
        """
        Get analytics settings.

        Returns:
            AnalyticsSettings with configured values
        """
        settings = AnalyticsSettings()

        # Try config file first
        if self.config and self.config.has_section('Settings'):
            settings.daily_trend_days = self.config.getint('Settings', 'daily_trend_days', fallback=60)
            settings.monthly_trend_months = self.config.getint('Settings', 'monthly_trend_months', fallback=60)
            settings.history_days = self.config.getint('Settings', 'history_days', fallback=60)
            settings.top_movies = self.config.getint('Settings', 'top_movies', fallback=30)
            settings.top_tv_shows = self.config.getint('Settings', 'top_tv_shows', fallback=30)
            settings.top_users = self.config.getint('Settings', 'top_users', fallback=20)
            return settings

        # Try environment variables
        settings.daily_trend_days = int(os.getenv('TAUTULLI_DAILY_TREND_DAYS', '60'))
        settings.monthly_trend_months = int(os.getenv('TAUTULLI_MONTHLY_TREND_MONTHS', '60'))
        settings.history_days = int(os.getenv('TAUTULLI_HISTORY_DAYS', '60'))
        settings.top_movies = int(os.getenv('TAUTULLI_TOP_MOVIES', '30'))
        settings.top_tv_shows = int(os.getenv('TAUTULLI_TOP_TV_SHOWS', '30'))
        settings.top_users = int(os.getenv('TAUTULLI_TOP_USERS', '20'))

        return settings


def load_config(config_file: str = "config.ini") -> Tuple[ServerConfig, ServerConfig, AnalyticsSettings]:
    """
    Convenience function to load all configuration.

    Args:
        config_file: Path to config file

    Returns:
        Tuple of (server_a, server_b, settings)

    Raises:
        ValueError: If configuration is missing or invalid
    """
    loader = ConfigLoader(config_file)
    loader.load_from_file()

    server_a, server_b = loader.get_server_configs()
    settings = loader.get_settings()

    return server_a, server_b, settings
