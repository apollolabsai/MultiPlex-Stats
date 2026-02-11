"""
Configuration service for managing database-driven configuration.
"""
from typing import Tuple, Optional, List
from flask_app.models import db, ServerConfig, AnalyticsSettings


class ConfigService:
    """Service for managing configuration from database."""

    @staticmethod
    def get_server_configs() -> Tuple[Optional[object], Optional[object]]:
        """
        Get server configurations in multiplex_stats format.

        Returns:
            Tuple of (ServerA config, ServerB config) as multiplex_stats.models.ServerConfig objects
        """
        servers = ServerConfig.query.filter_by(is_active=True).order_by(ServerConfig.server_order).all()

        server_a = servers[0].to_multiplex_config() if len(servers) > 0 else None
        server_b = servers[1].to_multiplex_config() if len(servers) > 1 else None

        return server_a, server_b

    @staticmethod
    def get_analytics_settings() -> object:
        """Get analytics settings in multiplex_stats format."""
        settings = AnalyticsSettings.query.first()
        return settings.to_multiplex_settings() if settings else None

    @staticmethod
    def has_valid_config() -> bool:
        """Check if at least one server is configured."""
        return ServerConfig.query.filter_by(is_active=True).count() > 0

    @staticmethod
    def get_active_servers() -> List[ServerConfig]:
        """Get all active servers ordered by server_order."""
        return ServerConfig.query.filter_by(is_active=True).order_by(ServerConfig.server_order).all()

    @staticmethod
    def create_or_update_server(multiplex_server_config, order: int):
        """
        Create or update server from multiplex_stats ServerConfig.

        Args:
            multiplex_server_config: multiplex_stats.models.ServerConfig object
            order: Server order (0=ServerA, 1=ServerB)
        """
        server = ServerConfig.query.filter_by(name=multiplex_server_config.name).first()

        if server:
            server.ip_address = multiplex_server_config.ip_address
            server.api_key = multiplex_server_config.api_key
            server.use_ssl = getattr(multiplex_server_config, 'use_ssl', False)
            server.verify_ssl = getattr(multiplex_server_config, 'verify_ssl', False)
            server.server_order = order
        else:
            server = ServerConfig(
                name=multiplex_server_config.name,
                ip_address=multiplex_server_config.ip_address,
                api_key=multiplex_server_config.api_key,
                use_ssl=getattr(multiplex_server_config, 'use_ssl', False),
                verify_ssl=getattr(multiplex_server_config, 'verify_ssl', False),
                server_order=order
            )
            db.session.add(server)

        db.session.commit()

    @staticmethod
    def update_analytics_settings(multiplex_settings):
        """
        Update analytics settings from multiplex_stats AnalyticsSettings.

        Args:
            multiplex_settings: multiplex_stats.config_loader.AnalyticsSettings object
        """
        settings = AnalyticsSettings.query.first()

        if not settings:
            settings = AnalyticsSettings()
            db.session.add(settings)

        settings.daily_trend_days = multiplex_settings.daily_trend_days
        settings.monthly_trend_months = multiplex_settings.monthly_trend_months
        settings.history_days = multiplex_settings.history_days
        settings.top_movies = multiplex_settings.top_movies
        settings.top_tv_shows = multiplex_settings.top_tv_shows
        settings.top_users = multiplex_settings.top_users

        db.session.commit()
