"""
Database models for Flask application.
"""
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class ServerConfig(db.Model):
    """Server configuration stored in database."""
    __tablename__ = 'server_configs'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    ip_address = db.Column(db.String(255), nullable=False)
    api_key = db.Column(db.String(255), nullable=False)
    use_ssl = db.Column(db.Boolean, default=False)
    verify_ssl = db.Column(db.Boolean, default=True)
    is_active = db.Column(db.Boolean, default=True)
    server_order = db.Column(db.Integer, default=0)  # 0=ServerA, 1=ServerB
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_multiplex_config(self):
        """Convert to multiplex_stats.models.ServerConfig"""
        from multiplex_stats.models import ServerConfig as MultiplexServerConfig
        return MultiplexServerConfig(
            name=self.name,
            ip_address=self.ip_address,
            api_key=self.api_key,
            use_ssl=self.use_ssl,
            verify_ssl=self.verify_ssl
        )


class AnalyticsSettings(db.Model):
    """Analytics configuration settings (singleton table)."""
    __tablename__ = 'analytics_settings'

    id = db.Column(db.Integer, primary_key=True)
    daily_trend_days = db.Column(db.Integer, default=60)
    monthly_trend_months = db.Column(db.Integer, default=60)
    history_days = db.Column(db.Integer, default=60)
    history_table_days = db.Column(db.Integer, default=60)
    top_movies = db.Column(db.Integer, default=30)
    top_tv_shows = db.Column(db.Integer, default=30)
    top_users = db.Column(db.Integer, default=20)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_multiplex_settings(self):
        """Convert to multiplex_stats.config_loader.AnalyticsSettings"""
        from multiplex_stats.config_loader import AnalyticsSettings as MultiplexSettings
        return MultiplexSettings(
            daily_trend_days=self.daily_trend_days,
            monthly_trend_months=self.monthly_trend_months,
            history_days=self.history_days,
            top_movies=self.top_movies,
            top_tv_shows=self.top_tv_shows,
            top_users=self.top_users
        )


class AnalyticsRun(db.Model):
    """Track analytics execution history."""
    __tablename__ = 'analytics_runs'

    id = db.Column(db.Integer, primary_key=True)
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(20), default='running')  # running, success, failed
    error_message = db.Column(db.Text, nullable=True)
    total_plays = db.Column(db.Integer, nullable=True)
    total_users = db.Column(db.Integer, nullable=True)
    summary_json = db.Column(db.Text, nullable=True)  # JSON blob of key stats
