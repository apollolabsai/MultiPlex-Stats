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
    verify_ssl = db.Column(db.Boolean, default=False)
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
    history_days = db.Column(db.Integer, default=60)  # Used for User Activity and Top Content charts
    history_backfill_days = db.Column(db.Integer, default=365)  # Days to load for viewing history table
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


class ViewingHistory(db.Model):
    """Cached viewing history from Tautulli."""
    __tablename__ = 'viewing_history'

    id = db.Column(db.Integer, primary_key=True)
    row_id = db.Column(db.Integer, nullable=False, unique=True, index=True)  # Tautulli's unique ID
    server_name = db.Column(db.String(100), nullable=False)
    server_order = db.Column(db.Integer, default=0)  # 0=ServerA, 1=ServerB

    # User info
    user_id = db.Column(db.Integer, nullable=True)
    user = db.Column(db.String(255), nullable=True)

    # Media info
    media_type = db.Column(db.String(50), nullable=True)  # movie, episode, track
    full_title = db.Column(db.String(500), nullable=True)
    title = db.Column(db.String(500), nullable=True)  # Movie title or episode title
    grandparent_title = db.Column(db.String(500), nullable=True)  # Show name for TV
    parent_media_index = db.Column(db.Integer, nullable=True)  # Season number
    media_index = db.Column(db.Integer, nullable=True)  # Episode number
    year = db.Column(db.Integer, nullable=True)
    rating_key = db.Column(db.Integer, nullable=True)
    parent_rating_key = db.Column(db.Integer, nullable=True)
    grandparent_rating_key = db.Column(db.Integer, nullable=True)
    thumb = db.Column(db.String(500), nullable=True)

    # Playback info
    started = db.Column(db.Integer, nullable=True)  # Unix timestamp
    stopped = db.Column(db.Integer, nullable=True)  # Unix timestamp
    duration = db.Column(db.Integer, nullable=True)  # Duration in seconds
    play_duration = db.Column(db.Integer, nullable=True)  # Actual play time
    percent_complete = db.Column(db.Integer, nullable=True)
    watched_status = db.Column(db.Float, nullable=True)

    # Client/platform info
    ip_address = db.Column(db.String(50), nullable=True)
    platform = db.Column(db.String(100), nullable=True)
    product = db.Column(db.String(100), nullable=True)
    player = db.Column(db.String(100), nullable=True)
    quality_profile = db.Column(db.String(100), nullable=True)
    transcode_decision = db.Column(db.String(50), nullable=True)  # direct play, transcode, etc.

    # Location info
    location = db.Column(db.String(50), nullable=True)  # lan, wan
    geo_city = db.Column(db.String(100), nullable=True)
    geo_region = db.Column(db.String(100), nullable=True)
    geo_country = db.Column(db.String(100), nullable=True)

    # Derived fields (calculated on insert)
    date_played = db.Column(db.Date, nullable=True, index=True)  # Date in configured timezone
    time_played = db.Column(db.String(20), nullable=True)  # Time string in configured timezone

    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class IPGeolocation(db.Model):
    """Cache for IP geolocation lookups."""
    __tablename__ = 'ip_geolocation'

    id = db.Column(db.Integer, primary_key=True)
    ip_address = db.Column(db.String(50), nullable=False, unique=True, index=True)
    city = db.Column(db.String(100), nullable=True)
    region = db.Column(db.String(100), nullable=True)
    country = db.Column(db.String(100), nullable=True)
    isp = db.Column(db.String(200), nullable=True)
    lookup_date = db.Column(db.DateTime, default=datetime.utcnow)


class HistorySyncStatus(db.Model):
    """Track history sync status for progress polling."""
    __tablename__ = 'history_sync_status'

    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(20), default='idle')  # idle, running, success, failed
    sync_type = db.Column(db.String(20), nullable=True)  # backfill, incremental
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)

    # Progress tracking
    records_fetched = db.Column(db.Integer, default=0)
    records_total = db.Column(db.Integer, nullable=True)  # Estimated total
    current_server = db.Column(db.String(100), nullable=True)

    # Result info
    records_inserted = db.Column(db.Integer, default=0)
    records_skipped = db.Column(db.Integer, default=0)  # Duplicates
    error_message = db.Column(db.Text, nullable=True)

    # Last successful sync
    last_sync_date = db.Column(db.DateTime, nullable=True)
    last_sync_record_count = db.Column(db.Integer, nullable=True)


class MediaSyncStatus(db.Model):
    """Track media library sync status for progress polling."""
    __tablename__ = 'media_sync_status'

    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(20), default='idle')  # idle, running, success, failed
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)

    # Progress tracking
    current_step = db.Column(db.String(100), nullable=True)  # e.g., "Fetching movies from Server A"
    records_fetched = db.Column(db.Integer, default=0)
    records_total = db.Column(db.Integer, nullable=True)

    # Result info
    movies_count = db.Column(db.Integer, default=0)
    tv_shows_count = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text, nullable=True)

    # Last successful sync
    last_sync_date = db.Column(db.DateTime, nullable=True)


class CachedMedia(db.Model):
    """Cached media library data from Tautulli."""
    __tablename__ = 'cached_media'

    id = db.Column(db.Integer, primary_key=True)
    media_type = db.Column(db.String(20), nullable=False, index=True)  # movie, show

    # Common fields
    title = db.Column(db.String(500), nullable=False)
    year = db.Column(db.Integer, nullable=True)
    added_at = db.Column(db.Integer, nullable=True)  # Unix timestamp (MAX across servers)
    last_played = db.Column(db.Integer, nullable=True)  # Unix timestamp (MAX across servers)
    play_count = db.Column(db.Integer, default=0)  # SUM across servers
    file_size = db.Column(db.BigInteger, default=0)  # SUM across servers (bytes)

    # Movie-specific fields
    video_codec = db.Column(db.String(50), nullable=True)
    video_resolution = db.Column(db.String(50), nullable=True)

    # Unique constraint on title + year + media_type
    __table_args__ = (
        db.UniqueConstraint('title', 'year', 'media_type', name='uq_media_title_year_type'),
    )
