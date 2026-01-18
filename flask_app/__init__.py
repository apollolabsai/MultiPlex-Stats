"""
Flask application factory.
"""
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import Flask


def create_app(config_name='development'):
    """Create and configure the Flask application."""
    app = Flask(__name__, instance_relative_config=True)

    # Get timezone from TZ environment variable, default to PST
    tz_name = os.environ.get('TZ', 'America/Los_Angeles')
    try:
        app_timezone = ZoneInfo(tz_name)
    except Exception:
        app_timezone = ZoneInfo('America/Los_Angeles')

    # Register custom Jinja filters
    @app.template_filter('timestamp_to_date')
    def timestamp_to_date(timestamp):
        """Convert Unix timestamp to readable date string in configured timezone."""
        if timestamp is None:
            return 'Never'
        try:
            # Convert timestamp to UTC datetime, then to configured timezone
            dt_utc = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            dt_local = dt_utc.astimezone(app_timezone)
            return dt_local.strftime('%Y-%m-%d')
        except (ValueError, TypeError, OSError):
            return 'Unknown'

    # Load configuration
    if config_name == 'production':
        app.config.from_object('flask_app.config.ProductionConfig')
    else:
        app.config.from_object('flask_app.config.DevelopmentConfig')

    # Ensure instance folder exists
    try:
        os.makedirs(app.instance_path, exist_ok=True)
        os.makedirs(os.path.join(app.instance_path, 'cache'), exist_ok=True)
    except OSError:
        pass

    # Initialize extensions
    from flask_app.models import db
    db.init_app(app)

    # Register blueprints
    from flask_app.routes.main import main_bp
    from flask_app.routes.settings import settings_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(settings_bp, url_prefix='/settings')

    # Create database tables and initialize default settings
    with app.app_context():
        db.create_all()
        _initialize_default_settings()

    return app


def _initialize_default_settings():
    """Create default AnalyticsSettings and HistorySyncStatus if none exist."""
    from flask_app.models import db, AnalyticsSettings, HistorySyncStatus

    if AnalyticsSettings.query.first() is None:
        default_settings = AnalyticsSettings()
        db.session.add(default_settings)
        db.session.commit()

    if HistorySyncStatus.query.first() is None:
        default_sync_status = HistorySyncStatus()
        db.session.add(default_sync_status)
        db.session.commit()
