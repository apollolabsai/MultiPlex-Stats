"""
Flask application factory.
"""
import os
from datetime import datetime, timezone
from flask import Flask
from multiplex_stats.timezone_utils import get_local_timezone


def create_app(config_name='development'):
    """Create and configure the Flask application."""
    app = Flask(__name__, instance_relative_config=True)

    # Get timezone from TZ environment variable, default to America/Los_Angeles
    app_timezone = get_local_timezone()

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

    @app.template_filter('datetime_to_local')
    def datetime_to_local(value, fmt='%Y-%m-%d %H:%M:%S'):
        """Format a datetime value in the configured timezone."""
        if value is None:
            return 'Unknown'
        try:
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(app_timezone).strftime(fmt)
        except (ValueError, TypeError, OSError):
            return 'Unknown'

    @app.context_processor
    def inject_timezone_name():
        """Expose the configured timezone name to templates."""
        return {'timezone_name': getattr(app_timezone, 'key', 'Local')}

    # Cache build info from environment (injected at build time).
    commit_hash = os.environ.get('GIT_COMMIT_HASH', 'unknown')
    commit_date_raw = os.environ.get('GIT_COMMIT_DATE', 'unknown')
    git_branch = os.environ.get('GIT_BRANCH', 'unknown')
    commit_date_str = commit_date_raw
    if commit_date_raw not in ('', 'unknown'):
        try:
            commit_ts = int(commit_date_raw)
            commit_dt = datetime.fromtimestamp(commit_ts, tz=timezone.utc).astimezone(app_timezone)
            commit_date_str = commit_dt.strftime('%Y-%m-%d %I:%M %p')
        except (ValueError, OSError):
            commit_date_str = commit_date_raw

    app.config['GIT_COMMIT_HASH'] = commit_hash
    app.config['GIT_COMMIT_DATE'] = commit_date_str
    app.config['GIT_BRANCH'] = git_branch

    @app.context_processor
    def inject_git_info():
        """Expose cached git commit info to templates."""
        return {
            'git_commit_hash': app.config.get('GIT_COMMIT_HASH', 'unknown'),
            'git_commit_date': app.config.get('GIT_COMMIT_DATE', 'unknown'),
            'git_branch': app.config.get('GIT_BRANCH', 'unknown')
        }

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
