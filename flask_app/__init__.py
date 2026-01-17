"""
Flask application factory.
"""
import os
from flask import Flask


def create_app(config_name='development'):
    """Create and configure the Flask application."""
    app = Flask(__name__, instance_relative_config=True)

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
