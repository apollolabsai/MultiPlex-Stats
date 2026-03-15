"""
Settings routes for managing server configuration and analytics settings.
"""
import locale
import os
import platform
import sqlite3
import sys
from datetime import datetime

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from flask_app.models import db, ServerConfig, AnalyticsSettings
from flask_app.services.config_service import ConfigService
from flask_app.services.history_sync_service import HistorySyncService
from flask_app.services.media_service import MediaService
from flask_app.services.media_lifetime_stats_service import MediaLifetimeStatsService
from flask_app.utils.validators import validate_server_config
from multiplex_stats.timezone_utils import get_local_timezone

settings_bp = Blueprint('settings', __name__)


def _project_root() -> str:
    """Return the repository root for this application."""
    return os.path.abspath(os.path.join(current_app.root_path, '..'))


def _get_config_file_display() -> str:
    """Return the legacy config.ini path if present, otherwise explain the current source."""
    config_path = os.path.join(_project_root(), 'config.ini')
    if os.path.exists(config_path):
        return config_path
    return 'Not present. MultiPlex Stats currently uses database + environment settings.'


def _get_database_display() -> tuple[str, str]:
    """Return the database location and status string."""
    database_uri = (current_app.config.get('SQLALCHEMY_DATABASE_URI') or '').strip()
    if database_uri == 'sqlite:///:memory:':
        return 'In-memory SQLite database', 'Memory only'

    sqlite_prefix = 'sqlite:///'
    if database_uri.startswith(sqlite_prefix):
        db_path = database_uri[len(sqlite_prefix):]
        if not db_path:
            return 'SQLite database path unavailable', 'Unknown'
        if not db_path.startswith('/'):
            db_path = os.path.abspath(os.path.join(_project_root(), db_path))
        status = 'Exists' if os.path.exists(db_path) else 'Missing'
        return db_path, status

    if database_uri:
        dialect = database_uri.split(':', 1)[0]
        return f'{dialect} database configured via SQLALCHEMY_DATABASE_URI', 'Configured'

    return 'Database URI not configured', 'Unknown'


def _get_system_language() -> str:
    """Return the active system locale in a compact display form."""
    locale_value = locale.setlocale(locale.LC_CTYPE, None) or ''
    if locale_value and locale_value not in ('C', 'POSIX'):
        return locale_value

    language_code, encoding = locale.getlocale()
    if language_code and encoding:
        return f'{language_code}.{encoding}'
    if language_code:
        return language_code
    if locale_value:
        return locale_value
    return 'Unknown'


def _build_runtime_configuration() -> list[dict[str, str]]:
    """Collect runtime configuration details for the settings page."""
    timezone_name = get_local_timezone()
    timezone_offset = datetime.now(timezone_name).strftime('%z')
    db_value, db_status = _get_database_display()

    return [
        {'label': 'Git Branch', 'value': current_app.config.get('GIT_BRANCH', 'unknown')},
        {'label': 'Git Commit Hash', 'value': current_app.config.get('GIT_COMMIT_HASH', 'unknown')},
        {'label': 'Configuration File', 'value': _get_config_file_display()},
        {'label': 'Database File', 'value': db_value, 'meta': f'Status: {db_status}'},
        {'label': 'Log File', 'value': os.path.join(current_app.instance_path, 'logs', 'multiplex_stats.log')},
        {'label': 'Cache Directory', 'value': os.path.join(current_app.instance_path, 'cache')},
        {'label': 'Arguments', 'value': repr(sys.argv[1:])},
        {'label': 'Platform', 'value': f'{platform.system()} {platform.release()} ({platform.version()})'},
        {'label': 'System Timezone', 'value': f'{getattr(timezone_name, "key", str(timezone_name))} (UTC{timezone_offset})'},
        {'label': 'System Language', 'value': _get_system_language()},
        {'label': 'Python Version', 'value': sys.version.replace('\n', ' ')},
        {'label': 'SQLite Version', 'value': sqlite3.sqlite_version},
    ]


@settings_bp.route('/')
def index():
    """Settings page with all configuration forms."""
    servers = ServerConfig.query.order_by(ServerConfig.server_order).all()
    settings = AnalyticsSettings.query.first()

    # Get history sync status and stats
    sync_service = HistorySyncService()
    sync_status = sync_service.get_sync_status()
    history_stats = sync_service.get_history_stats()
    media_sync_status = MediaService().get_sync_status()
    lifetime_sync_status = MediaLifetimeStatsService().get_sync_status()
    env_stadia_key_present = bool((current_app.config.get('STADIA_MAPS_API_KEY', '') or '').strip())
    stored_stadia_key_present = bool((getattr(settings, 'stadia_maps_api_key', '') or '').strip()) if settings else False
    env_mdblist_key_present = bool((current_app.config.get('MDBLIST_API_KEY', '') or '').strip())
    stored_mdblist_key_present = bool((getattr(settings, 'mdblist_api_key', '') or '').strip()) if settings else False
    runtime_configuration = _build_runtime_configuration()

    return render_template('settings.html',
                          servers=servers,
                          settings=settings,
                          sync_status=sync_status,
                          history_stats=history_stats,
                          media_sync_status=media_sync_status,
                          lifetime_sync_status=lifetime_sync_status,
                          env_stadia_key_present=env_stadia_key_present,
                          stored_stadia_key_present=stored_stadia_key_present,
                          env_mdblist_key_present=env_mdblist_key_present,
                          stored_mdblist_key_present=stored_mdblist_key_present,
                          runtime_configuration=runtime_configuration)


@settings_bp.route('/server/add', methods=['POST'])
def add_server():
    """Add or update server configuration."""
    try:
        data = {
            'name': request.form.get('name'),
            'ip_address': request.form.get('ip_address'),
            'api_key': request.form.get('api_key'),
            'use_ssl': request.form.get('use_ssl') == '1',
            'verify_ssl': request.form.get('verify_ssl') == '1',
            'server_order': int(request.form.get('server_order', 0))
        }

        # Validate inputs
        errors = validate_server_config(data)
        if errors:
            for error in errors:
                flash(error, 'error')
            return redirect(url_for('settings.index'))

        # Check if updating existing or creating new
        server_id = request.form.get('server_id')
        if server_id:
            server = ServerConfig.query.get(server_id)
            if server:
                server.name = data['name']
                server.ip_address = data['ip_address']
                server.api_key = data['api_key']
                server.use_ssl = data['use_ssl']
                server.verify_ssl = data['verify_ssl']
                server.server_order = data['server_order']
            else:
                flash('Server not found.', 'error')
                return redirect(url_for('settings.index'))
        else:
            server = ServerConfig(**data)
            db.session.add(server)

        db.session.commit()
        flash(f'Server "{data["name"]}" saved successfully!', 'success')

    except Exception as e:
        flash(f'Error saving server: {str(e)}', 'error')

    return redirect(url_for('settings.index'))


@settings_bp.route('/server/<int:server_id>/delete', methods=['POST'])
def delete_server(server_id):
    """Delete server configuration."""
    server = ServerConfig.query.get_or_404(server_id)
    db.session.delete(server)
    db.session.commit()
    flash(f'Server "{server.name}" deleted.', 'success')
    return redirect(url_for('settings.index'))


@settings_bp.route('/analytics', methods=['POST'])
def update_analytics_settings():
    """Update analytics settings."""
    settings = AnalyticsSettings.query.first()

    try:
        settings.daily_trend_days = int(request.form.get('daily_trend_days', 60))
        settings.monthly_trend_months = int(request.form.get('monthly_trend_months', 60))
        settings.history_days = int(request.form.get('history_days', 60))
        settings.top_movies = int(request.form.get('top_movies', 30))
        settings.top_tv_shows = int(request.form.get('top_tv_shows', 30))
        settings.top_users = int(request.form.get('top_users', 20))

        db.session.commit()
        flash('Analytics settings updated!', 'success')

    except ValueError as e:
        flash('Please enter valid numbers for all settings.', 'error')
    except Exception as e:
        flash(f'Error updating settings: {str(e)}', 'error')

    return redirect(url_for('settings.index'))


@settings_bp.route('/map', methods=['POST'])
def update_map_settings():
    """Update map-related settings."""
    settings = AnalyticsSettings.query.first()
    if not settings:
        settings = AnalyticsSettings()
        db.session.add(settings)

    try:
        settings.stadia_maps_api_key = (request.form.get('stadia_maps_api_key', '') or '').strip() or None
        db.session.commit()
        if settings.stadia_maps_api_key:
            flash('Stadia Maps API key saved. Dashboard map will use the stored key.', 'success')
        else:
            flash('Stored Stadia Maps API key cleared. The app will fall back to the environment variable if one is set.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error updating map settings: {str(e)}', 'error')

    return redirect(url_for('settings.index'))


@settings_bp.route('/mdblist', methods=['POST'])
def update_mdblist_settings():
    """Update MDBList API key setting."""
    settings = AnalyticsSettings.query.first()
    if not settings:
        settings = AnalyticsSettings()
        db.session.add(settings)

    try:
        settings.mdblist_api_key = (request.form.get('mdblist_api_key', '') or '').strip() or None
        db.session.commit()
        if settings.mdblist_api_key:
            flash('MDBList API key saved. Ratings will be fetched on the next media sync.', 'success')
        else:
            flash('Stored MDBList API key cleared. The app will fall back to the environment variable if one is set.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error updating MDBList settings: {str(e)}', 'error')

    return redirect(url_for('settings.index'))


@settings_bp.route('/import-from-ini', methods=['POST'])
def import_from_ini():
    """Import settings from existing config.ini file."""
    try:
        from multiplex_stats.config_loader import load_config

        # Load from config.ini
        server_a, server_b, analytics_settings = load_config()

        # Import ServerA
        ConfigService.create_or_update_server(server_a, order=0)

        # Import ServerB if exists
        if server_b:
            ConfigService.create_or_update_server(server_b, order=1)

        # Import analytics settings
        ConfigService.update_analytics_settings(analytics_settings)

        flash('Successfully imported configuration from config.ini!', 'success')

    except Exception as e:
        flash(f'Error importing config.ini: {str(e)}', 'error')

    return redirect(url_for('settings.index'))


# History Sync Routes

@settings_bp.route('/history/backfill', methods=['POST'])
def start_history_backfill():
    """Start a full history backfill."""
    try:
        days = int(request.form.get('backfill_days', 60))
        if days < 1 or days > 3650:  # Max ~10 years
            flash('Days must be between 1 and 3650.', 'error')
            return redirect(url_for('settings.index'))

        # Update the backfill days setting
        settings = AnalyticsSettings.query.first()
        if settings:
            settings.history_backfill_days = days
            db.session.commit()

        sync_service = HistorySyncService()
        if not sync_service.start_backfill_async(days):
            flash('A sync is already in progress.', 'error')
        else:
            flash(f'Started loading {days} days of history. Check progress below.', 'info')

    except ValueError:
        flash('Please enter a valid number of days.', 'error')
    except Exception as e:
        flash(f'Error starting backfill: {str(e)}', 'error')

    return redirect(url_for('settings.index'))


@settings_bp.route('/history/sync-status', methods=['GET'])
def get_history_sync_status():
    """Get current sync status for polling (JSON endpoint)."""
    sync_service = HistorySyncService()
    return jsonify(sync_service.get_sync_status())


@settings_bp.route('/history/full-backfill', methods=['POST'])
def start_full_history_backfill():
    """Start full history import without date filtering."""
    try:
        sync_service = HistorySyncService()
        if not sync_service.start_full_backfill_async():
            return jsonify({'error': 'A history sync is already in progress.'}), 409
        return jsonify({'status': 'started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
