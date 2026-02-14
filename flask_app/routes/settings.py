"""
Settings routes for managing server configuration and analytics settings.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_app.models import db, ServerConfig, AnalyticsSettings
from flask_app.services.config_service import ConfigService
from flask_app.services.history_sync_service import HistorySyncService
from flask_app.services.media_service import MediaService
from flask_app.services.media_lifetime_stats_service import MediaLifetimeStatsService
from flask_app.utils.validators import validate_server_config

settings_bp = Blueprint('settings', __name__)


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

    return render_template('settings.html',
                          servers=servers,
                          settings=settings,
                          sync_status=sync_status,
                          history_stats=history_stats,
                          media_sync_status=media_sync_status,
                          lifetime_sync_status=lifetime_sync_status)


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
