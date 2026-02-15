"""
Main application routes for dashboard and analytics execution.
"""
import json
from datetime import datetime, timezone
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify, abort
from flask_app.models import db, AnalyticsRun, ViewingHistory
from flask_app.services.analytics_service import AnalyticsService
from flask_app.services.media_service import MediaService
from flask_app.services.media_lifetime_stats_service import MediaLifetimeStatsService
from flask_app.services.content_service import ContentService
from flask_app.services.config_service import ConfigService
from multiplex_stats.timezone_utils import get_local_timezone

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Redirect root to the dashboard."""
    return redirect(url_for('main.dashboard'))


@main_bp.route('/run-analytics', methods=['POST'])
def run_analytics():
    """Execute analytics synchronously, then trigger a lifetime cache rebuild."""
    try:
        # Validate configuration exists
        if not ConfigService.has_valid_config():
            flash('Please configure at least one server before running analytics.', 'error')
            return redirect(url_for('settings.index'))

        daily_trend_days = None
        if request.form:
            custom_requested = request.form.get('apply_custom')
            if custom_requested:
                custom_days = request.form.get('daily_trend_days_custom', type=int)
                if not custom_days or custom_days < 1 or custom_days > 3650:
                    flash('Please enter a valid number of days (1-3650).', 'error')
                    return redirect(request.referrer or url_for('main.dashboard'))
                daily_trend_days = custom_days
            else:
                daily_trend_days = request.form.get('daily_trend_days', type=int)

        # Create run record
        run = AnalyticsRun(status='running')
        db.session.add(run)
        db.session.commit()

        # Execute analytics (synchronous - blocks for 30-60s)
        service = AnalyticsService()
        result = service.run_full_analytics(run.id, daily_trend_days_override=daily_trend_days)

        # Update run record with results
        run.status = 'success'
        run.completed_at = datetime.utcnow()
        run.total_plays = result['total_plays']
        run.total_users = result['total_users']
        run.summary_json = json.dumps(result['summary'])
        db.session.commit()

        try:
            lifetime_started = MediaLifetimeStatsService().start_sync(trigger='dashboard_refresh')
            if lifetime_started:
                flash('Analytics completed. Lifetime cache rebuild started.', 'success')
            else:
                flash('Analytics completed. Lifetime cache rebuild is already running.', 'success')
        except Exception as lifetime_error:
            flash('Analytics completed, but lifetime cache rebuild could not be started.', 'error')
            print(f"Lifetime cache rebuild start error: {lifetime_error}")
        return redirect(url_for('main.dashboard'))

    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        print(f"Analytics error traceback:\n{error_traceback}")

        if 'run' in locals():
            run.status = 'failed'
            run.completed_at = datetime.utcnow()
            run.error_message = f"{str(e)}\n\nTraceback:\n{error_traceback}"
            db.session.commit()

        flash(f'Analytics failed: {str(e)}', 'error')
        return redirect(url_for('main.dashboard'))


@main_bp.route('/dashboard')
def dashboard():
    """Display analytics dashboard with all charts."""
    last_success_run = AnalyticsRun.query.filter_by(status='success').order_by(
        AnalyticsRun.completed_at.desc()
    ).first()
    last_run = AnalyticsRun.query.order_by(AnalyticsRun.started_at.desc()).first()
    servers = ConfigService.get_active_servers()
    has_config = len(servers) > 0

    if not last_success_run:
        return render_template('dashboard.html',
                              has_data=False,
                              last_run=last_run,
                              has_config=has_config,
                              servers=servers)

    # Convert completed_at from UTC to Pacific Time
    completed_at_pt = last_success_run.completed_at.replace(tzinfo=timezone.utc).astimezone(get_local_timezone())

    # Load cached chart JSON and table data from service
    service = AnalyticsService()
    charts_json = service.get_cached_charts(last_success_run.id)
    table_data = service.get_cached_table_data(last_success_run.id)
    summary = json.loads(last_success_run.summary_json)

    # Get current streaming activity (real-time)
    current_activity = service.get_current_activity()

    return render_template('dashboard.html',
                          has_data=True,
                          charts_json=charts_json,
                          table_data=table_data,
                          summary=summary,
                          last_run=last_run,
                          completed_at_pt=completed_at_pt,
                          current_activity=current_activity,
                          has_config=has_config,
                          servers=servers)


@main_bp.route('/api/current-activity')
def api_current_activity():
    """Return current streaming activity table markup."""
    service = AnalyticsService()
    current_activity = service.get_current_activity()
    return render_template('partials/current_activity.html',
                          current_activity=current_activity)


@main_bp.route('/api/viewing-history')
def api_viewing_history():
    """
    AJAX endpoint for DataTables server-side processing of viewing history.

    Query params (from DataTables):
        draw: Request counter for DataTables
        start: Row offset
        length: Number of rows to return
        search[value]: Search filter
        order[0][column]: Column index to sort by
        order[0][dir]: Sort direction (asc/desc)

    Returns:
        JSON with DataTables format
    """
    # Parse DataTables parameters
    draw = request.args.get('draw', 1, type=int)
    start = request.args.get('start', 0, type=int)
    length = request.args.get('length', 50, type=int)
    search_value = request.args.get('search[value]', '')
    order_column = request.args.get('order[0][column]', 0, type=int)
    order_dir = request.args.get('order[0][dir]', 'desc')

    # Get paginated data
    service = AnalyticsService()
    result = service.get_viewing_history_paginated(
        start=start,
        length=length,
        search_value=search_value,
        order_column=order_column,
        order_dir=order_dir
    )

    # Add draw counter for DataTables
    result['draw'] = draw

    return jsonify(result)


@main_bp.route('/api/users-list')
def api_users_list():
    """Return list of users for dropdown filters."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    try:
        service = AnalyticsService()
        users = service.get_users_for_filter()
        return jsonify({'users': users})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/daily-chart')
def api_daily_chart():
    """Return daily chart JSON for the requested day range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    days = request.args.get('days', type=int)
    if not days or days < 1 or days > 3650:
        return jsonify({'error': 'Invalid day range. Use 1-3650.'}), 400

    user_id = request.args.get('user_id', type=int)

    try:
        service = AnalyticsService()
        result = service.get_daily_chart_json(daily_trend_days=days, user_id=user_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/monthly-chart')
def api_monthly_chart():
    """Return monthly chart JSON for the requested month range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    months = request.args.get('months', type=int)
    if not months or months < 1 or months > 120:
        return jsonify({'error': 'Invalid month range. Use 1-120.'}), 400

    user_id = request.args.get('user_id', type=int)

    try:
        service = AnalyticsService()
        result = service.get_monthly_chart_json(monthly_trend_months=months, user_id=user_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/distribution-charts')
def api_distribution_charts():
    """Return distribution chart JSON for the requested day range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    days = request.args.get('days', type=int)
    if not days or days < 1 or days > 3650:
        return jsonify({'error': 'Invalid day range. Use 1-3650.'}), 400

    try:
        service = AnalyticsService()
        result = service.get_distribution_charts_json(days=days)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/concurrent-streams')
def api_concurrent_streams():
    """Return concurrent streams chart JSON for the requested day range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    days = request.args.get('days', type=int)
    if not days or days < 1 or days > 3650:
        return jsonify({'error': 'Invalid day range. Use 1-3650.'}), 400

    try:
        service = AnalyticsService()
        result = service.get_concurrent_streams_json(days=days)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/ip-lookup')
def api_ip_lookup():
    """Lookup geo data for a single IP address and cache results."""
    ip_address = request.args.get('ip', type=str)
    if not ip_address:
        return jsonify({'error': 'IP address is required.'}), 400

    try:
        from flask_app.services.geolocation_service import GeolocationService
        from flask_app.models import ViewingHistory

        geo_service = GeolocationService()
        geo_data = geo_service.lookup_ip(ip_address)

        # Find a location value from history if available.
        location = (
            ViewingHistory.query.with_entities(ViewingHistory.location)
            .filter_by(ip_address=ip_address)
            .first()
        )
        location_value = location[0] if location and location[0] else ''

        # Update cached geo fields for this IP in viewing history.
        ViewingHistory.query.filter_by(ip_address=ip_address).update({
            'geo_city': geo_data.get('city'),
            'geo_region': geo_data.get('region'),
            'geo_country': geo_data.get('country')
        })
        db.session.commit()

        return jsonify({
            'ip': ip_address,
            'location': location_value,
            'city': geo_data.get('city') or '',
            'region': geo_data.get('region') or '',
            'country': geo_data.get('country') or '',
            'isp': geo_data.get('isp') or ''
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/user-chart')
def api_user_chart():
    """Return user activity chart JSON for the requested day range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    days = request.args.get('days', type=int)
    if not days or days < 1 or days > 3650:
        return jsonify({'error': 'Invalid day range. Use 1-3650.'}), 400

    top_n = request.args.get('top_n', type=int)
    if top_n is not None and (top_n < 1 or top_n > 100):
        return jsonify({'error': 'Invalid top_n. Use 1-100.'}), 400

    try:
        service = AnalyticsService()
        result = service.get_user_chart_json(days=days, top_n=top_n)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/movie-chart')
def api_movie_chart():
    """Return top movies chart JSON for the requested day range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    days = request.args.get('days', type=int)
    if not days or days < 1 or days > 3650:
        return jsonify({'error': 'Invalid day range. Use 1-3650.'}), 400

    top_n = request.args.get('top_n', type=int)
    if top_n is not None and (top_n < 1 or top_n > 100):
        return jsonify({'error': 'Invalid top_n. Use 1-100.'}), 400

    try:
        service = AnalyticsService()
        result = service.get_movie_chart_json(days=days, top_n=top_n)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/tv-chart')
def api_tv_chart():
    """Return top TV shows chart JSON for the requested day range."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    days = request.args.get('days', type=int)
    if not days or days < 1 or days > 3650:
        return jsonify({'error': 'Invalid day range. Use 1-3650.'}), 400

    top_n = request.args.get('top_n', type=int)
    if top_n is not None and (top_n < 1 or top_n > 100):
        return jsonify({'error': 'Invalid top_n. Use 1-100.'}), 400

    try:
        service = AnalyticsService()
        result = service.get_tv_chart_json(days=days, top_n=top_n)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/viewing-history')
def viewing_history():
    """Display viewing history page with DataTables."""
    return render_template('viewing_history.html')


@main_bp.route('/content/<int:history_id>')
def content_details(history_id):
    """Display detail page for clicked movie/show history entry."""
    service = ContentService()
    details = service.get_content_details(history_id)
    if not details:
        abort(404)
    return render_template('content_detail.html', **details)


@main_bp.route('/content/media/<int:media_id>')
def media_content_details(media_id):
    """Display detail page for clicked media-library row."""
    service = ContentService()
    details = service.get_content_details_for_media(media_id)
    if not details:
        abort(404)
    return render_template('content_detail.html', **details)


@main_bp.route('/api/viewing-history-stats')
def api_viewing_history_stats():
    """Get statistics from the viewing history database table."""
    from sqlalchemy import func, distinct
    from datetime import datetime, timedelta

    # Get total record count and date range
    total_plays = ViewingHistory.query.count()

    if total_plays == 0:
        return jsonify({
            'total_plays': 0,
            'total_users': 0,
            'servers': [],
            'days_of_history': 0,
            'oldest_date': None,
            'newest_date': None
        })

    # Count distinct users
    total_users = db.session.query(func.count(distinct(ViewingHistory.user))).scalar() or 0

    # Get plays per server
    server_stats = db.session.query(
        ViewingHistory.server_name,
        func.count(ViewingHistory.id).label('plays')
    ).group_by(ViewingHistory.server_name).all()

    servers = [{'name': s.server_name, 'plays': s.plays} for s in server_stats]

    # Get date range using configured timezone dates when available
    oldest = db.session.query(func.min(ViewingHistory.date_played)).scalar()
    newest = db.session.query(func.max(ViewingHistory.date_played)).scalar()

    days_of_history = 0
    oldest_date = None
    newest_date = None

    if oldest and newest:
        days_of_history = (newest - oldest).days + 1
        oldest_date = oldest.strftime('%Y-%m-%d')
        newest_date = newest.strftime('%Y-%m-%d')

    return jsonify({
        'total_plays': total_plays,
        'total_users': total_users,
        'servers': servers,
        'days_of_history': days_of_history,
        'oldest_date': oldest_date,
        'newest_date': newest_date
    })


@main_bp.route('/api/viewing-history-posters')
def api_viewing_history_posters():
    """Get recent unique title posters for the viewing-history hero background."""
    try:
        service = AnalyticsService()
        posters = service.get_recent_unique_history_posters(limit=40)
        return jsonify({'posters': posters})
    except Exception as e:
        return jsonify({'error': str(e), 'posters': []}), 500


@main_bp.route('/users')
def users():
    """Display all users from configured servers."""
    # Check if servers are configured
    if not ConfigService.has_valid_config():
        flash('Please configure at least one server before viewing users.', 'error')
        return redirect(url_for('settings.index'))

    service = AnalyticsService()
    all_users = service.get_all_users()

    return render_template('users.html', users=all_users)


@main_bp.route('/media')
def media():
    """Display media library page with Movies and TV Shows."""
    if not ConfigService.has_valid_config():
        flash('Please configure at least one server before viewing media.', 'error')
        return redirect(url_for('settings.index'))

    service = MediaService()
    lifetime_service = MediaLifetimeStatsService()
    sync_status = service.get_sync_status()
    lifetime_sync_status = lifetime_service.get_sync_status()

    movies = []
    tv_shows = []
    total_movie_plays = 0
    total_tv_plays = 0
    if sync_status['has_data']:
        movies = service.get_movies()
        tv_shows = service.get_tv_shows()
        if lifetime_sync_status['has_data']:
            movies, tv_shows = lifetime_service.apply_cached_play_counts(movies, tv_shows)
        total_movie_plays = sum(movie.get('play_count', 0) for movie in movies)
        total_tv_plays = sum(show.get('play_count', 0) for show in tv_shows)

    return render_template('media.html',
                          sync_status=sync_status,
                          lifetime_sync_status=lifetime_sync_status,
                          movies=movies,
                          tv_shows=tv_shows,
                          total_movie_plays=total_movie_plays,
                          total_tv_plays=total_tv_plays)


@main_bp.route('/api/media-top-posters')
def api_media_top_posters():
    """Get top-played media posters for the media-page hero background."""
    try:
        service = AnalyticsService()
        posters = service.get_top_media_posters_by_play_count(limit=80)
        return jsonify({'posters': posters})
    except Exception as e:
        return jsonify({'error': str(e), 'posters': []}), 500


@main_bp.route('/api/dashboard-top-posters')
def api_dashboard_top_posters():
    """Get top-played media posters for the dashboard hero background."""
    try:
        service = AnalyticsService()
        posters = service.get_top_media_posters_by_play_count(limit=120)
        return jsonify({'posters': posters})
    except Exception as e:
        return jsonify({'error': str(e), 'posters': []}), 500


@main_bp.route('/api/media/start-load', methods=['POST'])
def api_media_start_load():
    """Start loading media library data."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    try:
        service = MediaService()
        started = service.start_media_load()
        if not started:
            return jsonify({'error': 'Media load already in progress.'}), 409
        return jsonify({'status': 'started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/media/status')
def api_media_status():
    """Get current media load status for polling."""
    try:
        service = MediaService()
        status = service.get_sync_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/media/lifetime-stats/start', methods=['POST'])
def api_media_lifetime_stats_start():
    """Start lifetime play-count sync across all configured servers."""
    if not ConfigService.has_valid_config():
        return jsonify({'error': 'No server configuration found.'}), 400

    try:
        service = MediaLifetimeStatsService()
        started = service.start_sync()
        if not started:
            return jsonify({'error': 'Lifetime stats sync already in progress.'}), 409
        return jsonify({'status': 'started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/media/lifetime-stats/status')
def api_media_lifetime_stats_status():
    """Get current lifetime play-count sync status for polling."""
    try:
        service = MediaLifetimeStatsService()
        status = service.get_sync_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/media/movies')
def api_media_movies():
    """Get movies data for DataTables."""
    try:
        service = MediaService()
        movies = service.get_movies()
        return jsonify({'data': movies})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@main_bp.route('/api/media/tv-shows')
def api_media_tv_shows():
    """Get TV shows data for DataTables."""
    try:
        service = MediaService()
        tv_shows = service.get_tv_shows()
        return jsonify({'data': tv_shows})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
