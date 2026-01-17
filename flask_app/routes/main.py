"""
Main application routes for dashboard and analytics execution.
"""
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify
from flask_app.models import db, AnalyticsRun
from flask_app.services.analytics_service import AnalyticsService
from flask_app.services.config_service import ConfigService

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Landing page showing last run status and quick actions."""
    last_run = AnalyticsRun.query.order_by(AnalyticsRun.started_at.desc()).first()
    servers = ConfigService.get_active_servers()
    has_config = len(servers) > 0

    return render_template('index.html',
                          last_run=last_run,
                          has_config=has_config,
                          servers=servers)


@main_bp.route('/run-analytics', methods=['POST'])
def run_analytics():
    """Execute analytics synchronously and redirect to dashboard."""
    try:
        # Validate configuration exists
        if not ConfigService.has_valid_config():
            flash('Please configure at least one server before running analytics.', 'error')
            return redirect(url_for('settings.index'))

        # Create run record
        run = AnalyticsRun(status='running')
        db.session.add(run)
        db.session.commit()

        # Execute analytics (synchronous - blocks for 30-60s)
        service = AnalyticsService()
        result = service.run_full_analytics(run.id)

        # Update run record with results
        run.status = 'success'
        run.completed_at = datetime.utcnow()
        run.total_plays = result['total_plays']
        run.total_users = result['total_users']
        run.summary_json = json.dumps(result['summary'])
        db.session.commit()

        flash('Analytics completed successfully!', 'success')
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
        return redirect(url_for('main.index'))


@main_bp.route('/dashboard')
def dashboard():
    """Display analytics dashboard with all charts."""
    last_run = AnalyticsRun.query.filter_by(status='success').order_by(
        AnalyticsRun.completed_at.desc()
    ).first()

    if not last_run:
        flash('No analytics data available. Please run analytics first.', 'info')
        return redirect(url_for('main.index'))

    # Convert completed_at from UTC to Pacific Time
    completed_at_pt = last_run.completed_at.replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/Los_Angeles'))

    # Load cached chart HTML and table data from service
    service = AnalyticsService()
    charts = service.get_cached_charts(last_run.id)
    table_data = service.get_cached_table_data(last_run.id)
    summary = json.loads(last_run.summary_json)

    # Get current streaming activity (real-time)
    current_activity = service.get_current_activity()

    return render_template('dashboard.html',
                          charts=charts,
                          table_data=table_data,
                          summary=summary,
                          last_run=last_run,
                          completed_at_pt=completed_at_pt,
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
