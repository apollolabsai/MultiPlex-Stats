"""
Logs blueprint — live application log viewer.
"""
import os

from flask import Blueprint, Response, jsonify, render_template, request, send_file

from flask_app.services.log_service import get_logs, stream_logs

logs_bp = Blueprint('logs', __name__)


@logs_bp.route('/')
def index():
    """Render the logs viewer page."""
    return render_template('logs.html')


@logs_bp.route('/stream')
def stream():
    """SSE endpoint for real-time log streaming."""
    min_level = request.args.get('level', 'DEBUG').upper()
    return Response(
        stream_logs(min_level=min_level),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        },
    )


@logs_bp.route('/api')
def api():
    """JSON endpoint returning recent log entries (for initial page load)."""
    min_level = request.args.get('level', 'DEBUG').upper()
    since_id = request.args.get('since_id', 0, type=int)
    limit = request.args.get('limit', 500, type=int)
    entries = get_logs(min_level=min_level, since_id=since_id, limit=limit)
    return jsonify(entries)


@logs_bp.route('/download')
def download():
    """Download the current log file."""
    from flask import current_app
    log_path = os.path.join(current_app.instance_path, 'logs', 'multiplex_stats.log')
    if not os.path.exists(log_path):
        return 'No log file found.', 404
    return send_file(
        log_path,
        mimetype='text/plain',
        as_attachment=True,
        download_name='multiplex_stats.log',
    )
