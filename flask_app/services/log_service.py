"""
Application logging service with in-memory ring buffer and SSE streaming.
"""
import itertools
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------------
# Human-readable HTTP status text
# ---------------------------------------------------------------------------
HTTP_STATUS_TEXT = {
    200: '200 OK',
    201: '201 Created',
    204: '204 No Content',
    301: '301 Moved Permanently',
    302: '302 Found',
    304: '304 Not Modified',
    400: '400 Bad Request',
    401: '401 Unauthorized',
    403: '403 Forbidden',
    404: '404 Not Found',
    405: '405 Method Not Allowed',
    408: '408 Request Timeout',
    429: '429 Too Many Requests',
    500: '500 Internal Server Error',
    502: '502 Bad Gateway',
    503: '503 Service Unavailable',
    504: '504 Gateway Timeout',
}


def status_text(code):
    """Return human-readable status string for an HTTP status code."""
    return HTTP_STATUS_TEXT.get(code, str(code))


# ---------------------------------------------------------------------------
# In-memory ring buffer
# ---------------------------------------------------------------------------
_BUFFER_MAX = 2000
_log_buffer = deque(maxlen=_BUFFER_MAX)
_log_id_counter = itertools.count(1)
_sse_condition = threading.Condition()

_LEVELS_ORDERED = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
_LEVEL_VALUES = {name: i for i, name in enumerate(_LEVELS_ORDERED)}


def _passes_level(entry_level, min_level):
    return _LEVEL_VALUES.get(entry_level, 0) >= _LEVEL_VALUES.get(min_level, 0)


def get_logs(min_level='DEBUG', since_id=0, limit=500):
    """Return log entries from the ring buffer, filtered by level and cursor."""
    result = []
    for entry in _log_buffer:
        if entry['id'] <= since_id:
            continue
        if not _passes_level(entry['level'], min_level):
            continue
        result.append(entry)
        if len(result) >= limit:
            break
    return result


def stream_logs(min_level='DEBUG'):
    """Generator yielding SSE-formatted log entries. Blocks until new entries arrive."""
    last_id = 0
    if _log_buffer:
        last_id = _log_buffer[-1]['id']

    while True:
        with _sse_condition:
            _sse_condition.wait(timeout=15)

        new_entries = get_logs(min_level=min_level, since_id=last_id, limit=100)
        if new_entries:
            last_id = new_entries[-1]['id']
            for entry in new_entries:
                yield f"data: {json.dumps(entry)}\n\n"
        else:
            # Keepalive comment to prevent connection timeout
            yield ": keepalive\n\n"


# ---------------------------------------------------------------------------
# Custom logging handler
# ---------------------------------------------------------------------------
class BufferedLogHandler(logging.Handler):
    """Logging handler that appends records to the in-memory ring buffer."""

    def emit(self, record):
        try:
            entry = {
                'id': next(_log_id_counter),
                'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'level': record.levelname,
                'logger': record.name,
                'message': self.format(record),
            }
            _log_buffer.append(entry)
            with _sse_condition:
                _sse_condition.notify_all()
        except Exception:
            self.handleError(record)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(app):
    """Configure application-wide logging with file and buffer handlers."""
    log_dir = os.path.join(app.instance_path, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)-8s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Rotating file handler
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, 'multiplex_stats.log'),
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # In-memory buffer handler (message only — logger name is a separate field)
    buffer_handler = BufferedLogHandler()
    buffer_handler.setFormatter(logging.Formatter('%(message)s'))
    buffer_handler.setLevel(logging.DEBUG)

    # Application logger (all app code should use loggers under 'multiplex')
    app_logger = logging.getLogger('multiplex')
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(file_handler)
    app_logger.addHandler(buffer_handler)

    # Capture the existing multiplex_stats logger (TautulliClient, etc.)
    lib_logger = logging.getLogger('multiplex_stats')
    lib_logger.setLevel(logging.DEBUG)
    lib_logger.addHandler(file_handler)
    lib_logger.addHandler(buffer_handler)

    # Wire Flask's app.logger to our handlers
    app.logger.handlers.clear()
    app.logger.addHandler(file_handler)
    app.logger.addHandler(buffer_handler)
    app.logger.setLevel(logging.DEBUG)

    app_logger.info('Logging initialised (buffer=%d, file=%s)', _BUFFER_MAX, log_dir)
