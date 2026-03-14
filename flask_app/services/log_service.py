"""
Application logging service with in-memory ring buffer and SSE streaming.
"""
import itertools
import json
import logging
import os
import re
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


def get_logs(min_level='DEBUG', since_id=0, limit=2000):
    """Return log entries from the ring buffer, filtered by level and cursor."""
    if limit <= 0:
        return []

    if since_id <= 0:
        # Initial page loads should see the newest matching entries, not the
        # oldest slice of the buffer.
        recent_entries = deque(maxlen=limit)
        for entry in _log_buffer:
            if not _passes_level(entry['level'], min_level):
                continue
            recent_entries.append(entry)
        return list(recent_entries)

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

# Matches lines written by the file formatter:
# 2026-03-14 07:41:37 [INFO    ] multiplex.requests: IN  GET /health -> 204 No Content
_LOG_LINE_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\s*\] (.+)$')


# Messages to drop when seeding the buffer from old log files.
# These match the paths suppressed in the live after_request hook,
# plus the old health-check pattern (GET / -> 302) from before
# the /health endpoint existed.
_SEED_SKIP_PREFIXES = (
    'IN  GET /health ',
    'IN  GET / -> 302',
    'IN  GET /logs/stream ',
    'IN  GET /logs/api ',
)


def _is_suppressed_message(message):
    """Return True if this log message should be filtered from the seed buffer."""
    return any(message.startswith(p) for p in _SEED_SKIP_PREFIXES)


def _seed_buffer_from_file(log_dir):
    """Pre-populate the ring buffer from the persisted log file after a restart."""
    log_file = os.path.join(log_dir, 'multiplex_stats.log')

    # Collect files: oldest backup first, current last
    candidates = []
    for i in range(3, 0, -1):
        backup = f"{log_file}.{i}"
        if os.path.exists(backup):
            candidates.append(backup)
    if os.path.exists(log_file):
        candidates.append(log_file)

    if not candidates:
        return

    # Use a temporary deque to keep only the last _BUFFER_MAX parsed lines
    parsed = deque(maxlen=_BUFFER_MAX)
    for path in candidates:
        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    m = _LOG_LINE_RE.match(line.rstrip('\n'))
                    if not m:
                        continue
                    rest = m.group(3)
                    sep = rest.find(': ')
                    logger_name = rest[:sep] if sep != -1 else ''
                    message = rest[sep + 2:] if sep != -1 else rest
                    # Skip health-check noise (matches live suppression rules)
                    if _is_suppressed_message(message):
                        continue
                    parsed.append({
                        'timestamp': m.group(1),
                        'level': m.group(2).strip(),
                        'logger': logger_name,
                        'message': message,
                    })
        except OSError:
            continue

    for entry in parsed:
        entry['id'] = next(_log_id_counter)
        _log_buffer.append(entry)


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

    _seed_buffer_from_file(log_dir)
    app_logger.info('Logging initialised (buffer=%d, file=%s)', _BUFFER_MAX, log_dir)
