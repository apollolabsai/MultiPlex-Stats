"""
Logged HTTP session for outbound API calls.
"""
import logging
import re
import time

import requests

from flask_app.services.log_service import status_text

logger = logging.getLogger('multiplex.http')

# Strip API keys / tokens from URLs before logging
_SENSITIVE_PARAMS = re.compile(
    r'(apikey|api_key|token|key|secret)=[^&]+',
    re.IGNORECASE,
)


def _sanitize_url(url):
    """Replace sensitive query-string values with '***'."""
    return _SENSITIVE_PARAMS.sub(r'\1=***', url)


class LoggedSession(requests.Session):
    """requests.Session subclass that logs every outbound HTTP call."""

    def request(self, method, url, **kwargs):
        safe_url = _sanitize_url(str(url))
        start = time.monotonic()
        try:
            resp = super().request(method, url, **kwargs)
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.info(
                'OUT %s %s -> %s (%.0fms)',
                method.upper(), safe_url, status_text(resp.status_code), elapsed_ms,
            )
            return resp
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.error(
                'OUT %s %s -> FAILED (%.0fms): %s',
                method.upper(), safe_url, elapsed_ms, exc,
            )
            raise


logged_session = LoggedSession()
