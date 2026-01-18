"""
Timezone utilities shared across the app.
"""

import os
from zoneinfo import ZoneInfo


def get_local_timezone() -> ZoneInfo:
    """Return the configured timezone (TZ env) or default to America/Los_Angeles."""
    tz_name = os.environ.get('TZ', 'America/Los_Angeles')
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo('America/Los_Angeles')
