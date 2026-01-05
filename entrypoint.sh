#!/bin/bash

# Default PUID and PGID to 1000 if not set
PUID=${PUID:-1000}
PGID=${PGID:-1000}

echo "Starting MultiPlex Stats with UID:GID = $PUID:$PGID"

# Check if group exists, if not create it
if ! getent group $PGID > /dev/null 2>&1; then
    groupadd -g $PGID appgroup
else
    GROUP_NAME=$(getent group $PGID | cut -d: -f1)
    echo "Using existing group: $GROUP_NAME (GID: $PGID)"
fi

# Check if user exists, if not create it
if ! getent passwd $PUID > /dev/null 2>&1; then
    useradd -u $PUID -g $PGID -d /app -s /bin/bash appuser
else
    USER_NAME=$(getent passwd $PUID | cut -d: -f1)
    echo "Using existing user: $USER_NAME (UID: $PUID)"
fi

# Ensure instance directory exists and has correct permissions
mkdir -p /app/instance/cache
chown -R $PUID:$PGID /app/instance

# Execute the application as the specified user with proper PATH
exec gosu $PUID:$PGID /bin/bash -c "export PATH=/root/.local/bin:\$PATH && export PYTHONPATH=/app && python3 run_multiplex_stats.py"
