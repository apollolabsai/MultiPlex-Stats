"""
Service for syncing viewing history from Tautulli to local database.
Supports both full backfill and incremental sync.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from flask_app.models import db, ViewingHistory, HistorySyncStatus, ServerConfig
from flask_app.services.config_service import ConfigService
from multiplex_stats.api_client import TautulliClient
from multiplex_stats.timezone_utils import get_local_timezone


class HistorySyncService:
    """Service for managing viewing history sync operations."""

    PAGE_SIZE = 1000  # Records per API request

    def __init__(self):
        self.local_tz = get_local_timezone()

    def get_or_create_status(self) -> HistorySyncStatus:
        """Get or create the singleton sync status record."""
        status = HistorySyncStatus.query.first()
        if not status:
            status = HistorySyncStatus()
            db.session.add(status)
            db.session.commit()
        return status

    def get_sync_status(self) -> dict:
        """Get current sync status for polling."""
        status = self.get_or_create_status()
        last_sync_date = None
        if status.last_sync_date:
            last_sync_date = (
                status.last_sync_date.replace(tzinfo=timezone.utc)
                .astimezone(self.local_tz)
                .isoformat()
            )
        return {
            'status': status.status,
            'sync_type': status.sync_type,
            'records_fetched': status.records_fetched,
            'records_total': status.records_total,
            'records_inserted': status.records_inserted,
            'records_skipped': status.records_skipped,
            'current_server': status.current_server,
            'error_message': status.error_message,
            'last_sync_date': last_sync_date,
            'last_sync_record_count': status.last_sync_record_count,
            'total_records_in_db': ViewingHistory.query.count()
        }

    def start_backfill(self, days: int) -> bool:
        """
        Start a full backfill sync.

        Args:
            days: Number of days of history to fetch

        Returns:
            True if backfill started, False if already running
        """
        status = self.get_or_create_status()

        if status.status == 'running':
            return False

        # Reset status for new backfill
        status.status = 'running'
        status.sync_type = 'backfill'
        status.started_at = datetime.utcnow()
        status.completed_at = None
        status.records_fetched = 0
        status.records_total = None
        status.records_inserted = 0
        status.records_skipped = 0
        status.current_server = None
        status.error_message = None
        db.session.commit()

        # Truncate existing history for fresh start
        ViewingHistory.query.delete()
        db.session.commit()

        # Calculate after date
        after_date = datetime.now(self.local_tz) - timedelta(days=days)
        after_str = after_date.strftime("%Y-%m-%d")

        try:
            self._run_sync(after_str, is_backfill=True)
            status.status = 'success'
            status.completed_at = datetime.utcnow()
            status.last_sync_date = datetime.utcnow()
            status.last_sync_record_count = ViewingHistory.query.count()
        except Exception as e:
            status.status = 'failed'
            status.completed_at = datetime.utcnow()
            status.error_message = str(e)

        db.session.commit()
        return True

    def start_incremental_sync(self) -> bool:
        """
        Start an incremental sync (fetch only new records).

        Returns:
            True if sync started, False if already running or no existing data
        """
        status = self.get_or_create_status()

        if status.status == 'running':
            return False

        # Get the most recent record date to sync from
        latest_record = ViewingHistory.query.order_by(ViewingHistory.started.desc()).first()
        if not latest_record:
            # No existing data, can't do incremental - need backfill first
            return False

        # Use the most recent record's date as the after parameter
        # Subtract 1 day to ensure we don't miss any records due to timezone differences
        latest_date = (
            datetime.fromtimestamp(latest_record.started, tz=timezone.utc)
            .astimezone(self.local_tz)
            - timedelta(days=1)
        )
        after_str = latest_date.strftime("%Y-%m-%d")

        # Reset status for incremental sync
        status.status = 'running'
        status.sync_type = 'incremental'
        status.started_at = datetime.utcnow()
        status.completed_at = None
        status.records_fetched = 0
        status.records_total = None
        status.records_inserted = 0
        status.records_skipped = 0
        status.current_server = None
        status.error_message = None
        db.session.commit()

        try:
            self._run_sync(after_str, is_backfill=False)
            status.status = 'success'
            status.completed_at = datetime.utcnow()
            status.last_sync_date = datetime.utcnow()
            status.last_sync_record_count = ViewingHistory.query.count()
        except Exception as e:
            status.status = 'failed'
            status.completed_at = datetime.utcnow()
            status.error_message = str(e)

        db.session.commit()
        return True

    def _run_sync(self, after_date: str, is_backfill: bool = False):
        """
        Run the actual sync operation.

        Args:
            after_date: Date string in YYYY-MM-DD format
            is_backfill: Whether this is a full backfill (affects progress reporting)
        """
        status = self.get_or_create_status()
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            raise ValueError("No server configuration found")

        # Process Server A
        status.current_server = server_a_config.name
        db.session.commit()
        self._sync_server(server_a_config, after_date, server_order=0)

        # Process Server B if configured
        if server_b_config:
            status.current_server = server_b_config.name
            db.session.commit()
            self._sync_server(server_b_config, after_date, server_order=1)

        status.current_server = None
        db.session.commit()

    def _sync_server(self, server_config, after_date: str, server_order: int):
        """
        Sync history from a single server.

        Args:
            server_config: Server configuration object
            after_date: Date string in YYYY-MM-DD format
            server_order: 0 for ServerA, 1 for ServerB
        """
        status = self.get_or_create_status()
        client = TautulliClient(server_config)

        start = 0
        total_records = None

        while True:
            # Fetch page of records
            response = client.get_history_paginated(
                start=start,
                length=self.PAGE_SIZE,
                after=after_date
            )

            if not response or 'response' not in response:
                raise ValueError(f"Invalid API response from {server_config.name}")

            data = response['response'].get('data', {})
            records = data.get('data', [])

            # Get total on first request
            if total_records is None:
                total_records = data.get('recordsFiltered', 0)
                # Update status with estimated total (accumulate across servers)
                if status.records_total is None:
                    status.records_total = total_records
                else:
                    status.records_total += total_records
                db.session.commit()

            if not records:
                break

            # Process and insert records
            for record in records:
                self._insert_record(record, server_config.name, server_order)

            # Update progress
            status.records_fetched += len(records)
            db.session.commit()

            # Check if we've fetched all records
            start += len(records)
            if start >= total_records:
                break

    def _insert_record(self, record: dict, server_name: str, server_order: int):
        """
        Insert a single history record, handling duplicates.

        Args:
            record: History record from Tautulli API
            server_name: Name of the server
            server_order: 0 for ServerA, 1 for ServerB
        """
        status = self.get_or_create_status()
        row_id = record.get('row_id')

        if not row_id:
            return

        # Check for existing record (deduplication)
        existing = ViewingHistory.query.filter_by(row_id=row_id).first()
        if existing:
            status.records_skipped += 1
            return

        # Calculate PT date/time from started timestamp
        started_ts = record.get('started')
        date_played = None
        time_played = None
        if started_ts:
            try:
                local_dt = datetime.fromtimestamp(started_ts, tz=timezone.utc).astimezone(self.local_tz)
                date_played = local_dt.date()
                time_played = local_dt.strftime('%-I:%M%p').lower()
            except (ValueError, TypeError):
                pass

        # Create new record
        history_record = ViewingHistory(
            row_id=row_id,
            server_name=server_name,
            server_order=server_order,
            user_id=record.get('user_id'),
            user=record.get('user'),
            media_type=record.get('media_type'),
            full_title=record.get('full_title'),
            title=record.get('title'),
            grandparent_title=record.get('grandparent_title'),
            parent_media_index=record.get('parent_media_index'),
            media_index=record.get('media_index'),
            year=record.get('year'),
            rating_key=record.get('rating_key'),
            parent_rating_key=record.get('parent_rating_key'),
            grandparent_rating_key=record.get('grandparent_rating_key'),
            thumb=record.get('thumb'),
            started=started_ts,
            stopped=record.get('stopped'),
            duration=record.get('duration'),
            play_duration=record.get('play_duration'),
            percent_complete=record.get('percent_complete'),
            watched_status=record.get('watched_status'),
            ip_address=record.get('ip_address'),
            platform=record.get('platform'),
            product=record.get('product'),
            player=record.get('player'),
            quality_profile=record.get('quality_profile'),
            transcode_decision=record.get('transcode_decision'),
            location=record.get('location'),
            geo_city=record.get('geo_city'),
            geo_region=record.get('geo_region'),
            geo_country=record.get('geo_country'),
            date_played=date_played,
            time_played=time_played
        )

        db.session.add(history_record)
        status.records_inserted += 1

        # Commit in batches for performance
        if status.records_inserted % 100 == 0:
            db.session.commit()

    def has_history_data(self) -> bool:
        """Check if there's any history data in the database."""
        return ViewingHistory.query.count() > 0

    def get_history_stats(self) -> dict:
        """Get statistics about the stored history."""
        count = ViewingHistory.query.count()
        if count == 0:
            return {
                'total_records': 0,
                'oldest_date': None,
                'newest_date': None,
                'unique_users': 0
            }

        oldest = ViewingHistory.query.order_by(ViewingHistory.started.asc()).first()
        newest = ViewingHistory.query.order_by(ViewingHistory.started.desc()).first()
        unique_users = db.session.query(ViewingHistory.user).distinct().count()

        oldest_date = None
        newest_date = None
        if oldest and oldest.started:
            oldest_date = datetime.fromtimestamp(oldest.started, tz=timezone.utc).astimezone(self.local_tz).strftime('%Y-%m-%d')
        if newest and newest.started:
            newest_date = datetime.fromtimestamp(newest.started, tz=timezone.utc).astimezone(self.local_tz).strftime('%Y-%m-%d')

        return {
            'total_records': count,
            'oldest_date': oldest_date,
            'newest_date': newest_date,
            'unique_users': unique_users
        }
