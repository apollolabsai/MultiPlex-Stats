"""
Background sync service for lifetime media play counts.

This reads local ViewingHistory rows and aggregates by normalized title
(and year for movies) so plays are merged across multiple rating keys.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import current_app

from flask_app.models import db, LifetimeMediaPlayCount, LifetimeStatsSyncStatus, ViewingHistory
from flask_app.services.config_service import ConfigService
from multiplex_stats.timezone_utils import get_local_timezone


class MediaLifetimeStatsService:
    """Service for managing lifetime media play count sync operations."""

    PROGRESS_UPDATE_INTERVAL = 500
    _scheduler_lock = threading.Lock()
    _scheduler_started = False
    _status_write_lock = threading.Lock()

    def __init__(self):
        self.local_tz = get_local_timezone()

    def get_or_create_status(self) -> LifetimeStatsSyncStatus:
        """Get or create the singleton lifetime stats sync status record."""
        status = LifetimeStatsSyncStatus.query.first()
        if not status:
            status = LifetimeStatsSyncStatus()
            db.session.add(status)
            db.session.commit()
        return status

    def has_lifetime_stats(self) -> bool:
        """Check if cached lifetime play counts exist."""
        return LifetimeMediaPlayCount.query.count() > 0

    def get_sync_status(self) -> dict[str, Any]:
        """Get current lifetime stats sync status for polling."""
        status = self.get_or_create_status()
        last_sync_date = None
        if status.last_sync_date:
            last_sync_date = (
                status.last_sync_date.replace(tzinfo=timezone.utc)
                .astimezone(self.local_tz)
                .isoformat()
            )

        servers = []
        if status.server_a_name:
            servers.append({
                'name': status.server_a_name,
                'status': status.server_a_status or 'idle',
                'step': status.server_a_step or '',
                'fetched': status.server_a_fetched or 0,
                'total': status.server_a_total,
                'error': status.server_a_error,
            })
        if status.server_b_name:
            servers.append({
                'name': status.server_b_name,
                'status': status.server_b_status or 'idle',
                'step': status.server_b_step or '',
                'fetched': status.server_b_fetched or 0,
                'total': status.server_b_total,
                'error': status.server_b_error,
            })

        return {
            'status': status.status,
            'current_step': status.current_step,
            'records_fetched': status.records_fetched,
            'records_total': status.records_total,
            'error_message': status.error_message,
            'last_sync_date': last_sync_date,
            'has_data': self.has_lifetime_stats(),
            'servers': servers,
        }

    def start_sync(self, app=None, trigger: str = 'manual') -> bool:
        """
        Start lifetime stats sync in a background thread.

        Returns True when started, False when a sync is already running.
        """
        status = self.get_or_create_status()
        if status.status == 'running':
            return False

        server_a_config, server_b_config = ConfigService.get_server_configs()
        if not server_a_config:
            raise ValueError("No server configuration found")

        status.status = 'running'
        status.started_at = datetime.utcnow()
        status.completed_at = None
        status.current_step = f'Initializing local history scan ({trigger})...'
        status.records_fetched = 0
        status.records_total = None
        status.error_message = None

        status.server_a_name = server_a_config.name
        status.server_a_status = 'pending'
        status.server_a_step = ''
        status.server_a_fetched = 0
        status.server_a_total = None
        status.server_a_error = None

        status.server_b_name = server_b_config.name if server_b_config else None
        status.server_b_status = 'pending' if server_b_config else 'idle'
        status.server_b_step = ''
        status.server_b_fetched = 0
        status.server_b_total = None
        status.server_b_error = None
        db.session.commit()

        if app is None:
            app = current_app._get_current_object()

        thread = threading.Thread(target=self._run_sync_thread, args=(app,))
        thread.daemon = True
        thread.start()
        return True

    def _run_sync_thread(self, app):
        """Run lifetime stats sync with app context in background."""
        with app.app_context():
            try:
                self._run_sync_parallel(app)
                status = self.get_or_create_status()
                status.status = 'success'
                status.completed_at = datetime.utcnow()
                status.current_step = 'Complete'
                status.last_sync_date = datetime.utcnow()
                if status.records_total is None:
                    status.records_total = status.records_fetched
            except Exception as e:
                status = self.get_or_create_status()
                status.status = 'failed'
                status.completed_at = datetime.utcnow()
                status.error_message = str(e)
            db.session.commit()

    def _run_sync_parallel(self, app):
        """Scan local history from configured servers in parallel and cache totals."""
        server_a_config, server_b_config = ConfigService.get_server_configs()
        if not server_a_config:
            raise ValueError("No server configuration found")

        counts_by_key: dict[tuple[str, str, int | None], int] = {}
        counts_lock = threading.Lock()
        successful_servers: list[str] = []
        errors: list[str] = []

        def fetch_server(server_config, server_key: str):
            with app.app_context():
                try:
                    self._set_server_status(server_key, 'running', 'Scanning local history...')
                    server_counts = self._scan_server_history(server_config.name, server_key)
                    with counts_lock:
                        for key, value in server_counts.items():
                            counts_by_key[key] = counts_by_key.get(key, 0) + value
                    successful_servers.append(server_key)
                    self._set_server_status(server_key, 'success', 'Complete')
                except Exception as exc:
                    errors.append(f"{server_config.name}: {exc}")
                    self._set_server_status(server_key, 'failed', 'Failed', str(exc))

        threads = []
        thread_a = threading.Thread(target=fetch_server, args=(server_a_config, 'a'))
        thread_a.start()
        threads.append(thread_a)

        if server_b_config:
            thread_b = threading.Thread(target=fetch_server, args=(server_b_config, 'b'))
            thread_b.start()
            threads.append(thread_b)

        for thread in threads:
            thread.join()

        if not successful_servers:
            raise ValueError("Unable to scan local history for lifetime play counts.")

        status = self.get_or_create_status()
        status.current_step = 'Saving aggregated lifetime play counts...'
        if errors:
            status.error_message = "Partial warnings: " + " | ".join(errors)
        db.session.commit()

        self._store_counts(counts_by_key)

    def _scan_server_history(self, server_name: str, server_key: str) -> dict[tuple[str, str, int | None], int]:
        """Scan all local history rows for one server and aggregate play counts by content key."""
        counts: dict[tuple[str, str, int | None], int] = {}
        query = ViewingHistory.query.filter(ViewingHistory.server_name == server_name)
        total_rows = query.count()
        self._update_server_total(server_key, total_rows)
        if total_rows == 0:
            self._update_server_fetched(server_key, 0)
            return counts

        processed = 0
        rows = (
            query.with_entities(
                ViewingHistory.media_type,
                ViewingHistory.title,
                ViewingHistory.full_title,
                ViewingHistory.grandparent_title,
                ViewingHistory.year,
            )
            .yield_per(2000)
        )

        for media_type, title, full_title, grandparent_title, year in rows:
            key = self._extract_content_key(
                {
                    'media_type': media_type,
                    'title': title,
                    'full_title': full_title,
                    'grandparent_title': grandparent_title,
                    'year': year,
                }
            )
            if key is not None:
                counts[key] = counts.get(key, 0) + 1

            processed += 1
            if processed % self.PROGRESS_UPDATE_INTERVAL == 0 or processed == total_rows:
                self._update_server_fetched(server_key, processed)

        return counts

    def _store_counts(self, counts_by_key: dict[tuple[str, str, int | None], int]) -> None:
        """Replace cached lifetime play counts with the latest aggregated values."""
        now_utc = datetime.utcnow()
        LifetimeMediaPlayCount.query.delete()

        for (media_type, title_normalized, year), total_plays in counts_by_key.items():
            db.session.add(LifetimeMediaPlayCount(
                media_type=media_type,
                title_normalized=title_normalized,
                year=year,
                total_plays=total_plays,
                updated_at=now_utc,
            ))

        db.session.commit()

    def apply_cached_play_counts(self, movies: list[dict], tv_shows: list[dict]) -> tuple[list[dict], list[dict]]:
        """
        Apply cached lifetime play counts to media rows in-memory.
        Falls back to existing cached_media play_count when no lifetime match exists.
        """
        if not movies and not tv_shows:
            return movies, tv_shows

        movie_titles = {
            self._normalize_title(item.get('content_title'))
            for item in movies
            if self._normalize_title(item.get('content_title'))
        }
        show_titles = {
            self._normalize_title(item.get('content_title'))
            for item in tv_shows
            if self._normalize_title(item.get('content_title'))
        }

        movie_map_exact: dict[tuple[str, int | None], int] = {}
        movie_variants: dict[str, list[tuple[int | None, int]]] = {}
        if movie_titles:
            movie_rows = (
                LifetimeMediaPlayCount.query
                .filter_by(media_type='movie')
                .filter(LifetimeMediaPlayCount.title_normalized.in_(movie_titles))
                .all()
            )
            for row in movie_rows:
                key = (row.title_normalized, row.year)
                movie_map_exact[key] = row.total_plays
                movie_variants.setdefault(row.title_normalized, []).append((row.year, row.total_plays))

        show_map: dict[str, int] = {}
        if show_titles:
            show_rows = (
                LifetimeMediaPlayCount.query
                .filter_by(media_type='show')
                .filter(LifetimeMediaPlayCount.title_normalized.in_(show_titles))
                .all()
            )
            for row in show_rows:
                show_map[row.title_normalized] = row.total_plays

        for movie in movies:
            title = self._normalize_title(movie.get('content_title'))
            if not title:
                continue

            year = self._to_int(movie.get('content_year'))
            plays = movie_map_exact.get((title, year))
            if plays is None:
                variants = movie_variants.get(title, [])
                if year is None and variants:
                    plays = sum(value for _, value in variants)
                elif len(variants) == 1:
                    plays = variants[0][1]

            if plays is not None:
                movie['play_count'] = plays

        for show in tv_shows:
            title = self._normalize_title(show.get('content_title'))
            if not title:
                continue
            plays = show_map.get(title)
            if plays is not None:
                show['play_count'] = plays

        return movies, tv_shows

    @classmethod
    def start_daily_scheduler(cls, app) -> None:
        """Start a daemon thread that triggers lifetime stats refresh daily at 1:00 AM local time."""
        with cls._scheduler_lock:
            if cls._scheduler_started:
                return
            cls._scheduler_started = True

        thread = threading.Thread(target=cls._scheduler_loop, args=(app,), daemon=True)
        thread.start()

    @classmethod
    def _scheduler_loop(cls, app) -> None:
        """Scheduler loop for daily auto-refresh at 1:00 AM local time."""
        while True:
            sleep_seconds = cls._seconds_until_next_run(hour=1, minute=0)
            time.sleep(max(1, int(sleep_seconds)))

            with app.app_context():
                try:
                    if ConfigService.has_valid_config():
                        MediaLifetimeStatsService().start_sync(app=app, trigger='scheduled')
                except Exception as exc:
                    print(f"Lifetime stats scheduled sync failed: {exc}")

            time.sleep(1)

    @staticmethod
    def _seconds_until_next_run(hour: int, minute: int = 0) -> float:
        tz = get_local_timezone()
        now = datetime.now(tz)
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        return (next_run - now).total_seconds()

    def _set_server_status(self, server_key: str, status_value: str, step: str, error: str | None = None) -> None:
        with self._status_write_lock:
            status = self.get_or_create_status()
            if server_key == 'a':
                status.server_a_status = status_value
                status.server_a_step = step
                status.server_a_error = error
            else:
                status.server_b_status = status_value
                status.server_b_step = step
                status.server_b_error = error
            db.session.commit()

    def _update_server_total(self, server_key: str, total: int) -> None:
        with self._status_write_lock:
            status = self.get_or_create_status()
            if server_key == 'a':
                status.server_a_total = total
            else:
                status.server_b_total = total
            status.records_total = (status.server_a_total or 0) + (status.server_b_total or 0)
            db.session.commit()

    def _update_server_fetched(self, server_key: str, fetched: int) -> None:
        with self._status_write_lock:
            status = self.get_or_create_status()
            if server_key == 'a':
                status.server_a_fetched = fetched
            else:
                status.server_b_fetched = fetched
            status.records_fetched = (status.server_a_fetched or 0) + (status.server_b_fetched or 0)
            db.session.commit()

    @staticmethod
    def _extract_content_key(row: dict[str, Any]) -> tuple[str, str, int | None] | None:
        """Normalize a history row into a lifetime cache key."""
        media_type = MediaLifetimeStatsService._normalize_title(row.get('media_type'))
        if media_type == 'movie':
            title = MediaLifetimeStatsService._normalize_title(row.get('title') or row.get('full_title'))
            if not title:
                return None
            year = MediaLifetimeStatsService._to_int(row.get('year'))
            return ('movie', title, year)

        if media_type in {'episode', 'tv', 'show'}:
            show_title = MediaLifetimeStatsService._normalize_title(row.get('grandparent_title'))
            if not show_title:
                return None
            return ('show', show_title, None)

        return None

    @staticmethod
    def _normalize_title(value: Any) -> str:
        if not value:
            return ''
        return ' '.join(str(value).strip().lower().split())

    @staticmethod
    def _to_int(value: Any) -> int | None:
        try:
            if value in (None, ''):
                return None
            return int(value)
        except (TypeError, ValueError):
            return None
