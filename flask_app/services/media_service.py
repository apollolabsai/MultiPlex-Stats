"""
Service for syncing media library info from Tautulli to local database.
Uses export_metadata API for rich metadata including ratings.
Supports parallel fetching from multiple servers.
"""
import logging
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import PurePosixPath, PureWindowsPath
from typing import Optional

from flask import current_app

import re

logger = logging.getLogger('multiplex.media')

from flask_app.models import (
    db,
    MediaSyncStatus,
    CachedMedia,
    MediaRating,
    MediaTechnicalCache,
)
from flask_app.services.config_service import ConfigService
from flask_app.services.sync_progress import SyncProgressTracker
from flask_app.services.sync_recovery import is_recoverable_stale_run
from multiplex_stats.api_client import TautulliClient
from multiplex_stats.timezone_utils import get_local_timezone


class MediaService:
    """Service for managing media library sync operations."""

    EXPORT_POLL_INTERVAL = 2   # seconds between export status checks
    EXPORT_TIMEOUT = 1500      # max seconds to wait for export (25 minutes)
    EXPORT_MAX_ATTEMPTS = 3    # initial attempt + 2 retries
    EXPORT_RETRY_DELAY = 5     # seconds between export retries
    TECHNICAL_LOOKUP_WORKERS = 2
    TECHNICAL_LOOKUP_LIMIT_PER_SERVER = 300
    TECHNICAL_LOOKUP_BUDGET_SECONDS = 45
    RUN_MODE_MEDIA_ONLY = 'media_only'
    RUN_MODE_FULL_PIPELINE = 'full_pipeline'
    STAGE_NAME = 'media'
    _progress_tracker = SyncProgressTracker()
    MOVIE_EXPORT_CUSTOM_FIELDS = [
        'title',
        'year',
        'addedAt',
        'rating',
        'ratingImage',
        'audienceRating',
        'audienceRatingImage',
        'guid',
        'guids',
        'updatedAt',
        'media.parts.file',
    ]
    TV_EXPORT_CUSTOM_FIELDS = [
        'title',
        'addedAt',
        'rating',
        'audienceRating',
        'audienceRatingImage',
        'guid',
        'guids',
        'seasons.episodes.media.parts.size',
        'seasons.episodes.media.parts.sizeHuman',
        'seasons.episodes.media.parts.file',
    ]

    def __init__(self):
        self.local_tz = get_local_timezone()

    @classmethod
    def _step_id(cls, server_key: str | None, step_name: str) -> str:
        prefix = f'{cls.STAGE_NAME}-{server_key}' if server_key else cls.STAGE_NAME
        return f'{prefix}-{step_name}'

    @classmethod
    def _build_progress_steps(cls, server_a_config, server_b_config) -> list[dict]:
        steps = []
        for server_key, server_config in (('a', server_a_config), ('b', server_b_config)):
            if not server_config:
                continue
            server_label = server_config.name
            steps.extend([
                {
                    'id': cls._step_id(server_key, 'discover'),
                    'label': f'{server_label}: Discover libraries',
                    'stage': cls.STAGE_NAME,
                    'server_key': server_key,
                    'server_name': server_label,
                },
                {
                    'id': cls._step_id(server_key, 'movie-export'),
                    'label': f'{server_label}: Export movie metadata',
                    'stage': cls.STAGE_NAME,
                    'server_key': server_key,
                    'server_name': server_label,
                    'unit': 'items',
                },
                {
                    'id': cls._step_id(server_key, 'tv-export'),
                    'label': f'{server_label}: Export TV metadata, sizes, seasons, and episodes',
                    'stage': cls.STAGE_NAME,
                    'server_key': server_key,
                    'server_name': server_label,
                    'unit': 'items',
                },
                {
                    'id': cls._step_id(server_key, 'play-stats'),
                    'label': f'{server_label}: Fetch play stats',
                    'stage': cls.STAGE_NAME,
                    'server_key': server_key,
                    'server_name': server_label,
                    'unit': 'libraries',
                },
            ])

        steps.append({
            'id': cls._step_id(None, 'save'),
            'label': 'Save merged media library data',
            'stage': cls.STAGE_NAME,
        })
        steps.append({
            'id': cls._step_id(None, 'mdblist'),
            'label': 'Fetch MDBList ratings',
            'stage': cls.STAGE_NAME,
            'unit': 'items',
        })
        steps.append({
            'id': cls._step_id(None, 'finalize'),
            'label': 'Finalize media refresh',
            'stage': cls.STAGE_NAME,
        })
        return steps

    @staticmethod
    def _export_step_label(media_type: str, section_name: str, action: str) -> str:
        """Build explicit progress text for export-driven sync steps."""
        if media_type == 'show':
            return f'{action} TV metadata export for {section_name} (size/seasons/episodes)...'
        return f'{action} export for {section_name}...'

    @staticmethod
    def _client_server_name(client: TautulliClient) -> str:
        """Return the configured server name for log messages."""
        return getattr(getattr(client, 'config', None), 'name', None) or 'Tautulli'

    @staticmethod
    def _export_progress_detail(
        section_name: str,
        exported_items: int,
        total_items: int,
        elapsed: int,
    ) -> str:
        """Build explicit export progress text with item counts and elapsed time."""
        if total_items > 0:
            return f'Current library {section_name}: {exported_items:,} / {total_items:,} items ({elapsed}s)'
        return f'Current library {section_name}: waiting for item counts ({elapsed}s)'

    def get_or_create_status(self) -> MediaSyncStatus:
        """Get or create the singleton sync status record."""
        status = MediaSyncStatus.query.first()
        if not status:
            status = MediaSyncStatus()
            db.session.add(status)
            db.session.commit()
        return self._recover_stale_status(status)

    def _recover_stale_status(self, status: MediaSyncStatus) -> MediaSyncStatus:
        """Mark stale running rows failed after a process restart."""
        progress_items = self._progress_tracker.snapshot()
        if not is_recoverable_stale_run(status.status, status.started_at, progress_items):
            return status

        logger.warning(
            'Recovering stale media sync left running from %s.',
            status.started_at,
        )
        status.status = 'failed'
        status.completed_at = datetime.utcnow()
        status.current_step = 'Recovered stale media refresh after process restart.'
        status.error_message = (
            'Recovered stale media refresh after process restart. '
            'Start a new media refresh to rebuild the cache.'
        )

        if status.server_a_status == 'running':
            status.server_a_status = 'failed'
            status.server_a_step = 'Recovered stale media refresh after process restart.'
            status.server_a_error = 'Recovered stale media refresh after process restart.'
        if status.server_b_status == 'running':
            status.server_b_status = 'failed'
            status.server_b_step = 'Recovered stale media refresh after process restart.'
            status.server_b_error = 'Recovered stale media refresh after process restart.'

        self._progress_tracker.reset([])
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

        # Build per-server status
        servers = []

        # Server A
        if status.server_a_name:
            servers.append({
                'name': status.server_a_name,
                'status': status.server_a_status or 'idle',
                'step': status.server_a_step or '',
                'fetched': status.server_a_fetched or 0,
                'total': status.server_a_total,
                'error': status.server_a_error,
            })

        # Server B
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
            'run_mode': status.run_mode or self.RUN_MODE_MEDIA_ONLY,
            'current_step': status.current_step,
            'records_fetched': status.records_fetched,
            'records_total': status.records_total,
            'movies_count': status.movies_count,
            'tv_shows_count': status.tv_shows_count,
            'error_message': status.error_message,
            'mdblist_warning': status.mdblist_warning,
            'last_sync_date': last_sync_date,
            'has_data': self.has_media_data(),
            'servers': servers,
            'pipeline_items': self._progress_tracker.snapshot(),
        }

    def has_media_data(self) -> bool:
        """Check if there's any media data in the database."""
        return CachedMedia.query.count() > 0

    def start_media_load(self, app=None, run_mode: str = RUN_MODE_MEDIA_ONLY) -> bool:
        """
        Start loading media library data in background thread.

        Args:
            app: Flask application instance for app context in thread

        Returns:
            True if load started, False if already running
        """
        status = self.get_or_create_status()

        if status.status == 'running':
            return False

        # Get server configs to populate names
        server_a_config, server_b_config = ConfigService.get_server_configs()
        include_mdblist = bool(
            ConfigService.get_effective_mdblist_api_key(
                current_app.config.get('MDBLIST_API_KEY', '')
            )
        )

        # Reset status for new load
        status.status = 'running'
        status.started_at = datetime.utcnow()
        status.completed_at = None
        status.current_step = 'Initializing...'
        status.run_mode = run_mode
        status.records_fetched = 0
        status.records_total = None
        status.movies_count = 0
        status.tv_shows_count = 0
        status.error_message = None
        status.mdblist_warning = None

        # Reset per-server status
        status.server_a_name = server_a_config.name if server_a_config else None
        status.server_a_status = 'pending' if server_a_config else 'idle'
        status.server_a_step = None
        status.server_a_fetched = 0
        status.server_a_total = None
        status.server_a_error = None

        status.server_b_name = server_b_config.name if server_b_config else None
        status.server_b_status = 'pending' if server_b_config else 'idle'
        status.server_b_step = None
        status.server_b_fetched = 0
        status.server_b_total = None
        status.server_b_error = None

        db.session.commit()
        self._progress_tracker.reset(self._build_progress_steps(server_a_config, server_b_config))
        if not include_mdblist:
            self._progress_tracker.update(
                self._step_id(None, 'mdblist'),
                status='skipped',
                detail='MDBList API key not configured',
            )

        # Run sync in background thread
        if app is None:
            app = current_app._get_current_object()

        thread = threading.Thread(target=self._run_media_sync_thread, args=(app,))
        thread.daemon = True
        thread.start()

        return True

    def _run_media_sync_thread(self, app):
        """Run media sync in a background thread with app context."""
        with app.app_context():
            try:
                self._run_media_sync_parallel(app)

                # Enrich with MDBList ratings if API key is configured.
                self._run_mdblist_enrichment(app)

                self._progress_tracker.start(self._step_id(None, 'finalize'), detail='Wrapping up media refresh...')

                status = self.get_or_create_status()
                status.status = 'success'
                status.completed_at = datetime.utcnow()
                status.last_sync_date = datetime.utcnow()
                status.current_step = 'Complete'
                status.movies_count = CachedMedia.query.filter_by(media_type='movie').count()
                status.tv_shows_count = CachedMedia.query.filter_by(media_type='show').count()
                self._progress_tracker.complete(
                    self._step_id(None, 'finalize'),
                    detail=(
                        f"Stored {status.movies_count or 0:,} movies and "
                        f"{status.tv_shows_count or 0:,} TV shows"
                    ),
                )
            except Exception as e:
                status = self.get_or_create_status()
                status.status = 'failed'
                status.completed_at = datetime.utcnow()
                status.error_message = str(e)
                status.movies_count = CachedMedia.query.filter_by(media_type='movie').count()
                status.tv_shows_count = CachedMedia.query.filter_by(media_type='show').count()
                self._progress_tracker.fail_first_running_for_server('a', stage=self.STAGE_NAME, error=str(e))
                self._progress_tracker.fail_first_running_for_server('b', stage=self.STAGE_NAME, error=str(e))
                for step_name in ('save', 'mdblist', 'finalize'):
                    step = self._progress_tracker.get_step(self._step_id(None, step_name))
                    if step and step.get('status') == 'running':
                        self._progress_tracker.fail(
                            self._step_id(None, step_name),
                            detail=str(e),
                            error=str(e),
                        )

            db.session.commit()

    def _run_mdblist_enrichment(self, app):
        """Fetch MDBList ratings for all media items that have an IMDb ID."""
        from flask import current_app
        from flask_app.services.mdblist_service import MDBListService

        api_key = ConfigService.get_effective_mdblist_api_key(
            current_app.config.get('MDBLIST_API_KEY', '')
        )
        if not api_key:
            return

        status = self.get_or_create_status()
        status.current_step = 'Fetching MDBList ratings...'
        db.session.commit()
        self._progress_tracker.start(
            self._step_id(None, 'mdblist'),
            detail='Fetching MDBList ratings...',
            current=0,
        )

        def _progress(fetched, total):
            s = self.get_or_create_status()
            s.current_step = f'Fetching MDBList ratings ({fetched}/{total})...'
            db.session.commit()
            self._progress_tracker.update(
                self._step_id(None, 'mdblist'),
                status='running',
                detail=f'{fetched:,} / {total:,} items',
                current=fetched,
                total=total,
            )

        enrichment = MDBListService(api_key).enrich_media_ratings(progress_callback=_progress)

        if enrichment['failed_batches'] > 0:
            warning = (
                f"MDBList: {enrichment['failed_batches']} API batch(es) failed out of "
                f"{enrichment['total']} items. Check server logs for details. "
                f"{enrichment['ratings_stored']} rating(s) stored."
            )
            s = self.get_or_create_status()
            s.mdblist_warning = warning
            db.session.commit()
            logger.warning("MDBList: %s", warning)

        self._progress_tracker.complete(
            self._step_id(None, 'mdblist'),
            detail=f"{enrichment['ratings_stored']:,} ratings stored",
            current=enrichment.get('total', 0),
            total=enrichment.get('total', 0),
        )

    def _run_media_sync_parallel(self, app):
        """Run the actual media sync operation with parallel server fetching."""
        status = self.get_or_create_status()
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            raise ValueError("No server configuration found")

        # Shared storage for aggregation (thread-safe via locks)
        movies_data = {}
        tv_data = {}
        data_lock = threading.Lock()
        seen_technical_locators: set[tuple[int, str, str, str]] = set()
        technical_locator_lock = threading.Lock()

        # Track errors from threads
        errors = []
        errors_lock = threading.Lock()

        def fetch_server(server_config, is_primary: bool, server_key: str):
            """Fetch media from a single server (runs in thread)."""
            with app.app_context():
                try:
                    self._fetch_server_media_parallel(
                        server_config, movies_data, tv_data, data_lock,
                        is_primary, server_key,
                        seen_technical_locators,
                        technical_locator_lock,
                    )
                except Exception as e:
                    with errors_lock:
                        errors.append((server_key, str(e)))
                    logger.exception(
                        'Media refresh failed on %s; continuing with remaining pipeline steps.',
                        server_config.name,
                    )
                    self._progress_tracker.fail_first_running_for_server(
                        server_key,
                        stage=self.STAGE_NAME,
                        error=str(e),
                    )
                    # Update server status to failed
                    status = self.get_or_create_status()
                    failed_step = str(e)
                    if server_key == 'a':
                        status.server_a_status = 'failed'
                        status.server_a_step = failed_step
                        status.server_a_error = str(e)
                    else:
                        status.server_b_status = 'failed'
                        status.server_b_step = failed_step
                        status.server_b_error = str(e)
                    db.session.commit()

        # Start parallel fetching
        status.current_step = 'Fetching from servers...'
        db.session.commit()

        threads = []

        # Server A thread (primary)
        thread_a = threading.Thread(
            target=fetch_server,
            args=(server_a_config, True, 'a')
        )
        thread_a.start()
        threads.append(thread_a)

        # Server B thread (secondary) - if configured
        if server_b_config:
            thread_b = threading.Thread(
                target=fetch_server,
                args=(server_b_config, False, 'b')
            )
            thread_b.start()
            threads.append(thread_b)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Do not replace cached media with a partial dataset.
        if errors:
            error_summary = '; '.join(
                f"Server {server_key.upper()} failed: {error_msg}"
                for server_key, error_msg in errors
            )
            raise ValueError(
                "Media refresh incomplete; keeping previous cached media. "
                f"{error_summary}"
            )

        # Save aggregated data to database
        status = self.get_or_create_status()
        status.current_step = 'Saving media data...'
        db.session.commit()
        self._progress_tracker.start(
            self._step_id(None, 'save'),
            detail='Saving merged media library data...',
        )
        self._save_aggregated_media(movies_data, tv_data)
        self._prune_unseen_technical_cache(
            seen_technical_locators,
            [
                config.server_config_id
                for config in (server_a_config, server_b_config)
                if config and config.server_config_id is not None
            ],
        )
        self._progress_tracker.complete(
            self._step_id(None, 'save'),
            detail=f'Saved {len(movies_data):,} movies and {len(tv_data):,} TV shows',
        )

        status.current_step = 'Finalizing...'
        db.session.commit()

    def _fetch_server_media_parallel(
        self,
        server_config,
        movies_data: dict,
        tv_data: dict,
        data_lock: threading.Lock,
        is_primary: bool,
        server_key: str,
        seen_technical_locators: set[tuple[int, str, str, str]] | None = None,
        technical_locator_lock: Optional[object] = None,
    ):
        """
        Fetch media from a single server using export_metadata API.
        Thread-safe version that updates per-server status.

        Args:
            server_config: Server configuration object
            movies_data: Shared dict to aggregate movie data
            tv_data: Shared dict to aggregate TV data
            data_lock: Lock for thread-safe access to shared dicts
            is_primary: If True, this server's metadata takes priority
            server_key: 'a' or 'b' for status updates
        """
        status = self.get_or_create_status()
        discover_step_id = self._step_id(server_key, 'discover')
        movie_export_step_id = self._step_id(server_key, 'movie-export')
        tv_export_step_id = self._step_id(server_key, 'tv-export')
        play_stats_step_id = self._step_id(server_key, 'play-stats')

        # Update server status to running
        if server_key == 'a':
            status.server_a_status = 'running'
            status.server_a_step = 'Connecting...'
        else:
            status.server_b_status = 'running'
            status.server_b_step = 'Connecting...'
        db.session.commit()
        self._progress_tracker.start(discover_step_id, detail='Connecting to Tautulli...')

        client = TautulliClient(server_config)
        server_identifier = self._get_server_identifier(client, server_config)
        enrichment_state = {
            'started_at': None,
            'requests_used': 0,
            'section_cache_hits': 0,
            'local_cache_hits': 0,
            'fingerprint_mismatches': 0,
            'targeted_successes': 0,
            'targeted_failures': 0,
            'budget_skips': 0,
        }
        movie_play_stats: dict[str, list] = {}

        # Get all libraries
        libraries_response = client.get_libraries()
        if not libraries_response or 'response' not in libraries_response:
            raise ValueError(f"Failed to get libraries from {server_config.name}")

        libraries = libraries_response['response'].get('data', [])

        # Collect movie and TV libraries
        movie_libraries = []
        tv_libraries = []

        for lib in libraries:
            section_type = lib.get('section_type', '')
            section_id = lib.get('section_id')
            section_name = lib.get('section_name', f'Library {section_id}')
            count = int(lib.get('count', 0) or 0)
            if section_type == 'movie':
                movie_libraries.append({'id': section_id, 'name': section_name, 'count': count})
            elif section_type == 'show':
                tv_libraries.append({'id': section_id, 'name': section_name, 'count': count})

        # Update total for progress tracking
        server_total = sum(lib['count'] for lib in movie_libraries) + sum(lib['count'] for lib in tv_libraries)

        status = self.get_or_create_status()
        if server_key == 'a':
            status.server_a_total = server_total
        else:
            status.server_b_total = server_total
        db.session.commit()
        self._progress_tracker.complete(
            discover_step_id,
            detail=(
                f"{len(movie_libraries)} movie libraries, "
                f"{len(tv_libraries)} TV libraries"
            ),
        )

        # Process each library via export_metadata (for ratings)
        movie_total_items = sum(lib['count'] for lib in movie_libraries)
        completed_movie_items = 0
        if movie_libraries:
            self._progress_tracker.start(
                movie_export_step_id,
                detail=f'Queued {len(movie_libraries)} movie libraries',
                current=0,
                total=movie_total_items,
            )
        else:
            self._progress_tracker.complete(movie_export_step_id, detail='No movie libraries found')

        for lib in movie_libraries:
            play_stats_items = self._fetch_library_via_export_parallel(
                client, server_config.name, lib['id'], lib['name'],
                'movie', movies_data, tv_data, data_lock, is_primary, server_key,
                movie_export_step_id,
                completed_movie_items,
                movie_total_items,
                lib['count'],
                server_config=server_config,
                server_identifier=server_identifier,
                enrichment_state=enrichment_state,
                seen_technical_locators=seen_technical_locators,
                technical_locator_lock=technical_locator_lock,
            )
            movie_play_stats[str(lib['id'])] = play_stats_items or []
            completed_movie_items += lib['count']

        if movie_libraries:
            self._progress_tracker.complete(
                movie_export_step_id,
                detail=f'Exported {len(movie_libraries)} movie libraries',
                current=movie_total_items,
                total=movie_total_items,
            )

        tv_total_items = sum(lib['count'] for lib in tv_libraries)
        completed_tv_items = 0
        if tv_libraries:
            self._progress_tracker.start(
                tv_export_step_id,
                detail=f'Queued {len(tv_libraries)} TV libraries',
                current=0,
                total=tv_total_items,
            )
        else:
            self._progress_tracker.complete(tv_export_step_id, detail='No TV libraries found')

        for lib in tv_libraries:
            self._fetch_library_via_export_parallel(
                client, server_config.name, lib['id'], lib['name'],
                'show', movies_data, tv_data, data_lock, is_primary, server_key,
                tv_export_step_id,
                completed_tv_items,
                tv_total_items,
                lib['count'],
            )
            completed_tv_items += lib['count']

        if tv_libraries:
            self._progress_tracker.complete(
                tv_export_step_id,
                detail=f'Exported {len(tv_libraries)} TV libraries',
                current=tv_total_items,
                total=tv_total_items,
            )

        # Fetch play stats from get_library_media_info
        status = self.get_or_create_status()
        if server_key == 'a':
            status.server_a_step = 'Fetching play stats...'
        else:
            status.server_b_step = 'Fetching play stats...'
        db.session.commit()
        total_play_stats_libraries = len(movie_libraries) + len(tv_libraries)
        if total_play_stats_libraries > 0:
            self._progress_tracker.start(
                play_stats_step_id,
                detail='Fetching play stats...',
                current=0,
                total=total_play_stats_libraries,
            )
        else:
            self._progress_tracker.complete(play_stats_step_id, detail='No libraries for play stats')

        play_stats_completed = 0
        for lib in movie_libraries:
            self._fetch_library_play_stats_parallel(
                client,
                lib['id'],
                'movie',
                movies_data,
                data_lock,
                items=movie_play_stats.get(str(lib['id'])),
                include_movie_technical=False,
            )
            play_stats_completed += 1
            self._progress_tracker.update(
                play_stats_step_id,
                status='running',
                detail=f'Processed {play_stats_completed:,} / {total_play_stats_libraries:,} libraries',
                current=play_stats_completed,
                total=total_play_stats_libraries,
            )

        for lib in tv_libraries:
            self._fetch_library_play_stats_parallel(
                client, lib['id'], 'show', tv_data, data_lock
            )
            play_stats_completed += 1
            self._progress_tracker.update(
                play_stats_step_id,
                status='running',
                detail=f'Processed {play_stats_completed:,} / {total_play_stats_libraries:,} libraries',
                current=play_stats_completed,
                total=total_play_stats_libraries,
            )

        if total_play_stats_libraries > 0:
            self._progress_tracker.complete(
                play_stats_step_id,
                detail=f'Fetched play stats for {total_play_stats_libraries:,} libraries',
                current=total_play_stats_libraries,
                total=total_play_stats_libraries,
            )

        logger.info(
            'Technical enrichment on %s: section_cache_hits=%s local_cache_hits=%s '
            'fingerprint_mismatches=%s targeted_requests=%s targeted_successes=%s '
            'targeted_failures=%s budget_skips=%s',
            server_config.name,
            enrichment_state['section_cache_hits'],
            enrichment_state['local_cache_hits'],
            enrichment_state['fingerprint_mismatches'],
            enrichment_state['requests_used'],
            enrichment_state['targeted_successes'],
            enrichment_state['targeted_failures'],
            enrichment_state['budget_skips'],
        )

        # Mark server as complete
        status = self.get_or_create_status()
        if server_key == 'a':
            status.server_a_status = 'success'
            status.server_a_step = 'Complete'
        else:
            status.server_b_status = 'success'
            status.server_b_step = 'Complete'
        db.session.commit()

    def _fetch_library_via_export_parallel(
        self,
        client: TautulliClient,
        server_name: str,
        section_id: int,
        section_name: str,
        media_type: str,
        movies_data: dict,
        tv_data: dict,
        data_lock: threading.Lock,
        is_primary: bool,
        server_key: str,
        progress_step_id: str,
        completed_step_items: int,
        total_step_items: int,
        library_item_count: int,
        server_config=None,
        server_identifier: str | None = None,
        enrichment_state: dict | None = None,
        seen_technical_locators: set[tuple[int, str, str, str]] | None = None,
        technical_locator_lock: Optional[object] = None,
    ):
        """
        Fetch library metadata using export_metadata API (parallel version).
        """
        start_label = self._export_step_label(media_type, section_name, 'Starting')
        last_error: Exception | None = None

        for attempt in range(1, self.EXPORT_MAX_ATTEMPTS + 1):
            try:
                export_data = self._start_and_wait_for_library_export(
                    client=client,
                    server_name=server_name,
                    section_id=section_id,
                    section_name=section_name,
                    media_type=media_type,
                    server_key=server_key,
                    progress_step_id=progress_step_id,
                    completed_step_items=completed_step_items,
                    total_step_items=total_step_items,
                    library_item_count=library_item_count,
                    start_label=start_label,
                    attempt=attempt,
                    mark_failed=attempt >= self.EXPORT_MAX_ATTEMPTS,
                )

                play_stats_items = None
                if media_type == 'movie' and server_config is not None:
                    play_stats_items = self._get_library_media_info_items(
                        client,
                        section_id,
                    )
                    self._enrich_movie_export_records(
                        client=client,
                        server_config=server_config,
                        server_identifier=server_identifier,
                        section_id=section_id,
                        export_data=export_data,
                        section_cache_items=play_stats_items,
                        enrichment_state=enrichment_state,
                        seen_technical_locators=seen_technical_locators,
                        technical_locator_lock=technical_locator_lock,
                    )

                data_dict = movies_data if media_type == 'movie' else tv_data
                self._process_export_data_parallel(
                    export_data, media_type, data_dict, data_lock, is_primary, server_key,
                    server_name=server_name,
                )
                if attempt > 1:
                    logger.info(
                        'Export retry succeeded on %s for section %s (attempt %s/%s).',
                        server_name,
                        section_name,
                        attempt,
                        self.EXPORT_MAX_ATTEMPTS,
                    )
                return play_stats_items
            except Exception as exc:
                last_error = exc
                if attempt >= self.EXPORT_MAX_ATTEMPTS:
                    failed_step = self._progress_tracker.get_step(progress_step_id)
                    if not failed_step or failed_step.get('status') != 'failed':
                        self._progress_tracker.fail(
                            progress_step_id,
                            detail=str(exc),
                            error=str(exc),
                        )
                    raise

                retry_detail = (
                    f'{section_name}: export attempt {attempt} failed; '
                    f'retrying {attempt + 1} / {self.EXPORT_MAX_ATTEMPTS}'
                )
                self._progress_tracker.start(
                    progress_step_id,
                    detail=retry_detail,
                    current=completed_step_items,
                    total=total_step_items,
                )
                status = self.get_or_create_status()
                if server_key == 'a':
                    status.server_a_step = retry_detail
                else:
                    status.server_b_step = retry_detail
                db.session.commit()
                logger.warning(
                    'Export attempt %s/%s failed on %s for section %s; retrying in %ss: %s',
                    attempt,
                    self.EXPORT_MAX_ATTEMPTS,
                    server_name,
                    section_name,
                    self.EXPORT_RETRY_DELAY,
                    exc,
                )
                time.sleep(self.EXPORT_RETRY_DELAY)

        if last_error:
            raise last_error

    def _start_and_wait_for_library_export(
        self,
        client: TautulliClient,
        server_name: str,
        section_id: int,
        section_name: str,
        media_type: str,
        server_key: str,
        progress_step_id: str,
        completed_step_items: int,
        total_step_items: int,
        library_item_count: int,
        start_label: str,
        attempt: int,
        mark_failed: bool,
    ) -> list:
        """Start a Tautulli export and wait for the finished export data."""
        status = self.get_or_create_status()
        attempt_suffix = (
            f' (attempt {attempt} / {self.EXPORT_MAX_ATTEMPTS})'
            if self.EXPORT_MAX_ATTEMPTS > 1
            else ''
        )
        display_start_label = f'{start_label}{attempt_suffix}'

        # Update server step
        if server_key == 'a':
            status.server_a_step = display_start_label
        else:
            status.server_b_step = display_start_label
        db.session.commit()
        logger.info(
            'Starting %s export on %s for section %s (attempt %s/%s)',
            media_type,
            server_name,
            section_name,
            attempt,
            self.EXPORT_MAX_ATTEMPTS,
        )

        if media_type == 'movie':
            custom_fields = self.MOVIE_EXPORT_CUSTOM_FIELDS
            metadata_level = 1  # Basic metadata for movies
            media_info_level = 0
        else:
            custom_fields = self.TV_EXPORT_CUSTOM_FIELDS
            metadata_level = 0
            media_info_level = 0

        export_response = client.export_metadata(
            section_id=section_id,
            file_format='json',
            metadata_level=metadata_level,
            media_info_level=media_info_level,
            thumb_level=0,
            art_level=0,
            custom_fields=custom_fields
        )

        if not export_response or 'response' not in export_response:
            raise ValueError(f"Failed to start export for {section_name}")

        response_data = export_response['response'].get('data', {})
        export_id = response_data.get('export_id')
        if not export_id:
            raise ValueError(f"No export_id returned for {section_name}")

        # Wait for export to complete
        export_data = self._wait_for_export_parallel(
            client,
            section_id,
            export_id,
            section_name,
            media_type,
            server_key,
            progress_step_id,
            completed_step_items,
            total_step_items,
            library_item_count,
            mark_failed=mark_failed,
        )

        return export_data

    def _wait_for_export_parallel(
        self,
        client: TautulliClient,
        section_id: int,
        export_id: int,
        section_name: str,
        media_type: str,
        server_key: str,
        progress_step_id: str,
        completed_step_items: int,
        total_step_items: int,
        library_item_count: int,
        mark_failed: bool = True,
    ) -> list:
        """
        Poll for export completion and download when ready (parallel version).
        """
        start_time = time.time()

        while True:
            elapsed = int(time.time() - start_time)

            if elapsed > self.EXPORT_TIMEOUT:
                timeout_detail = f'{section_name}: export timed out after {self.EXPORT_TIMEOUT}s'
                if mark_failed:
                    self._progress_tracker.fail(
                        progress_step_id,
                        detail=f'{section_name}: export timed out',
                        error=timeout_detail,
                    )
                status = self.get_or_create_status()
                if server_key == 'a':
                    status.server_a_step = timeout_detail
                else:
                    status.server_b_step = timeout_detail
                db.session.commit()
                logger.error(
                    'Export timed out on %s for section %s after %ss.',
                    self._client_server_name(client),
                    section_name,
                    self.EXPORT_TIMEOUT,
                )
                raise ValueError(f"Export timed out for {section_name} after {self.EXPORT_TIMEOUT}s")

            # Update server step with elapsed time
            status = self.get_or_create_status()
            wait_label = self._export_step_label(media_type, section_name, f'Waiting {elapsed}s for')
            if server_key == 'a':
                status.server_a_step = wait_label
            else:
                status.server_b_step = wait_label
            db.session.commit()

            # Check export status
            exports_response = client.get_exports_table(section_id)
            if exports_response and 'response' in exports_response:
                outer_data = exports_response['response'].get('data', {})
                exports = outer_data.get('data', []) if isinstance(outer_data, dict) else []
                for export in exports:
                    if export.get('export_id') == export_id:
                        complete_status = export.get('complete')
                        exported_items = int(export.get('exported_items', 0) or 0)
                        total_items = int(export.get('total_items', 0) or 0) or library_item_count
                        progress_detail = self._export_progress_detail(
                            section_name,
                            exported_items,
                            total_items,
                            elapsed,
                        )
                        aggregate_current = min(
                            completed_step_items + exported_items,
                            total_step_items or completed_step_items + total_items,
                        )
                        self._progress_tracker.update(
                            progress_step_id,
                            status='running',
                            detail=progress_detail,
                            current=aggregate_current,
                            total=total_step_items or completed_step_items + total_items,
                        )
                        if server_key == 'a':
                            status.server_a_step = progress_detail
                        else:
                            status.server_b_step = progress_detail
                        db.session.commit()

                        # complete=-1 means export failed on Tautulli's side
                        if complete_status == -1:
                            failure_detail = (
                                f'{section_name}: export failed at '
                                f'{exported_items:,} / {total_items:,} items'
                            )
                            if mark_failed:
                                self._progress_tracker.fail(
                                    progress_step_id,
                                    detail=failure_detail,
                                    error=failure_detail,
                                )
                            status = self.get_or_create_status()
                            if server_key == 'a':
                                status.server_a_step = failure_detail
                            else:
                                status.server_b_step = failure_detail
                            db.session.commit()
                            logger.error(
                                "Tautulli reported export failure on %s for section %s "
                                "(export_id=%s, processed %s/%s items).",
                                self._client_server_name(client),
                                section_name,
                                export_id,
                                exported_items,
                                total_items,
                            )
                            raise ValueError(
                                f"Export failed for {section_name} on Tautulli's side "
                                f"(processed {exported_items:,}/{total_items:,} items)"
                            )

                        if complete_status == 1:
                            # Download completed export
                            status = self.get_or_create_status()
                            download_label = self._export_step_label(media_type, section_name, 'Downloading')
                            if server_key == 'a':
                                status.server_a_step = download_label
                            else:
                                status.server_b_step = download_label
                            db.session.commit()
                            self._progress_tracker.update(
                                progress_step_id,
                                status='running',
                                detail=f'Downloading {section_name}...',
                                current=min(completed_step_items + total_items, total_step_items or completed_step_items + total_items),
                                total=total_step_items or completed_step_items + total_items,
                            )
                            logger.info(
                                'Downloading completed %s export on section %s (export_id=%s)',
                                media_type,
                                section_name,
                                export_id,
                            )

                            download_response = client.download_export(export_id)
                            if download_response:
                                if isinstance(download_response, dict) and 'response' in download_response:
                                    response_data = download_response['response']
                                    if isinstance(response_data, dict):
                                        export_data = response_data.get('data', [])
                                    else:
                                        export_data = response_data
                                    if isinstance(export_data, str):
                                        import json
                                        try:
                                            export_data = json.loads(export_data)
                                        except json.JSONDecodeError:
                                            export_data = []
                                    return export_data if isinstance(export_data, list) else []
                                elif isinstance(download_response, list):
                                    return download_response
                                return []
                            raise ValueError(f"Failed to download export for {section_name}")
                        break

            time.sleep(self.EXPORT_POLL_INTERVAL)

    @staticmethod
    def _normalize_identity_text(value) -> str:
        return ' '.join(str(value or '').casefold().split())

    @staticmethod
    def _normalize_year(value) -> int | None:
        try:
            return int(value) if value not in (None, '') else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_updated_at(value) -> str:
        return str(value or '').strip()

    def _get_server_identifier(self, client: TautulliClient, server_config) -> str:
        """Return Plex's machine identifier when Tautulli exposes it."""
        fallback = f"config:{getattr(server_config, 'server_config_id', None) or server_config.name}"
        try:
            response = client.get_server_info()
            data = response.get('response', {}).get('data', {}) if isinstance(response, dict) else {}
            if not isinstance(data, dict):
                return fallback
            for field in (
                'pms_identifier',
                'machine_identifier',
                'machineIdentifier',
                'pms_machine_identifier',
            ):
                value = str(data.get(field) or '').strip()
                if value:
                    return value
        except Exception as exc:
            logger.warning(
                'Unable to read Plex machine identifier from %s; using configuration scope: %s',
                server_config.name,
                exc,
            )
        return fallback

    @staticmethod
    def _get_library_media_info_items(
        client: TautulliClient,
        section_id: int,
    ) -> list:
        """Fetch the existing Tautulli section cache once for stats and technical seeding."""
        response = client.get_library_media_info(
            section_id=section_id,
            length=25000,
            refresh=False,
        )
        if not response or 'response' not in response:
            return []
        outer_data = response['response'].get('data', {})
        if isinstance(outer_data, dict):
            items = outer_data.get('data', [])
            return items if isinstance(items, list) else []
        return []

    def _cache_fingerprint_matches(
        self,
        cached: MediaTechnicalCache,
        record: dict,
        server_identifier: str | None,
    ) -> bool:
        """Validate a cached locator against current export identity fields."""
        if cached.server_identifier and server_identifier:
            if cached.server_identifier != server_identifier:
                return False

        current_guid = str(record.get('guid') or '').strip()
        cached_guid = str(cached.plex_guid or '').strip()
        if current_guid and cached_guid:
            return current_guid == cached_guid

        imdb_id, tmdb_id = self._parse_guids(record)
        strong_comparisons = []
        if imdb_id and cached.imdb_id:
            strong_comparisons.append(imdb_id == cached.imdb_id)
        if tmdb_id and cached.tmdb_id:
            strong_comparisons.append(tmdb_id == cached.tmdb_id)
        if strong_comparisons:
            return all(strong_comparisons)

        return (
            self._normalize_identity_text(record.get('title'))
            == self._normalize_identity_text(cached.title)
            and self._normalize_year(record.get('year')) == cached.year
        )

    def _section_cache_identity_matches(self, item: dict, record: dict) -> bool:
        """Use stale Tautulli rows only when their human identity still agrees."""
        return (
            self._normalize_identity_text(item.get('title'))
            == self._normalize_identity_text(record.get('title'))
            and self._normalize_year(item.get('year'))
            == self._normalize_year(record.get('year'))
        )

    @staticmethod
    def _technical_complete(technical: dict | None) -> bool:
        if not technical:
            return False
        return bool(
            int(technical.get('file_size', 0) or 0) > 0
            and technical.get('video_codecs')
            and technical.get('video_resolutions')
        )

    @staticmethod
    def _technical_from_section_cache(item: dict) -> dict:
        try:
            file_size = int(item.get('file_size', 0) or 0)
        except (TypeError, ValueError):
            file_size = 0
        codec = str(item.get('video_codec') or '').strip()
        resolution = str(item.get('video_resolution') or '').strip()
        return {
            'file_size': file_size,
            'file_sizes': {file_size} if file_size else set(),
            'video_codecs': {codec} if codec else set(),
            'video_resolutions': {resolution} if resolution else set(),
        }

    @staticmethod
    def _decode_cache_set(value: str | None) -> set:
        if not value:
            return set()
        try:
            parsed = json.loads(value)
            return {item for item in parsed if item not in (None, '')}
        except (TypeError, ValueError, json.JSONDecodeError):
            return set()

    def _technical_from_local_cache(self, cached: MediaTechnicalCache) -> dict:
        return {
            'file_size': int(cached.file_size or 0),
            'file_sizes': {
                int(value)
                for value in self._decode_cache_set(cached.file_size_versions)
                if str(value).isdigit() and int(value) > 0
            },
            'video_codecs': {
                str(value)
                for value in self._decode_cache_set(cached.video_codecs)
            },
            'video_resolutions': {
                str(value)
                for value in self._decode_cache_set(cached.video_resolutions)
            },
        }

    @staticmethod
    def _technical_from_metadata_response(response: dict) -> dict:
        data = response.get('response', {}).get('data', {}) if isinstance(response, dict) else {}
        if not isinstance(data, dict):
            return {}

        media_rows = data.get('media_info') or data.get('media') or []
        if not isinstance(media_rows, list):
            return {}

        file_sizes = set()
        video_codecs = set()
        video_resolutions = set()

        for media_row in media_rows:
            if not isinstance(media_row, dict):
                continue
            codec = str(
                media_row.get('video_codec')
                or media_row.get('videoCodec')
                or ''
            ).strip()
            resolution = str(
                media_row.get('video_resolution')
                or media_row.get('videoResolution')
                or ''
            ).strip()
            if codec:
                video_codecs.add(codec)
            if resolution:
                video_resolutions.add(resolution)

            version_size = 0
            parts = media_row.get('parts') or []
            if isinstance(parts, list):
                for part in parts:
                    if not isinstance(part, dict):
                        continue
                    try:
                        version_size += int(
                            part.get('file_size')
                            or part.get('size')
                            or 0
                        )
                    except (TypeError, ValueError):
                        continue

            if not version_size:
                try:
                    version_size = int(
                        media_row.get('file_size')
                        or media_row.get('size')
                        or 0
                    )
                except (TypeError, ValueError):
                    version_size = 0
            if version_size:
                file_sizes.add(version_size)

        return {
            'file_size': sum(file_sizes),
            'file_sizes': file_sizes,
            'video_codecs': video_codecs,
            'video_resolutions': video_resolutions,
        }

    @staticmethod
    def _apply_technical_to_record(record: dict, technical: dict) -> None:
        record['_technical_file_size'] = int(technical.get('file_size', 0) or 0)
        record['_technical_file_sizes'] = sorted(technical.get('file_sizes') or [])
        record['_technical_video_codecs'] = sorted(technical.get('video_codecs') or [])
        record['_technical_video_resolutions'] = sorted(
            technical.get('video_resolutions') or []
        )

    def _upsert_technical_cache(
        self,
        cached: MediaTechnicalCache | None,
        *,
        server_config,
        server_identifier: str | None,
        section_id: int,
        rating_key: str,
        record: dict,
        technical: dict,
    ) -> MediaTechnicalCache:
        imdb_id, tmdb_id = self._parse_guids(record)
        if cached is None:
            cached = MediaTechnicalCache(
                server_config_id=server_config.server_config_id,
                server_name=server_config.name,
                section_id=str(section_id),
                media_type='movie',
                rating_key=rating_key,
                title=str(record.get('title') or ''),
            )
            db.session.add(cached)

        cached.server_identifier = server_identifier
        cached.server_name = server_config.name
        cached.plex_guid = str(record.get('guid') or '').strip() or None
        cached.imdb_id = imdb_id
        cached.tmdb_id = tmdb_id
        cached.title = str(record.get('title') or '')
        cached.year = self._normalize_year(record.get('year'))
        cached.plex_updated_at = self._normalize_updated_at(record.get('updatedAt')) or None
        cached.file_size = int(technical.get('file_size', 0) or 0)
        cached.file_size_versions = json.dumps(
            sorted(int(value) for value in (technical.get('file_sizes') or []) if int(value) > 0)
        )
        cached.video_codecs = json.dumps(sorted(technical.get('video_codecs') or []))
        cached.video_resolutions = json.dumps(
            sorted(technical.get('video_resolutions') or [])
        )
        cached.refreshed_at = datetime.utcnow()
        return cached

    def _enrich_movie_export_records(
        self,
        *,
        client: TautulliClient,
        server_config,
        server_identifier: str | None,
        section_id: int,
        export_data: list,
        section_cache_items: list,
        enrichment_state: dict | None,
        seen_technical_locators: set[tuple[int, str, str, str]] | None,
        technical_locator_lock: Optional[object],
    ) -> None:
        """Attach technical values before title/year aggregation."""
        server_config_id = getattr(server_config, 'server_config_id', None)
        if enrichment_state is None:
            enrichment_state = {
                'started_at': None,
                'requests_used': 0,
                'section_cache_hits': 0,
                'local_cache_hits': 0,
                'fingerprint_mismatches': 0,
                'targeted_successes': 0,
                'targeted_failures': 0,
                'budget_skips': 0,
            }

        section_cache_by_key = {
            str(item.get('rating_key') or item.get('ratingKey') or ''): item
            for item in section_cache_items
            if isinstance(item, dict)
            and (item.get('rating_key') or item.get('ratingKey')) not in (None, '')
        }

        local_by_key = {}
        if server_config_id is not None:
            local_rows = MediaTechnicalCache.query.filter_by(
                server_config_id=server_config_id,
                section_id=str(section_id),
                media_type='movie',
            ).all()
            local_by_key = {row.rating_key: row for row in local_rows}

        targeted = []
        cache_changed = False

        for record in export_data:
            if not isinstance(record, dict):
                continue
            rating_key = str(
                record.get('ratingKey')
                or record.get('rating_key')
                or ''
            ).strip()
            if not rating_key:
                continue

            if server_config_id is not None and seen_technical_locators is not None:
                locator = (
                    server_config_id,
                    str(section_id),
                    'movie',
                    rating_key,
                )
                if technical_locator_lock:
                    with technical_locator_lock:
                        seen_technical_locators.add(locator)
                else:
                    seen_technical_locators.add(locator)

            cached = local_by_key.get(rating_key)
            cached_fingerprint_valid = False
            if cached is not None:
                cached_fingerprint_valid = self._cache_fingerprint_matches(
                    cached,
                    record,
                    server_identifier,
                )
                if cached_fingerprint_valid:
                    cached_technical = self._technical_from_local_cache(cached)
                    if self._technical_complete(cached_technical):
                        self._apply_technical_to_record(record, cached_technical)
                        export_updated_at = self._normalize_updated_at(
                            record.get('updatedAt')
                        )
                        if (
                            not export_updated_at
                            or export_updated_at == (cached.plex_updated_at or '')
                        ):
                            enrichment_state['local_cache_hits'] += 1
                            continue
                else:
                    enrichment_state['fingerprint_mismatches'] += 1

            section_item = section_cache_by_key.get(rating_key)
            if (
                cached is None
                and section_item is not None
                and self._section_cache_identity_matches(section_item, record)
            ):
                section_technical = self._technical_from_section_cache(section_item)
                if self._technical_complete(section_technical):
                    self._apply_technical_to_record(record, section_technical)
                    enrichment_state['section_cache_hits'] += 1
                    if server_config_id is not None:
                        local_by_key[rating_key] = self._upsert_technical_cache(
                            None,
                            server_config=server_config,
                            server_identifier=server_identifier,
                            section_id=section_id,
                            rating_key=rating_key,
                            record=record,
                            technical=section_technical,
                        )
                        cache_changed = True
                    continue

            targeted.append((record, rating_key, cached))

        remaining_limit = max(
            self.TECHNICAL_LOOKUP_LIMIT_PER_SERVER
            - int(enrichment_state['requests_used']),
            0,
        )
        if targeted and enrichment_state['started_at'] is None:
            enrichment_state['started_at'] = time.monotonic()
        candidates = targeted[:remaining_limit]
        enrichment_state['budget_skips'] += len(targeted) - len(candidates)

        processed_candidates = 0
        if candidates:
            with ThreadPoolExecutor(max_workers=self.TECHNICAL_LOOKUP_WORKERS) as executor:
                while processed_candidates < len(candidates):
                    elapsed = time.monotonic() - enrichment_state['started_at']
                    if elapsed >= self.TECHNICAL_LOOKUP_BUDGET_SECONDS:
                        break

                    batch = candidates[
                        processed_candidates:
                        processed_candidates + self.TECHNICAL_LOOKUP_WORKERS
                    ]
                    enrichment_state['requests_used'] += len(batch)
                    future_map = {
                        executor.submit(client.get_metadata, rating_key): (
                            record,
                            rating_key,
                            cached,
                        )
                        for record, rating_key, cached in batch
                    }
                    processed_candidates += len(batch)

                    for future in as_completed(future_map):
                        record, rating_key, cached = future_map[future]
                        try:
                            technical = self._technical_from_metadata_response(
                                future.result()
                            )
                        except Exception as exc:
                            enrichment_state['targeted_failures'] += 1
                            logger.warning(
                                'Targeted technical lookup failed on %s for section %s '
                                'ratingKey=%s: %s',
                                server_config.name,
                                section_id,
                                rating_key,
                                exc,
                            )
                            continue

                        if not self._technical_complete(technical):
                            enrichment_state['targeted_failures'] += 1
                            logger.warning(
                                'Targeted technical lookup returned incomplete data on %s '
                                'for section %s ratingKey=%s.',
                                server_config.name,
                                section_id,
                                rating_key,
                            )
                            continue

                        self._apply_technical_to_record(record, technical)
                        enrichment_state['targeted_successes'] += 1
                        if server_config_id is not None:
                            local_by_key[rating_key] = self._upsert_technical_cache(
                                cached,
                                server_config=server_config,
                                server_identifier=server_identifier,
                                section_id=section_id,
                                rating_key=rating_key,
                                record=record,
                                technical=technical,
                            )
                            cache_changed = True

        enrichment_state['budget_skips'] += len(candidates) - processed_candidates

        if cache_changed:
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                logger.exception(
                    'Unable to persist technical cache updates for %s section %s; '
                    'continuing with in-memory enrichment.',
                    server_config.name,
                    section_id,
                )

    @staticmethod
    def _prune_unseen_technical_cache(
        seen_locators: set[tuple[int, str, str, str]],
        active_server_config_ids: list[int],
    ) -> None:
        """Remove stale locators only after every server export succeeded."""
        seen_by_server = {}
        for locator in seen_locators:
            seen_by_server.setdefault(locator[0], set()).add(locator)

        removed = 0
        for server_config_id in active_server_config_ids:
            server_seen = seen_by_server.get(server_config_id)
            if not server_seen:
                continue
            rows = MediaTechnicalCache.query.filter_by(
                server_config_id=server_config_id,
                media_type='movie',
            ).all()
            for row in rows:
                locator = (
                    row.server_config_id,
                    row.section_id,
                    row.media_type,
                    row.rating_key,
                )
                if locator not in server_seen:
                    db.session.delete(row)
                    removed += 1

        if removed:
            db.session.commit()
            logger.info('Pruned %s unseen technical-cache locator(s).', removed)

    def _process_export_data_parallel(
        self,
        export_data: list,
        media_type: str,
        data_dict: dict,
        data_lock: threading.Lock,
        is_primary: bool,
        server_key: str,
        server_name: str | None = None,
    ):
        """
        Process export metadata and merge into data dict (thread-safe).
        """
        records_processed = 0

        for record in export_data:
            if not isinstance(record, dict):
                continue

            title = record.get('title', '')
            if not title:
                continue

            if media_type == 'movie':
                year = record.get('year')
                key = (title, year)
                season_count = 0
                episode_count = 0
                file_size = int(record.get('_technical_file_size', 0) or 0)
                record_file_sizes = {
                    int(value)
                    for value in (record.get('_technical_file_sizes') or [])
                    if int(value) > 0
                }
                record_video_codecs = {
                    str(value)
                    for value in (record.get('_technical_video_codecs') or [])
                    if value
                }
                record_video_resolutions = {
                    str(value)
                    for value in (record.get('_technical_video_resolutions') or [])
                    if value
                }
            else:
                key = title
                year = None
                season_count, episode_count, file_size = self._extract_show_counts_and_size(record)
                record_file_sizes = {file_size} if file_size else set()
                record_video_codecs = set()
                record_video_resolutions = set()

            record_file_paths = self._extract_file_path_labels(
                record,
                media_type=media_type,
                server_name=server_name,
                title=title,
            )
            media_info = record.get('media', [{}])[0] if record.get('media') else {}

            # Parse addedAt from ISO format to unix timestamp
            added_at_str = record.get('addedAt', '')
            added_at = 0
            if added_at_str:
                try:
                    dt = datetime.fromisoformat(added_at_str.replace('Z', '+00:00'))
                    added_at = int(dt.timestamp())
                except (ValueError, AttributeError):
                    pass

            play_count = 0
            last_played = 0

            video_codec = media_info.get('videoCodec', '') or ''
            video_resolution = media_info.get('videoResolution', '') or ''
            if video_codec:
                record_video_codecs.add(video_codec)
            if video_resolution:
                record_video_resolutions.add(video_resolution)

            rating = record.get('rating')
            rating_image = record.get('ratingImage')
            audience_rating = record.get('audienceRating')
            audience_rating_image = record.get('audienceRatingImage')

            imdb_id, tmdb_id = self._parse_guids(record)

            # Thread-safe update to shared dict
            with data_lock:
                if key in data_dict:
                    existing = data_dict[key]

                    if media_type == 'movie':
                        existing['file_size'] += file_size
                    else:
                        existing['file_size'] = max(existing['file_size'], file_size)
                    existing['play_count'] += play_count
                    existing['season_count'] = max(existing['season_count'], season_count)
                    existing['episode_count'] = max(existing['episode_count'], episode_count)

                    if added_at:
                        if existing['added_at']:
                            existing['added_at'] = min(existing['added_at'], added_at)
                        else:
                            existing['added_at'] = added_at

                    if last_played:
                        existing['last_played'] = max(existing['last_played'] or 0, last_played)

                    existing['video_codecs'].update(record_video_codecs)
                    existing['video_resolutions'].update(record_video_resolutions)
                    existing['file_sizes'].update(record_file_sizes)
                    existing['file_paths'].update(record_file_paths)

                    # Ratings: primary server takes priority
                    if is_primary:
                        if rating:
                            existing['rating'] = rating
                        if rating_image:
                            existing['rating_image'] = rating_image
                        if audience_rating:
                            existing['audience_rating'] = audience_rating
                        if audience_rating_image:
                            existing['audience_rating_image'] = audience_rating_image
                        if imdb_id:
                            existing['imdb_id'] = imdb_id
                        if tmdb_id:
                            existing['tmdb_id'] = tmdb_id
                    else:
                        if not existing.get('rating') and rating:
                            existing['rating'] = rating
                        if not existing.get('rating_image') and rating_image:
                            existing['rating_image'] = rating_image
                        if not existing.get('audience_rating') and audience_rating:
                            existing['audience_rating'] = audience_rating
                        if not existing.get('audience_rating_image') and audience_rating_image:
                            existing['audience_rating_image'] = audience_rating_image
                        if not existing.get('imdb_id') and imdb_id:
                            existing['imdb_id'] = imdb_id
                        if not existing.get('tmdb_id') and tmdb_id:
                            existing['tmdb_id'] = tmdb_id
                else:
                    data_dict[key] = {
                        'title': title,
                        'year': year,
                        'file_size': file_size,
                        'play_count': play_count,
                        'season_count': season_count,
                        'episode_count': episode_count,
                        'added_at': added_at,
                        'last_played': last_played,
                        'video_codecs': record_video_codecs,
                        'video_resolutions': record_video_resolutions,
                        'file_sizes': record_file_sizes,
                        'file_paths': record_file_paths,
                        'rating': rating,
                        'rating_image': rating_image,
                        'audience_rating': audience_rating,
                        'audience_rating_image': audience_rating_image,
                        'imdb_id': imdb_id,
                        'tmdb_id': tmdb_id,
                    }

            records_processed += 1

        # Update fetched count
        status = self.get_or_create_status()
        if server_key == 'a':
            status.server_a_fetched += records_processed
        else:
            status.server_b_fetched += records_processed
        status.records_fetched += records_processed
        db.session.commit()

    @staticmethod
    def _media_part_paths(media_rows: object) -> set[str]:
        """Return file paths from a Tautulli media/parts collection."""
        if not isinstance(media_rows, list):
            return set()

        paths = set()
        for media_row in media_rows:
            if not isinstance(media_row, dict):
                continue
            parts = media_row.get('parts') or []
            if not isinstance(parts, list):
                continue
            for part in parts:
                if not isinstance(part, dict):
                    continue
                file_path = str(
                    part.get('file')
                    or part.get('file_path')
                    or part.get('path')
                    or ''
                ).strip()
                if file_path:
                    paths.add(file_path)
        return paths

    @classmethod
    def _record_part_paths(cls, record: dict, media_type: str) -> set[str]:
        """Extract movie paths or nested TV episode paths from an export row."""
        if media_type == 'movie':
            return cls._media_part_paths(
                record.get('media') or record.get('media_info') or []
            )

        paths = set()
        seasons = record.get('seasons') or []
        if not isinstance(seasons, list):
            return paths
        for season in seasons:
            if not isinstance(season, dict):
                continue
            episodes = season.get('episodes') or []
            if not isinstance(episodes, list):
                continue
            for episode in episodes:
                if not isinstance(episode, dict):
                    continue
                paths.update(cls._media_part_paths(episode.get('media') or []))
        return paths

    @classmethod
    def _show_folder_path(cls, file_path: str, title: str) -> str:
        """Reduce an episode file path to its complete show-level directory."""
        path_type = PureWindowsPath if '\\' in file_path else PurePosixPath
        parsed = path_type(file_path)
        title_key = cls._normalize_identity_text(title)

        for candidate in (parsed.parent, *parsed.parents):
            name = candidate.name
            if not name:
                continue
            candidate_key = cls._normalize_identity_text(name)
            if title_key and candidate_key.startswith(title_key):
                return str(candidate)

        parent = parsed.parent
        if re.match(r'^(season\s*\d+|s\d{1,3}|specials?)$', parent.name, re.IGNORECASE):
            parent = parent.parent
        return str(parent) if parent.name else title

    @classmethod
    def _extract_file_path_labels(
        cls,
        record: dict,
        *,
        media_type: str,
        server_name: str | None,
        title: str,
    ) -> set[str]:
        """Format export paths for CSV-friendly multi-server aggregation."""
        raw_paths = cls._record_part_paths(record, media_type)
        if media_type == 'show':
            values = {
                cls._show_folder_path(file_path, title)
                for file_path in raw_paths
            }
        else:
            values = raw_paths

        prefix = str(server_name or '').strip()
        if prefix:
            return {f'{prefix} / {value}' for value in values if value}
        return {value for value in values if value}

    @staticmethod
    def _extract_show_counts_and_size(record: dict) -> tuple[int, int, int]:
        """Derive season count, episode count, and total size from a show export record."""
        def to_int(value) -> int | None:
            try:
                if value in (None, ''):
                    return None
                return int(value)
            except (TypeError, ValueError):
                return None

        season_count = (
            to_int(record.get('seasonCount'))
            or to_int(record.get('season_count'))
            or to_int(record.get('seasonsCount'))
            or to_int(record.get('childCount'))
            or to_int(record.get('childrenCount'))
            or to_int(record.get('children_count'))
            or to_int(record.get('child_count'))
            or 0
        )
        episode_count = (
            to_int(record.get('leafCount'))
            or to_int(record.get('leaf_count'))
            or to_int(record.get('grandchildCount'))
            or to_int(record.get('grandchildrenCount'))
            or to_int(record.get('grandchild_count'))
            or to_int(record.get('grandchildren_count'))
            or to_int(record.get('episodeCount'))
            or to_int(record.get('episode_count'))
            or to_int(record.get('episodesCount'))
            or 0
        )
        seasons = record.get('seasons') or []
        if not isinstance(seasons, list):
            return season_count, episode_count, 0

        nested_season_count = 0
        nested_episode_count = 0
        total_size = 0

        for season in seasons:
            if not isinstance(season, dict):
                continue
            nested_season_count += 1
            episodes = season.get('episodes') or []
            if not isinstance(episodes, list):
                continue

            for episode in episodes:
                if not isinstance(episode, dict):
                    continue
                nested_episode_count += 1
                media_rows = episode.get('media') or []
                if not isinstance(media_rows, list):
                    continue

                for media_row in media_rows:
                    if not isinstance(media_row, dict):
                        continue
                    parts = media_row.get('parts') or []
                    if not isinstance(parts, list):
                        continue

                    for part in parts:
                        if not isinstance(part, dict):
                            continue
                        try:
                            total_size += int(part.get('size', 0) or 0)
                        except (TypeError, ValueError):
                            continue

        if nested_season_count:
            season_count = max(season_count, nested_season_count)
        if nested_episode_count:
            episode_count = max(episode_count, nested_episode_count)
        return season_count, episode_count, total_size

    def _fetch_library_play_stats_parallel(
        self,
        client: TautulliClient,
        section_id: int,
        media_type: str,
        data_dict: dict,
        data_lock: threading.Lock,
        items: list | None = None,
        include_movie_technical: bool = True,
    ):
        """
        Fetch play stats from get_library_media_info and merge into data dict (thread-safe).
        """
        if items is None:
            items = self._get_library_media_info_items(client, section_id)

        for item in items:
            if not isinstance(item, dict):
                continue

            title = item.get('title', '')
            if not title:
                continue

            if media_type == 'movie':
                year_str = item.get('year', '')
                year = int(year_str) if year_str else None
                key = (title, year)
            else:
                key = title

            # Thread-safe update
            with data_lock:
                if key not in data_dict:
                    continue

                play_count = int(item.get('play_count', 0) or 0)
                last_played = int(item.get('last_played', 0) or 0)
                video_codec = item.get('video_codec', '') or ''
                video_resolution = item.get('video_resolution', '') or ''

                existing = data_dict[key]

                existing['play_count'] += play_count

                if media_type == 'movie' and include_movie_technical:
                    file_size = int(item.get('file_size', 0) or 0)
                    existing['file_size'] += file_size
                    if file_size:
                        existing['file_sizes'].add(file_size)

                if last_played:
                    existing['last_played'] = max(existing['last_played'] or 0, last_played)

                if video_codec and (media_type != 'movie' or include_movie_technical):
                    existing['video_codecs'].add(video_codec)
                if video_resolution and (media_type != 'movie' or include_movie_technical):
                    existing['video_resolutions'].add(video_resolution)

    @staticmethod
    def _parse_guids(record: dict):
        """
        Extract imdb_id and tmdb_id from a Tautulli export record.

        Supports both:
        - New Plex agent: guids = [{"id": "imdb://tt..."}, {"id": "tmdb://..."}]
        - Old Plex agent: guid = "com.plexapp.agents.imdb://tt...?lang=en"
        """
        imdb_id = None
        tmdb_id = None

        # New Plex agent — guids array
        guids = record.get('guids') or []
        if isinstance(guids, list):
            for g in guids:
                gid = (g.get('id') or '') if isinstance(g, dict) else str(g or '')
                if gid.startswith('imdb://'):
                    imdb_id = gid[7:].split('?')[0]
                elif gid.startswith('tmdb://'):
                    tmdb_id = gid[7:].split('?')[0]

        # Old Plex agent fallback — single guid string
        if not imdb_id and not tmdb_id:
            guid = record.get('guid', '') or ''
            m = re.search(r'agents\.imdb://([^?]+)', guid)
            if m:
                imdb_id = m.group(1)
            m = re.search(r'agents\.themoviedb://([^?]+)', guid)
            if m:
                tmdb_id = m.group(1)

        return imdb_id or None, tmdb_id or None

    def _save_aggregated_media(self, movies_data: dict, tv_data: dict):
        """Save aggregated media data to the database."""
        if not movies_data and not tv_data:
            raise ValueError("Media refresh returned no media; keeping previous cached media.")

        resolution_order = {
            '4k': 0, '2160p': 0, '1080p': 1, '1080': 1,
            '720p': 2, '720': 2, '480p': 3, '480': 3, 'sd': 4
        }

        def sort_resolutions(resolutions: set) -> str:
            sorted_res = sorted(
                resolutions,
                key=lambda r: resolution_order.get(r.lower(), 99)
            )
            return ' | '.join(sorted_res)

        def format_size_versions(sizes: set) -> str:
            if not sizes:
                return ''
            sorted_sizes = sorted(sizes, reverse=True)
            return ' | '.join(f"{size / (1024 ** 3):.2f}" for size in sorted_sizes)

        try:
            MediaRating.query.delete()
            CachedMedia.query.delete()

            # Save movies
            for key, data in movies_data.items():
                video_codec = ' | '.join(sorted(data['video_codecs'])) if data['video_codecs'] else ''
                video_resolution = sort_resolutions(data['video_resolutions']) if data['video_resolutions'] else ''
                file_size_versions = format_size_versions(data.get('file_sizes', set()))

                media = CachedMedia(
                    media_type='movie',
                    title=data['title'],
                    year=data['year'],
                    file_size=data['file_size'],
                    file_size_versions=file_size_versions,
                    file_paths=json.dumps(sorted(data.get('file_paths', set()))),
                    play_count=data['play_count'],
                    added_at=data['added_at'] if data['added_at'] else None,
                    last_played=data['last_played'] if data['last_played'] else None,
                    video_codec=video_codec,
                    video_resolution=video_resolution,
                    rating=str(data['rating']) if data.get('rating') else None,
                    rating_image=data.get('rating_image'),
                    audience_rating=str(data['audience_rating']) if data.get('audience_rating') else None,
                    audience_rating_image=data.get('audience_rating_image'),
                    imdb_id=data.get('imdb_id'),
                    tmdb_id=data.get('tmdb_id'),
                )
                db.session.add(media)

            # Save TV shows
            for key, data in tv_data.items():
                media = CachedMedia(
                    media_type='show',
                    title=data['title'],
                    year=None,
                    file_size=data['file_size'],
                    file_paths=json.dumps(sorted(data.get('file_paths', set()))),
                    play_count=data['play_count'],
                    season_count=data.get('season_count', 0),
                    episode_count=data.get('episode_count', 0),
                    added_at=data['added_at'] if data['added_at'] else None,
                    last_played=data['last_played'] if data['last_played'] else None,
                    rating=str(data['rating']) if data.get('rating') else None,
                    rating_image=data.get('rating_image'),
                    audience_rating=str(data['audience_rating']) if data.get('audience_rating') else None,
                    audience_rating_image=data.get('audience_rating_image'),
                    imdb_id=data.get('imdb_id'),
                    tmdb_id=data.get('tmdb_id'),
                )
                db.session.add(media)

            db.session.commit()
        except Exception:
            db.session.rollback()
            raise

    @staticmethod
    def _ratings_by_media_id(media_ids: list) -> dict:
        """Bulk-load MediaRating rows and return a dict of {media_id: {source: row}}."""
        if not media_ids:
            return {}
        rows = MediaRating.query.filter(MediaRating.cached_media_id.in_(media_ids)).all()
        result = {}
        for r in rows:
            result.setdefault(r.cached_media_id, {})[r.source] = r
        return result

    @staticmethod
    def _extract_mdb_summary(ratings_for_item: dict) -> dict:
        """Extract a compact MDB ratings summary dict for table display."""
        def _val(source):
            r = ratings_for_item.get(source)
            return r.value if r else None

        def _score(source):
            r = ratings_for_item.get(source)
            return r.score if r else None

        def _votes(source):
            r = ratings_for_item.get(source)
            return r.votes if r else None

        imdb_r = ratings_for_item.get('imdb')
        return {
            'imdb': _val('imdb'),
            'imdb_votes': _votes('imdb'),
            'imdb_popular': imdb_r.popular if imdb_r else None,
            'tmdb': _val('tmdb'),
            'tmdb_votes': _votes('tmdb'),
            'trakt': _val('trakt'),
            'tomatoes': _score('tomatoes'),
            'tomatoes_votes': _votes('tomatoes'),
            'tomatoesaudience': _score('tomatoesaudience') or _score('popcorn'),
            'tomatoesaudience_votes': _votes('tomatoesaudience') or _votes('popcorn'),
            'metacritic': _score('metacritic'),
            'metacritic_votes': _votes('metacritic'),
            'letterboxd': _val('letterboxd'),
            'letterboxd_score': _score('letterboxd'),
            'letterboxd_votes': _votes('letterboxd'),
        }

    def get_movies(self) -> list[dict]:
        """Get all movies formatted for display."""
        movies = CachedMedia.query.filter_by(media_type='movie').order_by(
            CachedMedia.added_at.desc()
        ).all()

        all_ratings = self._ratings_by_media_id([m.id for m in movies])

        result = []
        for movie in movies:
            title_with_year = movie.title
            if movie.year:
                title_with_year = f"{movie.title} ({movie.year})"

            added_at_str = ''
            if movie.added_at:
                added_at_dt = datetime.fromtimestamp(movie.added_at, tz=timezone.utc).astimezone(self.local_tz)
                added_at_str = added_at_dt.strftime('%Y-%m-%d')

            last_played_str = ''
            if movie.last_played:
                last_played_dt = datetime.fromtimestamp(movie.last_played, tz=timezone.utc).astimezone(self.local_tz)
                last_played_str = last_played_dt.strftime('%Y-%m-%d')

            file_size_gb = movie.file_size / (1024 ** 3) if movie.file_size else 0

            mdb = self._extract_mdb_summary(all_ratings.get(movie.id, {}))
            result.append({
                'media_id': movie.id,
                'title': title_with_year,
                'content_title': movie.title,
                'content_year': movie.year,
                'added_at': added_at_str,
                'video_codec': movie.video_codec or '',
                'video_resolution': movie.video_resolution or '',
                'file_size': round(file_size_gb, 2),
                'file_size_versions': movie.file_size_versions or '',
                'file_paths': sorted(self._decode_cache_set(movie.file_paths)),
                'last_played': last_played_str,
                'play_count': movie.play_count,
                'rating': movie.rating or '',
                'rating_image': movie.rating_image or '',
                'audience_rating': movie.audience_rating or '',
                'audience_rating_image': movie.audience_rating_image or '',
                'imdb_id': movie.imdb_id or '',
                'tmdb_id': movie.tmdb_id or '',
                'mdb': mdb,
            })

        return result

    def get_tv_shows(self) -> list[dict]:
        """Get all TV shows formatted for display."""
        shows = CachedMedia.query.filter_by(media_type='show').order_by(
            CachedMedia.added_at.desc()
        ).all()

        all_ratings = self._ratings_by_media_id([s.id for s in shows])

        result = []
        for show in shows:
            added_at_str = ''
            if show.added_at:
                added_at_dt = datetime.fromtimestamp(show.added_at, tz=timezone.utc).astimezone(self.local_tz)
                added_at_str = added_at_dt.strftime('%Y-%m-%d')

            last_played_str = ''
            if show.last_played:
                last_played_dt = datetime.fromtimestamp(show.last_played, tz=timezone.utc).astimezone(self.local_tz)
                last_played_str = last_played_dt.strftime('%Y-%m-%d')

            file_size_gb = show.file_size / (1024 ** 3) if show.file_size else 0

            mdb = self._extract_mdb_summary(all_ratings.get(show.id, {}))
            result.append({
                'media_id': show.id,
                'title': show.title,
                'content_title': show.title,
                'content_year': None,
                'added_at': added_at_str,
                'file_size': round(file_size_gb, 2),
                'file_paths': sorted(self._decode_cache_set(show.file_paths)),
                'last_played': last_played_str,
                'play_count': show.play_count,
                'season_count': show.season_count or 0,
                'episode_count': show.episode_count or 0,
                'rating': show.rating or '',
                'rating_image': show.rating_image or '',
                'audience_rating': show.audience_rating or '',
                'audience_rating_image': show.audience_rating_image or '',
                'imdb_id': show.imdb_id or '',
                'tmdb_id': show.tmdb_id or '',
                'mdb': mdb,
            })

        return result
