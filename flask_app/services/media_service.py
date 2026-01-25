"""
Service for syncing media library info from Tautulli to local database.
Uses export_metadata API for rich metadata including ratings.
Supports parallel fetching from multiple servers.
"""
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

from flask import current_app
from flask_app.models import db, MediaSyncStatus, CachedMedia
from flask_app.services.config_service import ConfigService
from multiplex_stats.api_client import TautulliClient
from multiplex_stats.timezone_utils import get_local_timezone


class MediaService:
    """Service for managing media library sync operations."""

    EXPORT_POLL_INTERVAL = 2   # seconds between export status checks
    EXPORT_TIMEOUT = 600       # max seconds to wait for export (10 minutes)

    def __init__(self):
        self.local_tz = get_local_timezone()

    def get_or_create_status(self) -> MediaSyncStatus:
        """Get or create the singleton sync status record."""
        status = MediaSyncStatus.query.first()
        if not status:
            status = MediaSyncStatus()
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
            'current_step': status.current_step,
            'records_fetched': status.records_fetched,
            'records_total': status.records_total,
            'movies_count': status.movies_count,
            'tv_shows_count': status.tv_shows_count,
            'error_message': status.error_message,
            'last_sync_date': last_sync_date,
            'has_data': self.has_media_data(),
            'servers': servers,
        }

    def has_media_data(self) -> bool:
        """Check if there's any media data in the database."""
        return CachedMedia.query.count() > 0

    def start_media_load(self, app=None) -> bool:
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

        # Reset status for new load
        status.status = 'running'
        status.started_at = datetime.utcnow()
        status.completed_at = None
        status.current_step = 'Initializing...'
        status.records_fetched = 0
        status.records_total = None
        status.movies_count = 0
        status.tv_shows_count = 0
        status.error_message = None

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

        # Clear existing media data
        CachedMedia.query.delete()
        db.session.commit()

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
                status = self.get_or_create_status()
                status.status = 'success'
                status.completed_at = datetime.utcnow()
                status.last_sync_date = datetime.utcnow()
                status.current_step = 'Complete'
                status.movies_count = CachedMedia.query.filter_by(media_type='movie').count()
                status.tv_shows_count = CachedMedia.query.filter_by(media_type='show').count()
            except Exception as e:
                status = self.get_or_create_status()
                status.status = 'failed'
                status.completed_at = datetime.utcnow()
                status.error_message = str(e)

            db.session.commit()

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

        # Track errors from threads
        errors = []
        errors_lock = threading.Lock()

        def fetch_server(server_config, is_primary: bool, server_key: str):
            """Fetch media from a single server (runs in thread)."""
            with app.app_context():
                try:
                    self._fetch_server_media_parallel(
                        server_config, movies_data, tv_data, data_lock,
                        is_primary, server_key
                    )
                except Exception as e:
                    with errors_lock:
                        errors.append((server_key, str(e)))
                    # Update server status to failed
                    status = self.get_or_create_status()
                    if server_key == 'a':
                        status.server_a_status = 'failed'
                        status.server_a_error = str(e)
                    else:
                        status.server_b_status = 'failed'
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

        # Check if any critical errors occurred
        if errors:
            # If Server A failed, that's critical
            for server_key, error_msg in errors:
                if server_key == 'a':
                    raise ValueError(f"Server A failed: {error_msg}")

        # Save aggregated data to database
        status = self.get_or_create_status()
        status.current_step = 'Saving media data...'
        db.session.commit()
        self._save_aggregated_media(movies_data, tv_data)

        status.current_step = 'Finalizing...'
        db.session.commit()

    def _fetch_server_media_parallel(
        self,
        server_config,
        movies_data: dict,
        tv_data: dict,
        data_lock: threading.Lock,
        is_primary: bool,
        server_key: str
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

        # Update server status to running
        if server_key == 'a':
            status.server_a_status = 'running'
            status.server_a_step = 'Connecting...'
        else:
            status.server_b_status = 'running'
            status.server_b_step = 'Connecting...'
        db.session.commit()

        client = TautulliClient(server_config)

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

        # Process each library via export_metadata (for ratings)
        for lib in movie_libraries:
            self._fetch_library_via_export_parallel(
                client, server_config.name, lib['id'], lib['name'],
                'movie', movies_data, tv_data, data_lock, is_primary, server_key
            )

        for lib in tv_libraries:
            self._fetch_library_via_export_parallel(
                client, server_config.name, lib['id'], lib['name'],
                'show', movies_data, tv_data, data_lock, is_primary, server_key
            )

        # Fetch play stats from get_library_media_info
        status = self.get_or_create_status()
        if server_key == 'a':
            status.server_a_step = 'Fetching play stats...'
        else:
            status.server_b_step = 'Fetching play stats...'
        db.session.commit()

        for lib in movie_libraries:
            self._fetch_library_play_stats_parallel(
                client, lib['id'], 'movie', movies_data, data_lock
            )

        for lib in tv_libraries:
            self._fetch_library_play_stats_parallel(
                client, lib['id'], 'show', tv_data, data_lock
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
        server_key: str
    ):
        """
        Fetch library metadata using export_metadata API (parallel version).
        """
        status = self.get_or_create_status()

        # Update server step
        if server_key == 'a':
            status.server_a_step = f'Starting export for {section_name}...'
        else:
            status.server_b_step = f'Starting export for {section_name}...'
        db.session.commit()

        export_response = client.export_metadata(
            section_id=section_id,
            file_format='json',
            metadata_level=1,
            media_info_level=1
        )

        if not export_response or 'response' not in export_response:
            raise ValueError(f"Failed to start export for {section_name}")

        response_data = export_response['response'].get('data', {})
        export_id = response_data.get('export_id')
        if not export_id:
            raise ValueError(f"No export_id returned for {section_name}")

        # Wait for export to complete
        export_data = self._wait_for_export_parallel(
            client, section_id, export_id, section_name, server_key
        )

        # Process the export data
        data_dict = movies_data if media_type == 'movie' else tv_data
        self._process_export_data_parallel(
            export_data, media_type, data_dict, data_lock, is_primary, server_key
        )

    def _wait_for_export_parallel(
        self,
        client: TautulliClient,
        section_id: int,
        export_id: int,
        section_name: str,
        server_key: str
    ) -> list:
        """
        Poll for export completion and download when ready (parallel version).
        """
        start_time = time.time()

        while True:
            elapsed = int(time.time() - start_time)

            if elapsed > self.EXPORT_TIMEOUT:
                raise ValueError(f"Export timed out for {section_name} after {self.EXPORT_TIMEOUT}s")

            # Update server step with elapsed time
            status = self.get_or_create_status()
            if server_key == 'a':
                status.server_a_step = f'Waiting for export {section_name} ({elapsed}s)...'
            else:
                status.server_b_step = f'Waiting for export {section_name} ({elapsed}s)...'
            db.session.commit()

            # Check export status
            exports_response = client.get_exports_table(section_id)
            if exports_response and 'response' in exports_response:
                outer_data = exports_response['response'].get('data', {})
                exports = outer_data.get('data', []) if isinstance(outer_data, dict) else []
                for export in exports:
                    if export.get('export_id') == export_id:
                        if export.get('complete') == 1:
                            # Download completed export
                            status = self.get_or_create_status()
                            if server_key == 'a':
                                status.server_a_step = f'Downloading export for {section_name}...'
                            else:
                                status.server_b_step = f'Downloading export for {section_name}...'
                            db.session.commit()

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

    def _process_export_data_parallel(
        self,
        export_data: list,
        media_type: str,
        data_dict: dict,
        data_lock: threading.Lock,
        is_primary: bool,
        server_key: str
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
            else:
                key = title
                year = None

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

            file_size = 0
            play_count = 0
            last_played = 0

            video_codec = media_info.get('videoCodec', '') or ''
            video_resolution = media_info.get('videoResolution', '') or ''

            rating = record.get('rating')
            rating_image = record.get('ratingImage')
            audience_rating = record.get('audienceRating')
            audience_rating_image = record.get('audienceRatingImage')

            # Thread-safe update to shared dict
            with data_lock:
                if key in data_dict:
                    existing = data_dict[key]

                    existing['file_size'] += file_size
                    existing['play_count'] += play_count

                    if added_at:
                        if existing['added_at']:
                            existing['added_at'] = min(existing['added_at'], added_at)
                        else:
                            existing['added_at'] = added_at

                    if last_played:
                        existing['last_played'] = max(existing['last_played'] or 0, last_played)

                    if video_codec:
                        existing['video_codecs'].add(video_codec)
                    if video_resolution:
                        existing['video_resolutions'].add(video_resolution)
                    if file_size:
                        existing['file_sizes'].add(file_size)

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
                    else:
                        if not existing.get('rating') and rating:
                            existing['rating'] = rating
                        if not existing.get('rating_image') and rating_image:
                            existing['rating_image'] = rating_image
                        if not existing.get('audience_rating') and audience_rating:
                            existing['audience_rating'] = audience_rating
                        if not existing.get('audience_rating_image') and audience_rating_image:
                            existing['audience_rating_image'] = audience_rating_image
                else:
                    data_dict[key] = {
                        'title': title,
                        'year': year,
                        'file_size': file_size,
                        'play_count': play_count,
                        'added_at': added_at,
                        'last_played': last_played,
                        'video_codecs': {video_codec} if video_codec else set(),
                        'video_resolutions': {video_resolution} if video_resolution else set(),
                        'file_sizes': {file_size} if file_size else set(),
                        'rating': rating,
                        'rating_image': rating_image,
                        'audience_rating': audience_rating,
                        'audience_rating_image': audience_rating_image,
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

    def _fetch_library_play_stats_parallel(
        self,
        client: TautulliClient,
        section_id: int,
        media_type: str,
        data_dict: dict,
        data_lock: threading.Lock
    ):
        """
        Fetch play stats from get_library_media_info and merge into data dict (thread-safe).
        """
        response = client.get_library_media_info(
            section_id=section_id,
            length=25000,
            refresh=False
        )

        if not response or 'response' not in response:
            return

        outer_data = response['response'].get('data', {})
        items = outer_data.get('data', []) if isinstance(outer_data, dict) else []

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

                file_size = int(item.get('file_size', 0) or 0)
                play_count = int(item.get('play_count', 0) or 0)
                last_played = int(item.get('last_played', 0) or 0)
                video_codec = item.get('video_codec', '') or ''
                video_resolution = item.get('video_resolution', '') or ''

                existing = data_dict[key]

                existing['file_size'] += file_size
                existing['play_count'] += play_count

                if last_played:
                    existing['last_played'] = max(existing['last_played'] or 0, last_played)

                if video_codec:
                    existing['video_codecs'].add(video_codec)
                if video_resolution:
                    existing['video_resolutions'].add(video_resolution)
                if file_size:
                    existing['file_sizes'].add(file_size)

    def _save_aggregated_media(self, movies_data: dict, tv_data: dict):
        """Save aggregated media data to the database."""
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
                play_count=data['play_count'],
                added_at=data['added_at'] if data['added_at'] else None,
                last_played=data['last_played'] if data['last_played'] else None,
                video_codec=video_codec,
                video_resolution=video_resolution,
                rating=str(data['rating']) if data.get('rating') else None,
                rating_image=data.get('rating_image'),
                audience_rating=str(data['audience_rating']) if data.get('audience_rating') else None,
                audience_rating_image=data.get('audience_rating_image'),
            )
            db.session.add(media)

        # Save TV shows
        for key, data in tv_data.items():
            media = CachedMedia(
                media_type='show',
                title=data['title'],
                year=None,
                file_size=data['file_size'],
                play_count=data['play_count'],
                added_at=data['added_at'] if data['added_at'] else None,
                last_played=data['last_played'] if data['last_played'] else None,
                rating=str(data['rating']) if data.get('rating') else None,
                rating_image=data.get('rating_image'),
                audience_rating=str(data['audience_rating']) if data.get('audience_rating') else None,
                audience_rating_image=data.get('audience_rating_image'),
            )
            db.session.add(media)

        db.session.commit()

    def get_movies(self) -> list[dict]:
        """Get all movies formatted for display."""
        movies = CachedMedia.query.filter_by(media_type='movie').order_by(
            CachedMedia.added_at.desc()
        ).all()

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

            result.append({
                'title': title_with_year,
                'added_at': added_at_str,
                'video_codec': movie.video_codec or '',
                'video_resolution': movie.video_resolution or '',
                'file_size': round(file_size_gb, 2),
                'file_size_versions': movie.file_size_versions or '',
                'last_played': last_played_str,
                'play_count': movie.play_count,
                'rating': movie.rating or '',
                'rating_image': movie.rating_image or '',
                'audience_rating': movie.audience_rating or '',
                'audience_rating_image': movie.audience_rating_image or '',
            })

        return result

    def get_tv_shows(self) -> list[dict]:
        """Get all TV shows formatted for display."""
        shows = CachedMedia.query.filter_by(media_type='show').order_by(
            CachedMedia.added_at.desc()
        ).all()

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

            result.append({
                'title': show.title,
                'added_at': added_at_str,
                'file_size': round(file_size_gb, 2),
                'last_played': last_played_str,
                'play_count': show.play_count,
                'rating': show.rating or '',
                'rating_image': show.rating_image or '',
                'audience_rating': show.audience_rating or '',
                'audience_rating_image': show.audience_rating_image or '',
            })

        return result
