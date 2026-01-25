"""
Service for syncing media library info from Tautulli to local database.
Uses export_metadata API for rich metadata including ratings.
"""
import time
import threading
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
    EXPORT_TIMEOUT = 300       # max seconds to wait for export (5 minutes)

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
        return {
            'status': status.status,
            'current_step': status.current_step,
            'records_fetched': status.records_fetched,
            'records_total': status.records_total,
            'movies_count': status.movies_count,
            'tv_shows_count': status.tv_shows_count,
            'error_message': status.error_message,
            'last_sync_date': last_sync_date,
            'has_data': self.has_media_data()
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
                self._run_media_sync()
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

    def _run_media_sync(self):
        """Run the actual media sync operation using export_metadata."""
        status = self.get_or_create_status()
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            raise ValueError("No server configuration found")

        # Temporary storage for aggregation
        # Movies: key = (title, year), TV: key = title
        movies_data = {}
        tv_data = {}

        # Process Server A (primary - its metadata takes priority)
        status.current_step = f'Processing {server_a_config.name}...'
        db.session.commit()
        self._fetch_server_media(server_a_config, movies_data, tv_data, is_primary=True)

        # Process Server B if configured (secondary)
        if server_b_config:
            status.current_step = f'Processing {server_b_config.name}...'
            db.session.commit()
            self._fetch_server_media(server_b_config, movies_data, tv_data, is_primary=False)

        # Save aggregated data to database
        status.current_step = 'Saving media data...'
        db.session.commit()
        self._save_aggregated_media(movies_data, tv_data)

        status.current_step = 'Finalizing...'
        db.session.commit()

    def _fetch_server_media(
        self,
        server_config,
        movies_data: dict,
        tv_data: dict,
        is_primary: bool
    ):
        """
        Fetch media from a single server using export_metadata API.

        Args:
            server_config: Server configuration object
            movies_data: Dict to aggregate movie data
            tv_data: Dict to aggregate TV data
            is_primary: If True, this server's metadata takes priority
        """
        status = self.get_or_create_status()
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
        if status.records_total is None:
            status.records_total = server_total
        else:
            status.records_total += server_total
        db.session.commit()

        # Process each library via export_metadata
        for lib in movie_libraries:
            self._fetch_library_via_export(
                client, server_config.name, lib['id'], lib['name'],
                'movie', movies_data, is_primary
            )

        for lib in tv_libraries:
            self._fetch_library_via_export(
                client, server_config.name, lib['id'], lib['name'],
                'show', tv_data, is_primary
            )

    def _fetch_library_via_export(
        self,
        client: TautulliClient,
        server_name: str,
        section_id: int,
        section_name: str,
        media_type: str,
        data_dict: dict,
        is_primary: bool
    ):
        """
        Fetch library metadata using export_metadata API.

        Args:
            client: TautulliClient instance
            server_name: Server name for status updates
            section_id: Library section ID
            section_name: Library name for status updates
            media_type: 'movie' or 'show'
            data_dict: Dict to aggregate data into
            is_primary: If True, this server's metadata takes priority
        """
        status = self.get_or_create_status()

        # Start export
        status.current_step = f'Starting export for {server_name} - {section_name}...'
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
        export_data = self._wait_for_export(
            client, section_id, export_id, section_name, server_name
        )

        # Process the export data
        self._process_export_data(export_data, media_type, data_dict, is_primary)

    def _wait_for_export(
        self,
        client: TautulliClient,
        section_id: int,
        export_id: int,
        section_name: str,
        server_name: str
    ) -> list:
        """
        Poll for export completion and download when ready.

        Args:
            client: TautulliClient instance
            section_id: Library section ID
            export_id: Export ID to wait for
            section_name: Library name for status updates
            server_name: Server name for status updates

        Returns:
            List of metadata records from the export

        Raises:
            ValueError: If export times out or fails
        """
        status = self.get_or_create_status()
        start_time = time.time()

        while True:
            elapsed = int(time.time() - start_time)

            if elapsed > self.EXPORT_TIMEOUT:
                raise ValueError(f"Export timed out for {section_name} after {self.EXPORT_TIMEOUT}s")

            status.current_step = f'Waiting for export {section_name} ({elapsed}s)...'
            db.session.commit()

            # Check export status
            exports_response = client.get_exports_table(section_id)
            if exports_response and 'response' in exports_response:
                exports = exports_response['response'].get('data', [])
                for export in exports:
                    if export.get('export_id') == export_id:
                        if export.get('complete') == 1:
                            # Download completed export
                            status.current_step = f'Downloading export for {section_name}...'
                            db.session.commit()

                            download_response = client.download_export(export_id)
                            if download_response:
                                # download_export returns the data directly
                                return download_response if isinstance(download_response, list) else []
                            raise ValueError(f"Failed to download export for {section_name}")
                        break

            time.sleep(self.EXPORT_POLL_INTERVAL)

    def _process_export_data(
        self,
        export_data: list,
        media_type: str,
        data_dict: dict,
        is_primary: bool
    ):
        """
        Process export metadata and merge into data dict.

        Args:
            export_data: List of metadata records from export
            media_type: 'movie' or 'show'
            data_dict: Dict to aggregate data into
            is_primary: If True, this server's metadata takes priority for ratings
        """
        status = self.get_or_create_status()

        for record in export_data:
            title = record.get('title', '')
            if not title:
                continue

            if media_type == 'movie':
                year = record.get('year')
                key = (title, year)
            else:
                key = title
                year = None

            # Extract fields from export
            file_size = int(record.get('file_size', 0) or 0)
            play_count = int(record.get('play_count', 0) or 0)
            added_at = int(record.get('added_at', 0) or 0)
            last_played = int(record.get('last_played', 0) or 0)
            video_codec = record.get('video_codec', '') or ''
            video_resolution = record.get('video_resolution', '') or ''
            rating = record.get('rating')
            rating_image = record.get('rating_image')
            audience_rating = record.get('audience_rating')
            audience_rating_image = record.get('audience_rating_image')

            if key in data_dict:
                existing = data_dict[key]

                # Aggregate numeric fields
                existing['file_size'] += file_size
                existing['play_count'] += play_count

                # MIN for added_at
                if added_at:
                    if existing['added_at']:
                        existing['added_at'] = min(existing['added_at'], added_at)
                    else:
                        existing['added_at'] = added_at

                # MAX for last_played
                if last_played:
                    existing['last_played'] = max(existing['last_played'] or 0, last_played)

                # Collect unique codecs, resolutions, file sizes
                if video_codec:
                    existing['video_codecs'].add(video_codec)
                if video_resolution:
                    existing['video_resolutions'].add(video_resolution)
                if file_size:
                    existing['file_sizes'].add(file_size)

                # Ratings: primary server takes priority, only fill if not already set
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
                    # Secondary: only fill if not set
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

            status.records_fetched += 1

        db.session.commit()

    def _save_aggregated_media(self, movies_data: dict, tv_data: dict):
        """Save aggregated media data to the database."""
        # Resolution sort order (highest quality first)
        resolution_order = {
            '4k': 0, '2160p': 0, '1080p': 1, '1080': 1,
            '720p': 2, '720': 2, '480p': 3, '480': 3, 'sd': 4
        }

        def sort_resolutions(resolutions: set) -> str:
            """Sort resolutions by quality (highest first) and join with |"""
            sorted_res = sorted(
                resolutions,
                key=lambda r: resolution_order.get(r.lower(), 99)
            )
            return ' | '.join(sorted_res)

        def format_size_versions(sizes: set) -> str:
            """Format unique file sizes (bytes) in GB, largest first."""
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
