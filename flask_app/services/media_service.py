"""
Service for syncing media library info from Tautulli to local database.
"""
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
        """Run the actual media sync operation."""
        status = self.get_or_create_status()
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            raise ValueError("No server configuration found")

        # Temporary storage for aggregation
        movies_data = {}  # key: (title, year) -> aggregate stats + version details
        tv_data = {}  # key: title -> {file_size, play_count, added_at, last_played}

        # Process Server A
        status.current_step = f'Fetching libraries from {server_a_config.name}...'
        db.session.commit()

        self._fetch_server_media(server_a_config, movies_data, tv_data)

        # Process Server B if configured
        if server_b_config:
            status.current_step = f'Fetching libraries from {server_b_config.name}...'
            db.session.commit()
            self._fetch_server_media(server_b_config, movies_data, tv_data)

        # Now insert aggregated data into the database
        status.current_step = 'Saving media data...'
        db.session.commit()

        self._save_aggregated_media(movies_data, tv_data)

        status.current_step = 'Finalizing...'
        db.session.commit()

    def _fetch_server_media(self, server_config, movies_data: dict, tv_data: dict):
        """
        Fetch media from a single server and aggregate into the data dicts.

        Args:
            server_config: Server configuration object
            movies_data: Dict to aggregate movie data
            tv_data: Dict to aggregate TV data
        """
        status = self.get_or_create_status()
        client = TautulliClient(server_config)

        # First, get libraries to find ALL movie and TV section IDs
        libraries_response = client.get_libraries()
        if not libraries_response or 'response' not in libraries_response:
            raise ValueError(f"Failed to get libraries from {server_config.name}")

        libraries = libraries_response['response'].get('data', [])

        # Collect ALL movie and TV libraries with their counts
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

        # Add library counts to cumulative total for progress tracking
        server_total = sum(lib['count'] for lib in movie_libraries) + sum(lib['count'] for lib in tv_libraries)
        if status.records_total is None:
            status.records_total = server_total
        else:
            status.records_total += server_total
        db.session.commit()

        # Fetch movies from ALL movie libraries
        for lib in movie_libraries:
            status.current_step = f'Fetching movies from {server_config.name} - {lib["name"]}...'
            db.session.commit()
            self._fetch_library_media(client, lib['id'], 'movie', movies_data, server_config.name)

        # Fetch TV shows from ALL TV libraries
        for lib in tv_libraries:
            status.current_step = f'Fetching TV shows from {server_config.name} - {lib["name"]}...'
            db.session.commit()
            self._fetch_library_media(client, lib['id'], 'show', tv_data, server_config.name)

    def _fetch_library_media(
        self,
        client: TautulliClient,
        section_id: int,
        media_type: str,
        data_dict: dict,
        server_name: str
    ):
        """
        Fetch media info for a library section with pagination.

        Args:
            client: TautulliClient instance
            section_id: Library section ID
            media_type: 'movie' or 'show'
            data_dict: Dict to aggregate data into
            server_name: Server name for progress updates
        """
        status = self.get_or_create_status()
        page_size = 3000
        start = 0
        total_records = None

        while True:
            response = client.get_library_media_info(
                section_id=section_id,
                start=start,
                length=page_size
            )

            if not response or 'response' not in response:
                break

            data = response['response'].get('data', {})
            records = data.get('data', [])

            # Get total on first request
            if total_records is None:
                total_records = data.get('recordsTotal', 0)

            if not records:
                break

            # Process records from this page
            for record in records:
                if media_type == 'movie':
                    title = record.get('title', '')
                    year = record.get('year')
                    key = (title, year)

                    file_size = int(record.get('file_size', 0) or 0)
                    play_count = int(record.get('play_count', 0) or 0)
                    added_at = int(record.get('added_at', 0) or 0)
                    last_played = int(record.get('last_played', 0) or 0)
                    video_codec = record.get('video_codec', '')
                    video_resolution = record.get('video_resolution', '')

                    if key in data_dict:
                        # Aggregate: SUM file_size and play_count, MAX on dates
                        data_dict[key]['file_size'] += file_size
                        data_dict[key]['play_count'] += play_count
                        if added_at:
                            if data_dict[key]['added_at']:
                                data_dict[key]['added_at'] = min(data_dict[key]['added_at'], added_at)
                            else:
                                data_dict[key]['added_at'] = added_at
                        if last_played:
                            data_dict[key]['last_played'] = max(data_dict[key]['last_played'], last_played)
                        # Collect all unique codecs and resolutions
                        if video_codec and video_codec not in data_dict[key]['video_codecs']:
                            data_dict[key]['video_codecs'].add(video_codec)
                        if video_resolution and video_resolution not in data_dict[key]['video_resolutions']:
                            data_dict[key]['video_resolutions'].add(video_resolution)
                        if file_size and file_size not in data_dict[key]['file_sizes']:
                            data_dict[key]['file_sizes'].add(file_size)
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
                            'file_sizes': {file_size} if file_size else set()
                        }
                else:
                    # TV show - aggregate by title only
                    title = record.get('title', '')
                    key = title

                    file_size = int(record.get('file_size', 0) or 0)
                    play_count = int(record.get('play_count', 0) or 0)
                    added_at = int(record.get('added_at', 0) or 0)
                    last_played = int(record.get('last_played', 0) or 0)

                    if key in data_dict:
                        data_dict[key]['file_size'] += file_size
                        data_dict[key]['play_count'] += play_count
                        if added_at:
                            if data_dict[key]['added_at']:
                                data_dict[key]['added_at'] = min(data_dict[key]['added_at'], added_at)
                            else:
                                data_dict[key]['added_at'] = added_at
                        if last_played:
                            data_dict[key]['last_played'] = max(data_dict[key]['last_played'], last_played)
                    else:
                        data_dict[key] = {
                            'title': title,
                            'file_size': file_size,
                            'play_count': play_count,
                            'added_at': added_at,
                            'last_played': last_played
                        }

                status.records_fetched += 1

            db.session.commit()

            # Check if we've fetched all records
            start += len(records)
            if start >= total_records:
                break

    def _save_aggregated_media(self, movies_data: dict, tv_data: dict):
        """Save aggregated media data to the database."""
        # Resolution sort order (highest quality first)
        resolution_order = {'4k': 0, '2160p': 0, '1080p': 1, '1080': 1, '720p': 2, '720': 2, '480p': 3, '480': 3, 'sd': 4}

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
        for (title, year), data in movies_data.items():
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
                video_resolution=video_resolution
            )
            db.session.add(media)

        # Save TV shows
        for title, data in tv_data.items():
            media = CachedMedia(
                media_type='show',
                title=data['title'],
                year=None,
                file_size=data['file_size'],
                play_count=data['play_count'],
                added_at=data['added_at'] if data['added_at'] else None,
                last_played=data['last_played'] if data['last_played'] else None
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
                'play_count': movie.play_count
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
                'play_count': show.play_count
            })

        return result
