"""
Service for content detail pages linked from viewing history.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import func, or_

from flask_app.models import CachedMedia, LifetimeMediaPlayCount, ServerConfig, ViewingHistory
from flask_app.services.utils import normalize_title, to_int
from multiplex_stats.api_client import TautulliClient
from multiplex_stats.timezone_utils import get_local_timezone


class ContentService:
    """Build detail-page data for movies and TV shows."""

    def __init__(self):
        self.local_tz = get_local_timezone()

    def get_content_details(self, history_id: int) -> dict[str, Any] | None:
        """
        Build all data needed for a content detail page.

        Args:
            history_id: Local ViewingHistory primary key from clicked table row

        Returns:
            Dictionary for template rendering, or None if not found
        """
        record = ViewingHistory.query.get(history_id)
        if not record:
            return None

        media_type = (record.media_type or '').lower()
        is_movie = media_type == 'movie'

        if is_movie:
            content_title = (record.title or record.full_title or 'Unknown').strip()
            details_title = content_title
            if record.year:
                details_title = f"{content_title} ({record.year})"
            content_kind = 'movie'
            query = self._build_movie_query(record, content_title)
        else:
            content_title = (record.grandparent_title or record.title or record.full_title or 'Unknown').strip()
            details_title = content_title
            content_kind = 'show'
            query = self._build_show_query(record, content_title)

        plays = query.order_by(ViewingHistory.started.desc()).all()
        metadata = self._get_metadata_for_record(record, is_movie)

        if not metadata.get('summary'):
            metadata['summary'] = 'No summary available.'

        lifetime_stats = self._get_lifetime_content_stats(
            record=record,
            plays=plays,
            is_movie=is_movie,
            content_title=content_title,
            content_year=record.year if is_movie else None,
            fallback_total_plays=None,
        )
        watch_history = [self._format_watch_history_row(item, content_title) for item in plays]
        plays_chart = self._build_plays_by_year_chart(plays, details_title)

        plays_by_user_chart = None
        if not is_movie:
            endpoint_user_counts = lifetime_stats.get('user_play_counts', {})
            endpoint_user_labels = lifetime_stats.get('user_display_names', {})
            if endpoint_user_counts:
                plays_by_user_chart = self._build_plays_by_user_chart_from_counts(
                    user_play_counts=endpoint_user_counts,
                    user_display_names=endpoint_user_labels,
                    title=details_title,
                )
            else:
                plays_by_user_chart = self._build_plays_by_user_chart(plays, details_title)

        return {
            'content_kind': content_kind,
            'content_title': details_title,
            'is_movie': is_movie,
            'metadata': metadata,
            'plays_chart': plays_chart,
            'plays_by_user_chart': plays_by_user_chart,
            'watch_history': watch_history,
            'total_plays': lifetime_stats['total_plays'],
            'unique_users': lifetime_stats['unique_users'],
            'source_record_id': record.id,
        }

    def get_content_details_for_media(self, media_id: int) -> dict[str, Any] | None:
        """
        Build content detail page data from CachedMedia id.
        This keeps media-page links available even when local history cache is limited.
        """
        media = CachedMedia.query.get(media_id)
        if not media:
            return None

        is_movie = (media.media_type or '').lower() == 'movie'
        content_title = (media.title or 'Unknown').strip()
        content_kind = 'movie' if is_movie else 'show'
        details_title = content_title
        if is_movie and media.year:
            details_title = f"{content_title} ({media.year})"

        if is_movie:
            query = (
                ViewingHistory.query
                .filter(func.lower(ViewingHistory.media_type) == 'movie')
                .filter(
                    or_(
                        func.lower(ViewingHistory.title) == content_title.lower(),
                        func.lower(ViewingHistory.full_title) == content_title.lower(),
                    )
                )
            )
            if media.year:
                query = query.filter(ViewingHistory.year == media.year)
        else:
            query = (
                ViewingHistory.query
                .filter(func.lower(ViewingHistory.media_type).in_(['episode', 'tv', 'show']))
                .filter(func.lower(ViewingHistory.grandparent_title) == content_title.lower())
            )

        plays = query.order_by(ViewingHistory.started.desc()).all()
        source_record = plays[0] if plays else None
        metadata = self._get_metadata_for_media(
            media=media,
            source_record=source_record,
            is_movie=is_movie,
            content_title=content_title,
        )

        if not metadata.get('summary'):
            metadata['summary'] = 'No summary available.'

        lifetime_stats = self._get_lifetime_content_stats(
            record=source_record,
            plays=plays,
            is_movie=is_movie,
            content_title=content_title,
            content_year=media.year if is_movie else None,
            fallback_total_plays=media.play_count,
        )
        watch_history = [self._format_watch_history_row(item, content_title) for item in plays]
        plays_chart = self._build_plays_by_year_chart(plays, details_title)

        plays_by_user_chart = None
        if not is_movie:
            endpoint_user_counts = lifetime_stats.get('user_play_counts', {})
            endpoint_user_labels = lifetime_stats.get('user_display_names', {})
            if endpoint_user_counts:
                plays_by_user_chart = self._build_plays_by_user_chart_from_counts(
                    user_play_counts=endpoint_user_counts,
                    user_display_names=endpoint_user_labels,
                    title=details_title,
                )
            else:
                plays_by_user_chart = self._build_plays_by_user_chart(plays, details_title)

        return {
            'content_kind': content_kind,
            'content_title': details_title,
            'is_movie': is_movie,
            'metadata': metadata,
            'plays_chart': plays_chart,
            'plays_by_user_chart': plays_by_user_chart,
            'watch_history': watch_history,
            'total_plays': lifetime_stats['total_plays'],
            'unique_users': lifetime_stats['unique_users'],
            'source_record_id': source_record.id if source_record else None,
        }

    def _build_movie_query(self, record: ViewingHistory, content_title: str):
        query = ViewingHistory.query.filter(func.lower(ViewingHistory.media_type) == 'movie')

        title_filters = []
        if record.title:
            title_filters.append(func.lower(ViewingHistory.title) == record.title.lower())
        if record.full_title:
            title_filters.append(func.lower(ViewingHistory.full_title) == record.full_title.lower())
        if content_title:
            title_filters.append(func.lower(ViewingHistory.title) == content_title.lower())
            title_filters.append(func.lower(ViewingHistory.full_title) == content_title.lower())

        if title_filters:
            query = query.filter(or_(*title_filters))

        if record.year:
            query = query.filter(ViewingHistory.year == record.year)

        return query

    def _build_show_query(self, record: ViewingHistory, content_title: str):
        query = ViewingHistory.query.filter(
            func.lower(ViewingHistory.media_type).in_(['episode', 'tv', 'show'])
        )

        title_filters = []
        if record.grandparent_title:
            title_filters.append(func.lower(ViewingHistory.grandparent_title) == record.grandparent_title.lower())
        if content_title:
            title_filters.append(func.lower(ViewingHistory.grandparent_title) == content_title.lower())

        if title_filters:
            query = query.filter(or_(*title_filters))

        return query

    def _get_metadata_for_record(self, record: ViewingHistory, is_movie: bool) -> dict[str, Any]:
        metadata = {
            'summary': '',
            'poster_url': '',
            'banner_url': '',
            'director': '',
            'studio': '',
            'year': record.year or '',
            'date_added': '',
            'season_count': '',
            'episode_count': '',
            'runtime': self._format_runtime(record.duration),
            'rated': '',
            'critic_rating': '',
            'critic_rating_image': '',
            'critic_rating_display': '',
            'audience_rating': '',
            'audience_rating_image': '',
            'audience_rating_display': '',
            'video_codec': '',
            'resolution': '',
            'audio': '',
        }

        server = ServerConfig.query.filter_by(name=record.server_name, is_active=True).first()
        if not server:
            return metadata

        rating_key = record.rating_key if is_movie else (record.grandparent_rating_key or record.rating_key)
        if not rating_key:
            return metadata

        try:
            client = TautulliClient(server.to_multiplex_config())
            response = client.get_metadata(int(rating_key))
            data = self._extract_metadata_payload(response)
        except Exception:
            data = {}

        if not data:
            return metadata

        metadata['summary'] = data.get('summary') or metadata['summary']
        metadata['director'] = self._extract_director(data)
        metadata['studio'] = data.get('studio') or metadata['studio']
        metadata['year'] = data.get('year') or metadata['year']
        metadata['date_added'] = self._format_added_date(
            data.get('added_at') or data.get('addedAt') or data.get('added')
        )
        metadata_children_count = to_int(data.get('children_count') or data.get('child_count'))
        season_count, episode_count = self._extract_show_structure_counts(data)
        if season_count is not None:
            metadata['season_count'] = season_count
        if episode_count is not None:
            metadata['episode_count'] = episode_count
        metadata['runtime'] = self._format_runtime(data.get('duration') or data.get('duration_ms') or record.duration)
        metadata['rated'] = data.get('content_rating') or data.get('rating') or ''
        metadata['critic_rating'] = data.get('rating') or data.get('rating_value') or ''
        metadata['critic_rating_image'] = data.get('rating_image') or data.get('ratingImage') or ''
        metadata['audience_rating'] = data.get('audience_rating') or data.get('audienceRating') or ''
        metadata['audience_rating_image'] = (
            data.get('audience_rating_image') or data.get('audienceRatingImage') or ''
        )
        metadata['critic_rating_display'] = self._format_rating_display(
            metadata['critic_rating'],
            metadata['critic_rating_image'],
        )
        metadata['audience_rating_display'] = self._format_rating_display(
            metadata['audience_rating'],
            metadata['audience_rating_image'],
        )

        if not is_movie and (not metadata.get('season_count') or not metadata.get('episode_count')):
            try:
                children_response = client.get_children_metadata(int(rating_key))
                child_seasons, child_episodes = self._extract_show_structure_counts_from_children(
                    children_response
                )
                if not metadata.get('season_count') and child_seasons is not None:
                    metadata['season_count'] = child_seasons
                if not metadata.get('episode_count') and child_episodes is not None:
                    metadata['episode_count'] = child_episodes
            except Exception:
                pass

        if (
            not is_movie
            and not metadata.get('episode_count')
            and metadata_children_count is not None
        ):
            seasons = to_int(metadata.get('season_count'))
            if seasons is None or metadata_children_count >= seasons:
                metadata['episode_count'] = metadata_children_count

        media_info = self._extract_media_info(data)
        if media_info:
            metadata['video_codec'] = media_info.get('video_codec') or media_info.get('videoCodec') or ''
            metadata['resolution'] = media_info.get('video_resolution') or media_info.get('videoResolution') or ''
            audio_codec = media_info.get('audio_codec') or media_info.get('audioCodec') or ''
            audio_channels = media_info.get('audio_channels') or media_info.get('audioChannels') or ''
            metadata['audio'] = self._format_audio(audio_codec, audio_channels)

        protocol = 'https' if server.use_ssl else 'http'
        thumb = data.get('thumb') or record.thumb
        art = data.get('art') or ''
        metadata['poster_url'] = self._build_proxy_url(
            protocol=protocol,
            server_address=server.ip_address,
            image_path=thumb,
            rating_key=rating_key,
            width=400,
            height=600,
            fallback='poster',
        )
        metadata['banner_url'] = self._build_proxy_url(
            protocol=protocol,
            server_address=server.ip_address,
            image_path=art,
            rating_key=rating_key,
            width=1600,
            height=500,
            fallback='art',
        )

        return metadata

    def _get_metadata_for_media(
        self,
        media: CachedMedia,
        source_record: ViewingHistory | None,
        is_movie: bool,
        content_title: str,
    ) -> dict[str, Any]:
        """Get content metadata for media-page links, even when local history is sparse."""
        if source_record:
            metadata = self._get_metadata_for_record(source_record, is_movie)
        else:
            metadata = {
                'summary': '',
                'poster_url': '',
                'banner_url': '',
                'director': '',
                'studio': '',
                'year': media.year or '',
                'date_added': self._format_added_date(media.added_at),
                'season_count': '',
                'episode_count': '',
                'runtime': '',
                'rated': '',
                'critic_rating': '',
                'critic_rating_image': '',
                'critic_rating_display': '',
                'audience_rating': '',
                'audience_rating_image': '',
                'audience_rating_display': '',
                'video_codec': '',
                'resolution': '',
                'audio': '',
            }

            active_servers = (
                ServerConfig.query
                .filter(ServerConfig.is_active.is_(True))
                .order_by(ServerConfig.server_order)
                .all()
            )

            content_year = media.year if is_movie else None
            for server in active_servers:
                discovered_keys = self._discover_server_content_rating_keys(
                    server=server,
                    content_title=content_title,
                    is_movie=is_movie,
                    content_year=content_year,
                )
                if not discovered_keys:
                    continue

                selected_key = sorted(discovered_keys)[0]
                proxy_record = ViewingHistory(
                    server_name=server.name,
                    media_type='movie' if is_movie else 'episode',
                    title=content_title if is_movie else '',
                    grandparent_title='' if is_movie else content_title,
                    year=media.year if is_movie else None,
                    rating_key=selected_key,
                    grandparent_rating_key=selected_key if not is_movie else None,
                )
                metadata = self._get_metadata_for_record(proxy_record, is_movie)
                break

        if not metadata.get('year') and media.year:
            metadata['year'] = media.year
        if not metadata.get('date_added') and media.added_at:
            metadata['date_added'] = self._format_added_date(media.added_at)
        if not metadata.get('video_codec') and media.video_codec:
            metadata['video_codec'] = media.video_codec
        if not metadata.get('resolution') and media.video_resolution:
            metadata['resolution'] = media.video_resolution
        if not metadata.get('critic_rating') and media.rating:
            metadata['critic_rating'] = media.rating
        if not metadata.get('critic_rating_image') and media.rating_image:
            metadata['critic_rating_image'] = media.rating_image
        if not metadata.get('audience_rating') and media.audience_rating:
            metadata['audience_rating'] = media.audience_rating
        if not metadata.get('audience_rating_image') and media.audience_rating_image:
            metadata['audience_rating_image'] = media.audience_rating_image
        if not metadata.get('critic_rating_display'):
            metadata['critic_rating_display'] = self._format_rating_display(
                metadata.get('critic_rating'),
                metadata.get('critic_rating_image'),
            )
        if not metadata.get('audience_rating_display'):
            metadata['audience_rating_display'] = self._format_rating_display(
                metadata.get('audience_rating'),
                metadata.get('audience_rating_image'),
            )

        return metadata

    @staticmethod
    def _extract_show_structure_counts(metadata: dict[str, Any]) -> tuple[int | None, int | None]:
        """Extract season/episode totals from show metadata payload."""
        if not isinstance(metadata, dict):
            return None, None

        child_type = str(
            metadata.get('children_type')
            or metadata.get('child_type')
            or ''
        ).strip().lower()

        season_count = to_int(
            metadata.get('season_count')
            or metadata.get('seasons_count')
            or metadata.get('seasons')
        )
        episode_count = to_int(
            metadata.get('leaf_count')
            or metadata.get('grandchild_count')
            or metadata.get('grandchildren_count')
            or metadata.get('episode_count')
            or metadata.get('episodes_count')
        )

        children_count = to_int(
            metadata.get('children_count') or metadata.get('child_count')
        )
        if children_count is not None:
            if child_type.startswith('season'):
                if season_count is None:
                    season_count = children_count
            elif child_type.startswith('episode'):
                if episode_count is None:
                    episode_count = children_count

        return season_count, episode_count

    @staticmethod
    def _extract_show_structure_counts_from_children(
        response: dict[str, Any]
    ) -> tuple[int | None, int | None]:
        """Extract season/episode totals from get_children_metadata payload."""
        if not isinstance(response, dict):
            return None, None

        envelope = response.get('response', {})
        if not isinstance(envelope, dict):
            return None, None

        data = envelope.get('data', {})
        rows: list[dict[str, Any]] = []

        if isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]
        elif isinstance(data, dict):
            nested_rows = data.get('data')
            if isinstance(nested_rows, list):
                rows = [row for row in nested_rows if isinstance(row, dict)]

        if rows:
            row_types = {
                str(row.get('media_type') or '').strip().lower()
                for row in rows
                if row.get('media_type')
            }
            if row_types and row_types.issubset({'episode'}):
                return None, len(rows)

            season_rows = rows
            if row_types and 'season' in row_types:
                season_rows = [row for row in rows if str(row.get('media_type') or '').strip().lower() == 'season']
            season_count = len(season_rows)

            episode_total = 0
            has_episode_total = False
            for row in season_rows:
                child_count = to_int(
                    row.get('children_count')
                    or row.get('child_count')
                    or row.get('leaf_count')
                    or row.get('episode_count')
                )
                if child_count is not None:
                    episode_total += child_count
                    has_episode_total = True

            return season_count, (episode_total if has_episode_total else None)

        if isinstance(data, dict):
            return ContentService._extract_show_structure_counts(data)

        return None, None

    @staticmethod
    def _extract_metadata_payload(response: dict[str, Any]) -> dict[str, Any]:
        data = response.get('response', {}).get('data', {})
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict):
            return data
        return {}

    @staticmethod
    def _extract_media_info(metadata: dict[str, Any]) -> dict[str, Any]:
        media_info = metadata.get('media_info')
        if isinstance(media_info, list):
            return media_info[0] if media_info else {}
        if isinstance(media_info, dict):
            return media_info

        media = metadata.get('media')
        if isinstance(media, list):
            return media[0] if media else {}
        if isinstance(media, dict):
            return media

        return {}

    @staticmethod
    def _extract_director(metadata: dict[str, Any]) -> str:
        directors = metadata.get('directors') or metadata.get('director') or []
        if isinstance(directors, str):
            return directors
        if not isinstance(directors, list):
            return ''

        names = []
        for item in directors:
            if isinstance(item, dict):
                name = item.get('tag') or item.get('name') or ''
            else:
                name = str(item)
            if name:
                names.append(name)
        return ', '.join(names)

    @staticmethod
    def _format_runtime(duration_value: Any) -> str:
        if duration_value in (None, '', 0, '0'):
            return ''
        try:
            total = int(float(duration_value))
        except (ValueError, TypeError):
            return ''

        # Tautulli metadata duration is typically milliseconds.
        total_seconds = total // 1000 if total > 10000 else total
        if total_seconds <= 0:
            return ''

        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60

        if hours > 0:
            hour_label = 'hr' if hours == 1 else 'hrs'
            return f"{hours} {hour_label} {minutes} mins"
        return f"{minutes} mins"

    @staticmethod
    def _format_audio(audio_codec: str, audio_channels: Any) -> str:
        channels = ContentService._format_audio_channels(audio_channels)
        if audio_codec and channels:
            return f"{audio_codec.upper()} {channels}"
        if audio_codec:
            return audio_codec.upper()
        return channels

    @staticmethod
    def _format_audio_channels(audio_channels: Any) -> str:
        if audio_channels in (None, '', '0', 0):
            return ''

        try:
            value = float(audio_channels)
        except (ValueError, TypeError):
            return str(audio_channels)

        if value == 1:
            return '1.0'
        if value == 2:
            return '2.0'
        if value == 6:
            return '5.1'
        if value == 8:
            return '7.1'
        if value.is_integer():
            return f"{int(value)}.0"
        return str(value)

    def _format_added_date(self, value: Any) -> str:
        """Normalize metadata added-at values to YYYY-MM-DD in local timezone."""
        if value in (None, '', 0, '0'):
            return ''

        dt_utc: datetime | None = None

        if isinstance(value, (int, float)):
            timestamp = int(value)
            if timestamp <= 0:
                return ''
            if timestamp > 10**12:
                timestamp //= 1000
            try:
                dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (ValueError, OSError, TypeError):
                return ''
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return ''

            if text.isdigit():
                timestamp = int(text)
                if timestamp <= 0:
                    return ''
                if timestamp > 10**12:
                    timestamp //= 1000
                try:
                    dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except (ValueError, OSError, TypeError):
                    return ''
            else:
                try:
                    parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
                except ValueError:
                    return ''
                if parsed.tzinfo is None:
                    dt_utc = parsed.replace(tzinfo=timezone.utc)
                else:
                    dt_utc = parsed.astimezone(timezone.utc)
        else:
            return ''

        if not dt_utc:
            return ''

        return dt_utc.astimezone(self.local_tz).strftime('%Y-%m-%d')

    @staticmethod
    def _format_rating_display(value: Any, rating_image: str | None) -> str:
        if value in (None, ''):
            return ''

        try:
            numeric = float(value)
        except (ValueError, TypeError):
            return str(value)

        source = (rating_image or '').lower()
        if source.startswith('rottentomatoes://'):
            if 0 < numeric <= 1:
                numeric *= 100
            elif 1 < numeric <= 10:
                numeric *= 10
            return f"{round(numeric)}%"

        if numeric > 10:
            return f"{round(numeric)}%"

        return f"{numeric:.1f}"

    @staticmethod
    def _build_proxy_url(
        protocol: str,
        server_address: str,
        image_path: str | None,
        rating_key: int | None,
        width: int,
        height: int,
        fallback: str,
    ) -> str:
        if not image_path:
            return ''
        params = {
            'img': image_path,
            'width': width,
            'height': height,
            'fallback': fallback,
        }
        if rating_key:
            params['rating_key'] = rating_key
        return f"{protocol}://{server_address}/pms_image_proxy?{urlencode(params)}"

    def _build_plays_by_year_chart(self, plays: list[ViewingHistory], title: str) -> dict[str, Any]:
        configured_servers = (
            ServerConfig.query
            .filter_by(is_active=True)
            .order_by(ServerConfig.server_order)
            .all()
        )
        configured_names = [server.name for server in configured_servers if server.name]

        counts: dict[int, dict[str, int]] = {}
        for item in plays:
            year = None
            if item.date_played:
                year = item.date_played.year
            elif item.started:
                try:
                    year = (
                        datetime.fromtimestamp(item.started, tz=timezone.utc)
                        .astimezone(self.local_tz)
                        .year
                    )
                except (ValueError, OSError, TypeError):
                    year = None

            if year is not None:
                server_name = item.server_name or 'Unknown'
                if year not in counts:
                    counts[year] = {}
                counts[year][server_name] = counts[year].get(server_name, 0) + 1

        years = sorted(counts.keys())
        discovered_names = sorted({
            server_name
            for server_counts in counts.values()
            for server_name in server_counts.keys()
        })

        # MultiPlex supports up to 2 servers; keep chart components to Server A/B.
        if configured_names:
            ordered_names = configured_names[:2]
        else:
            ordered_names = discovered_names[:2]

        if not ordered_names:
            ordered_names = ['Server A', 'Server B']

        default_colors = ['#E6B413', '#e36414', '#7cb5ec', '#90ed7d']
        series = []
        for index, name in enumerate(ordered_names):
            series.append({
                'name': name,
                'data': [counts.get(year, {}).get(name, 0) for year in years],
                'color': default_colors[index % len(default_colors)],
            })

        totals = []
        for index, _year in enumerate(years):
            year_total = sum(series_item['data'][index] for series_item in series)
            totals.append(year_total)

        return {
            'categories': [str(year) for year in years],
            'series': series,
            'totals': totals,
            'overall_total': sum(totals),
            'title': f'Plays by Year - {title}',
        }

    def _build_plays_by_user_chart(self, plays: list[ViewingHistory], title: str) -> dict[str, Any]:
        counts: dict[str, int] = {}
        display_names: dict[str, str] = {}

        for item in plays:
            user_name = (item.user or '').strip()
            if not user_name:
                continue

            key = user_name.lower()
            counts[key] = counts.get(key, 0) + 1
            if key not in display_names:
                display_names[key] = user_name

        sorted_users = sorted(
            counts.items(),
            key=lambda pair: (-pair[1], display_names[pair[0]].lower()),
        )

        categories = [display_names[key] for key, _ in sorted_users]
        raw_counts = [count for _, count in sorted_users]
        max_count = max(raw_counts) if raw_counts else 1
        min_count = min(raw_counts) if raw_counts else 0

        data = []
        for count in raw_counts:
            ratio = (count - min_count) / (max_count - min_count) if max_count > min_count else 0
            data.append({
                'y': int(count),
                'color': self._interpolate_color('#ff9800', '#ed542c', ratio),
            })

        return {
            'categories': categories,
            'data': data,
            'overall_total': sum(raw_counts),
            'title': f'Plays by User - {title}',
        }

    def _build_plays_by_user_chart_from_counts(
        self,
        user_play_counts: dict[str, int],
        user_display_names: dict[str, str],
        title: str,
    ) -> dict[str, Any]:
        sorted_users = sorted(
            user_play_counts.items(),
            key=lambda pair: (-pair[1], user_display_names.get(pair[0], pair[0]).lower()),
        )

        categories = [user_display_names.get(token, token) for token, _ in sorted_users]
        raw_counts = [count for _, count in sorted_users]
        max_count = max(raw_counts) if raw_counts else 1
        min_count = min(raw_counts) if raw_counts else 0

        data = []
        for count in raw_counts:
            ratio = (count - min_count) / (max_count - min_count) if max_count > min_count else 0
            data.append({
                'y': int(count),
                'color': self._interpolate_color('#ff9800', '#ed542c', ratio),
            })

        return {
            'categories': categories,
            'data': data,
            'overall_total': sum(raw_counts),
            'title': f'Plays by User - {title}',
        }

    @staticmethod
    def _interpolate_color(color1: str, color2: str, ratio: float) -> str:
        def hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
            hex_color = hex_color.lstrip('#')
            return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))

        def rgb_to_hex(rgb: tuple[int, int, int]) -> str:
            return '#{:02x}{:02x}{:02x}'.format(*rgb)

        r1, g1, b1 = hex_to_rgb(color1)
        r2, g2, b2 = hex_to_rgb(color2)

        r = int(r1 + (r2 - r1) * ratio)
        g = int(g1 + (g2 - g1) * ratio)
        b = int(b1 + (b2 - b1) * ratio)

        return rgb_to_hex((r, g, b))

    def _format_watch_history_row(
        self,
        item: ViewingHistory,
        content_title: str,
    ) -> dict[str, Any]:
        date_str = ''
        time_str = ''
        sortable_datetime = ''
        if item.started:
            try:
                local_dt = datetime.fromtimestamp(item.started, tz=timezone.utc).astimezone(self.local_tz)
                date_str = local_dt.strftime('%Y-%m-%d')
                time_str = local_dt.strftime('%-I:%M%p').lower()
                sortable_datetime = local_dt.strftime('%Y-%m-%d %H:%M')
            except (ValueError, OSError, TypeError):
                date_str = ''
                time_str = ''
                sortable_datetime = ''

        quality = ''
        if item.transcode_decision:
            decision = item.transcode_decision.lower()
            if decision == 'direct play':
                quality = 'Direct Play'
            elif decision == 'copy':
                quality = 'Direct Stream'
            elif decision == 'transcode':
                quality = 'Transcode'
            else:
                quality = item.transcode_decision.title()

        title = item.full_title or item.title or ''
        subtitle = ''
        media_type = item.media_type or ''
        media_type_lower = media_type.lower()

        if media_type_lower in ['tv', 'episode', 'show']:
            if item.grandparent_title:
                title = item.grandparent_title
            season = item.parent_media_index
            episode = item.media_index
            if season not in (None, '') and episode not in (None, ''):
                try:
                    subtitle = f"S{int(season):02d}E{int(episode):02d}"
                    if item.title and item.title.lower() != content_title.lower():
                        subtitle += f" - {item.title}"
                except (ValueError, TypeError):
                    subtitle = ''
        else:
            if item.year:
                subtitle = f"({item.year})"

        server_order_class = ''
        if item.server_order == 0:
            server_order_class = 'server-a'
        elif item.server_order == 1:
            server_order_class = 'server-b'

        return {
            'date_pt': date_str,
            'time_pt': time_str,
            'sortable_datetime': sortable_datetime,
            'Server': item.server_name or '',
            'server_order': server_order_class,
            'user': item.user or '',
            'ip_address': item.ip_address or '',
            'media_type': media_type,
            'title': title,
            'subtitle': subtitle,
            'platform': item.platform or '',
            'product': item.product or '',
            'quality': quality,
            'percent_complete': item.percent_complete or 0,
        }

    def _get_lifetime_content_stats(
        self,
        record: ViewingHistory | None,
        plays: list[ViewingHistory],
        is_movie: bool,
        content_title: str,
        content_year: int | None = None,
        fallback_total_plays: int | None = None,
    ) -> dict[str, Any]:
        # Content detail should stay fast: prefer local lifetime/cache data and
        # avoid blocking per-request Tautulli lifetime stat calls.
        local_total_plays = len(plays)
        local_unique_users = len({(item.user or '').strip().lower() for item in plays if item.user})
        lifetime_total = self._lookup_local_lifetime_total(
            is_movie=is_movie,
            content_title=content_title,
            content_year=content_year,
        )

        fallback_total = to_int(fallback_total_plays) or 0
        total_plays = lifetime_total
        if total_plays is None:
            if local_total_plays > 0:
                total_plays = local_total_plays
            else:
                total_plays = fallback_total
        unique_users = local_unique_users

        return {
            'total_plays': total_plays,
            'unique_users': unique_users,
            'user_play_counts': {},
            'user_display_names': {},
        }

    def _lookup_local_lifetime_total(
        self,
        is_movie: bool,
        content_title: str,
        content_year: int | None,
    ) -> int | None:
        normalized_title = normalize_title(content_title)
        if not normalized_title:
            return None

        if is_movie:
            query = LifetimeMediaPlayCount.query.filter_by(
                media_type='movie',
                title_normalized=normalized_title,
            )
            if content_year is not None:
                exact_row = query.filter(LifetimeMediaPlayCount.year == content_year).first()
                if exact_row:
                    return int(exact_row.total_plays or 0)

            rows = query.all()
            if not rows:
                return None

            # For yearless lookup, sum all matching movie variants.
            if content_year is None:
                return sum(int(row.total_plays or 0) for row in rows)

            # If only one variant exists, treat it as the best local lifetime match.
            if len(rows) == 1:
                return int(rows[0].total_plays or 0)
            return None

        show_row = LifetimeMediaPlayCount.query.filter_by(
            media_type='show',
            title_normalized=normalized_title,
        ).first()
        if not show_row:
            return None
        return int(show_row.total_plays or 0)

    def _collect_server_lifetime_stats(
        self,
        server: ServerConfig,
        rating_keys: set[int],
        item_media_type: str,
    ) -> dict[str, Any] | None:
        """
        Collect lifetime play stats for one server.

        Calls remain sequential per rating key; callers can execute this method
        in parallel across servers.
        """
        endpoint_total_plays = 0
        endpoint_play_sources = 0
        user_key_sources = 0
        user_play_counts: dict[str, int] = {}
        user_display_names: dict[str, str] = {}

        try:
            client = TautulliClient(server.to_multiplex_config())
            for rating_key in sorted(rating_keys):
                total_plays = self._fetch_watch_total_plays(client, int(rating_key), item_media_type)
                user_stats = self._fetch_item_user_stats(client, int(rating_key), item_media_type)

                if total_plays is not None:
                    endpoint_total_plays += total_plays
                    endpoint_play_sources += 1
                elif user_stats is not None:
                    endpoint_total_plays += user_stats['total_plays']
                    endpoint_play_sources += 1

                if user_stats is not None:
                    for token, count in user_stats['play_counts'].items():
                        user_play_counts[token] = user_play_counts.get(token, 0) + count
                    for token, display_name in user_stats['display_names'].items():
                        if token not in user_display_names and display_name:
                            user_display_names[token] = display_name
                    user_key_sources += 1
        except Exception:
            return None

        return {
            'endpoint_total_plays': endpoint_total_plays,
            'endpoint_play_sources': endpoint_play_sources,
            'user_key_sources': user_key_sources,
            'user_play_counts': user_play_counts,
            'user_display_names': user_display_names,
        }

    def _fetch_watch_total_plays(
        self,
        client: TautulliClient,
        rating_key: int,
        item_media_type: str,
    ) -> int | None:
        attempts = [
            {'query_days': 0},
            {'media_type': item_media_type, 'query_days': 0},
        ]

        for params in attempts:
            try:
                response = client.get_item_watch_time_stats(rating_key, **params)
            except Exception:
                continue

            if not self._is_tautulli_success_response(response):
                continue

            total_plays = self._extract_watch_stats_total_plays(response)
            if total_plays is not None:
                return total_plays

        return None

    def _fetch_item_user_stats(
        self,
        client: TautulliClient,
        rating_key: int,
        item_media_type: str,
    ) -> dict[str, Any] | None:
        attempts = [
            {},
            {'media_type': item_media_type},
        ]

        for params in attempts:
            try:
                response = client.get_item_user_stats(rating_key, **params)
            except Exception:
                continue

            if not self._is_tautulli_success_response(response):
                continue

            return {
                'tokens': self._extract_item_user_tokens(response),
                'total_plays': self._extract_item_user_total_plays(response),
                'play_counts': self._extract_item_user_play_counts(response),
                'display_names': self._extract_item_user_display_names(response),
            }

        return None

    @staticmethod
    def _is_tautulli_success_response(response: dict[str, Any]) -> bool:
        envelope = response.get('response', {})
        if not isinstance(envelope, dict):
            return False

        result = envelope.get('result')
        if result is None:
            return True
        return str(result).strip().lower() == 'success'

    def _resolve_server_rating_keys(
        self,
        record: ViewingHistory | None,
        plays: list[ViewingHistory],
        is_movie: bool,
        content_title: str,
        content_year: int | None,
        active_servers: list[ServerConfig],
    ) -> dict[str, set[int]]:
        keys_by_server: dict[str, set[int]] = {}

        for item in plays:
            server_name = (item.server_name or '').strip()
            if not server_name:
                continue

            rating_key = item.rating_key if is_movie else (item.grandparent_rating_key or item.rating_key)
            parsed_key = to_int(rating_key)
            if parsed_key is not None:
                keys_by_server.setdefault(server_name, set()).add(parsed_key)

        if record:
            source_server_name = (record.server_name or '').strip()
            if source_server_name:
                source_rating_key = record.rating_key if is_movie else (record.grandparent_rating_key or record.rating_key)
                parsed_source_key = to_int(source_rating_key)
                if parsed_source_key is not None:
                    keys_by_server.setdefault(source_server_name, set()).add(parsed_source_key)

        for server in active_servers:
            if not server.name:
                continue
            discovered_keys = self._discover_server_content_rating_keys(
                server=server,
                content_title=content_title,
                is_movie=is_movie,
                content_year=content_year if is_movie else None,
            )
            if discovered_keys:
                keys_by_server.setdefault(server.name, set()).update(discovered_keys)

        return {
            server_name: keys
            for server_name, keys in keys_by_server.items()
            if keys
        }

    def _discover_server_content_rating_keys(
        self,
        server: ServerConfig,
        content_title: str,
        is_movie: bool,
        content_year: int | None,
    ) -> set[int]:
        normalized_title = normalize_title(content_title)
        if not normalized_title:
            return set()

        try:
            client = TautulliClient(server.to_multiplex_config())
        except Exception:
            return set()

        page_size = 1000
        max_pages = 250
        start = 0
        pages = 0
        discovered: set[int] = set()

        while pages < max_pages:
            try:
                response = client.get_history_paginated(
                    start=start,
                    length=page_size,
                    search=content_title,
                )
            except Exception:
                break

            rows, records_filtered = self._extract_history_page(response)
            if rows is None:
                break

            for row in rows:
                key = self._extract_matching_rating_key(
                    row=row,
                    normalized_title=normalized_title,
                    is_movie=is_movie,
                    content_year=content_year,
                )
                if key is not None:
                    discovered.add(key)

            pages += 1
            fetched = len(rows)
            start += fetched
            if fetched == 0:
                break
            if records_filtered is not None and start >= records_filtered:
                break

        return discovered

    @staticmethod
    def _extract_history_page(response: dict[str, Any]) -> tuple[list[dict[str, Any]] | None, int | None]:
        if not isinstance(response, dict):
            return None, None

        envelope = response.get('response', {})
        if not isinstance(envelope, dict):
            return None, None

        data = envelope.get('data', {})
        if not isinstance(data, dict):
            return None, None

        rows = data.get('data', [])
        if not isinstance(rows, list):
            rows = []

        parsed_rows = [row for row in rows if isinstance(row, dict)]
        records_filtered = to_int(data.get('recordsFiltered'))
        return parsed_rows, records_filtered

    def _extract_matching_rating_key(
        self,
        row: dict[str, Any],
        normalized_title: str,
        is_movie: bool,
        content_year: int | None,
    ) -> int | None:
        media_type = normalize_title(row.get('media_type'))

        if is_movie:
            if media_type != 'movie':
                return None

            row_title = normalize_title(row.get('title') or row.get('full_title'))
            if row_title != normalized_title:
                return None

            if content_year is not None:
                row_year = to_int(row.get('year'))
                if row_year is not None and row_year != content_year:
                    return None

            return to_int(row.get('rating_key'))

        if media_type not in {'episode', 'tv', 'show'}:
            return None

        show_title = normalize_title(row.get('grandparent_title'))
        if show_title != normalized_title:
            return None

        return to_int(row.get('grandparent_rating_key') or row.get('rating_key'))

    @staticmethod
    def _extract_watch_stats_total_plays(response: dict[str, Any]) -> int | None:
        data = response.get('response', {}).get('data', {})

        if isinstance(data, dict):
            if 'data' in data and isinstance(data.get('data'), list):
                data = data.get('data')
            else:
                return to_int(data.get('total_plays'))

        if not isinstance(data, list):
            return None

        zero_day_total: int | None = None
        highest_total: int | None = None

        for row in data:
            if not isinstance(row, dict):
                continue

            total_plays = to_int(row.get('total_plays'))
            if total_plays is None:
                continue

            if highest_total is None or total_plays > highest_total:
                highest_total = total_plays

            query_days = to_int(row.get('query_days'))
            if query_days == 0:
                zero_day_total = total_plays

        return zero_day_total if zero_day_total is not None else highest_total

    @staticmethod
    def _extract_item_user_tokens(response: dict[str, Any]) -> set[str]:
        rows = ContentService._extract_item_user_rows(response)
        tokens: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue

            total_plays = to_int(row.get('total_plays'))
            if total_plays is not None and total_plays <= 0:
                continue

            token = ContentService._build_user_token(row)
            if token:
                tokens.add(token)

        return tokens

    @staticmethod
    def _extract_item_user_total_plays(response: dict[str, Any]) -> int:
        rows = ContentService._extract_item_user_rows(response)
        total = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            value = to_int(row.get('total_plays'))
            if value is not None and value > 0:
                total += value
        return total

    @staticmethod
    def _extract_item_user_play_counts(response: dict[str, Any]) -> dict[str, int]:
        rows = ContentService._extract_item_user_rows(response)
        play_counts: dict[str, int] = {}

        for row in rows:
            if not isinstance(row, dict):
                continue

            value = to_int(row.get('total_plays'))
            if value is None or value <= 0:
                continue

            token = ContentService._build_user_token(row)
            if not token:
                continue

            play_counts[token] = play_counts.get(token, 0) + value

        return play_counts

    @staticmethod
    def _extract_item_user_display_names(response: dict[str, Any]) -> dict[str, str]:
        rows = ContentService._extract_item_user_rows(response)
        display_names: dict[str, str] = {}

        for row in rows:
            if not isinstance(row, dict):
                continue

            token = ContentService._build_user_token(row)
            if not token or token in display_names:
                continue

            display_name = (
                row.get('friendly_name')
                or row.get('username')
                or row.get('user')
                or row.get('email')
            )
            if display_name:
                display_names[token] = str(display_name)
            else:
                display_names[token] = token.replace('name:', '').replace('id:', '')

        return display_names

    @staticmethod
    def _extract_item_user_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
        data = response.get('response', {}).get('data', [])
        if isinstance(data, dict):
            if 'data' in data and isinstance(data.get('data'), list):
                rows = data.get('data', [])
            else:
                rows = [data]
        elif isinstance(data, list):
            rows = data
        else:
            rows = []
        return [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _build_user_token(row: dict[str, Any]) -> str:
        for key in ('friendly_name', 'username', 'user', 'email'):
            value = row.get(key)
            if value:
                return f"name:{str(value).strip().lower()}"

        user_id = row.get('user_id')
        parsed_user_id = to_int(user_id)
        if parsed_user_id is not None:
            return f"id:{parsed_user_id}"

        return ''

