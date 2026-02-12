"""
Service for content detail pages linked from viewing history.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from sqlalchemy import func, or_

from flask_app.models import ServerConfig, ViewingHistory
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

        watch_history = [self._format_watch_history_row(item, is_movie, content_title) for item in plays]
        plays_chart = self._build_plays_by_year_chart(plays, details_title)

        unique_users = len({(item.user or '').strip().lower() for item in plays if item.user})

        return {
            'content_kind': content_kind,
            'content_title': details_title,
            'is_movie': is_movie,
            'metadata': metadata,
            'plays_chart': plays_chart,
            'watch_history': watch_history,
            'total_plays': len(plays),
            'unique_users': unique_users,
            'source_record_id': record.id,
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
            title_filters.append(func.lower(ViewingHistory.title) == content_title.lower())

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
            'runtime': self._format_runtime(record.duration),
            'rated': '',
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
        metadata['runtime'] = self._format_runtime(data.get('duration') or data.get('duration_ms') or record.duration)
        metadata['rated'] = data.get('content_rating') or data.get('rating') or ''

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
        counts: dict[int, int] = {}
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
                counts[year] = counts.get(year, 0) + 1

        years = sorted(counts.keys())
        return {
            'categories': [str(year) for year in years],
            'series': [counts[year] for year in years],
            'title': f'Plays by Year - {title}',
        }

    def _format_watch_history_row(
        self,
        item: ViewingHistory,
        is_movie: bool,
        content_title: str,
    ) -> dict[str, Any]:
        played_at = ''
        if item.started:
            try:
                played_at = (
                    datetime.fromtimestamp(item.started, tz=timezone.utc)
                    .astimezone(self.local_tz)
                    .strftime('%Y-%m-%d %I:%M %p')
                )
            except (ValueError, OSError, TypeError):
                played_at = ''

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

        detail_label = ''
        if not is_movie:
            season = item.parent_media_index
            episode = item.media_index
            if season not in (None, '') and episode not in (None, ''):
                try:
                    detail_label = f"S{int(season):02d}E{int(episode):02d}"
                except (ValueError, TypeError):
                    detail_label = ''

            episode_title = (item.title or '').strip()
            if episode_title and episode_title.lower() != content_title.lower():
                detail_label = f"{detail_label} - {episode_title}" if detail_label else episode_title

        platform = item.platform or ''
        if item.product:
            platform = f"{platform} / {item.product}" if platform else item.product

        return {
            'played_at': played_at,
            'server': item.server_name or '',
            'user': item.user or '',
            'detail': detail_label,
            'platform': platform,
            'quality': quality,
            'progress': item.percent_complete or 0,
        }
