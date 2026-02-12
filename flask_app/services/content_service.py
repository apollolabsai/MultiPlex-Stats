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

        watch_history = [self._format_watch_history_row(item, content_title) for item in plays]
        plays_chart = self._build_plays_by_year_chart(plays, details_title)
        lifetime_stats = self._get_lifetime_content_stats(
            record=record,
            plays=plays,
            is_movie=is_movie,
        )

        return {
            'content_kind': content_kind,
            'content_title': details_title,
            'is_movie': is_movie,
            'metadata': metadata,
            'plays_chart': plays_chart,
            'watch_history': watch_history,
            'total_plays': lifetime_stats['total_plays'],
            'unique_users': lifetime_stats['unique_users'],
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
        record: ViewingHistory,
        plays: list[ViewingHistory],
        is_movie: bool,
    ) -> dict[str, int]:
        local_total_plays = len(plays)
        local_unique_users = len({(item.user or '').strip().lower() for item in plays if item.user})

        server_rating_keys = self._resolve_server_rating_keys(
            record=record,
            plays=plays,
            is_movie=is_movie,
        )
        if not server_rating_keys:
            return {
                'total_plays': local_total_plays,
                'unique_users': local_unique_users,
            }

        active_servers = (
            ServerConfig.query
            .filter(
                ServerConfig.is_active.is_(True),
                ServerConfig.name.in_(list(server_rating_keys.keys()))
            )
            .all()
        )
        server_lookup = {server.name: server for server in active_servers}

        endpoint_total_plays = 0
        unique_user_tokens: set[str] = set()
        watch_servers_processed = 0
        user_servers_processed = 0
        item_media_type = 'movie' if is_movie else 'show'

        for server_name, rating_key in server_rating_keys.items():
            server = server_lookup.get(server_name)
            if not server:
                continue

            try:
                client = TautulliClient(server.to_multiplex_config())
                total_plays = self._fetch_watch_total_plays(client, int(rating_key), item_media_type)
                if total_plays is not None:
                    endpoint_total_plays += total_plays
                    watch_servers_processed += 1

                user_tokens = self._fetch_item_user_tokens(client, int(rating_key), item_media_type)
                if user_tokens is not None:
                    unique_user_tokens.update(user_tokens)
                    user_servers_processed += 1
            except Exception:
                continue

        total_plays = endpoint_total_plays if watch_servers_processed > 0 else local_total_plays
        unique_users = len(unique_user_tokens) if user_servers_processed > 0 else local_unique_users

        return {
            'total_plays': total_plays,
            'unique_users': unique_users,
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

    def _fetch_item_user_tokens(
        self,
        client: TautulliClient,
        rating_key: int,
        item_media_type: str,
    ) -> set[str] | None:
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

            return self._extract_item_user_tokens(response)

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

    @staticmethod
    def _resolve_server_rating_keys(
        record: ViewingHistory,
        plays: list[ViewingHistory],
        is_movie: bool,
    ) -> dict[str, int]:
        key_by_server: dict[str, int] = {}

        for item in plays:
            server_name = (item.server_name or '').strip()
            if not server_name or server_name in key_by_server:
                continue

            rating_key = item.rating_key if is_movie else (item.grandparent_rating_key or item.rating_key)
            parsed_key = ContentService._to_int(rating_key)
            if parsed_key is not None:
                key_by_server[server_name] = parsed_key

        source_server_name = (record.server_name or '').strip()
        if source_server_name and source_server_name not in key_by_server:
            source_rating_key = record.rating_key if is_movie else (record.grandparent_rating_key or record.rating_key)
            parsed_source_key = ContentService._to_int(source_rating_key)
            if parsed_source_key is not None:
                key_by_server[source_server_name] = parsed_source_key

        return key_by_server

    @staticmethod
    def _extract_watch_stats_total_plays(response: dict[str, Any]) -> int | None:
        data = response.get('response', {}).get('data', {})

        if isinstance(data, dict):
            if 'data' in data and isinstance(data.get('data'), list):
                data = data.get('data')
            else:
                return ContentService._to_int(data.get('total_plays'))

        if not isinstance(data, list):
            return None

        zero_day_total: int | None = None
        highest_total: int | None = None

        for row in data:
            if not isinstance(row, dict):
                continue

            total_plays = ContentService._to_int(row.get('total_plays'))
            if total_plays is None:
                continue

            if highest_total is None or total_plays > highest_total:
                highest_total = total_plays

            query_days = ContentService._to_int(row.get('query_days'))
            if query_days == 0:
                zero_day_total = total_plays

        return zero_day_total if zero_day_total is not None else highest_total

    @staticmethod
    def _extract_item_user_tokens(response: dict[str, Any]) -> set[str]:
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

        tokens: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue

            total_plays = ContentService._to_int(row.get('total_plays'))
            if total_plays is not None and total_plays <= 0:
                continue

            token = ContentService._build_user_token(row)
            if token:
                tokens.add(token)

        return tokens

    @staticmethod
    def _build_user_token(row: dict[str, Any]) -> str:
        for key in ('friendly_name', 'username', 'user', 'email'):
            value = row.get(key)
            if value:
                return f"name:{str(value).strip().lower()}"

        user_id = row.get('user_id')
        parsed_user_id = ContentService._to_int(user_id)
        if parsed_user_id is not None:
            return f"id:{parsed_user_id}"

        return ''

    @staticmethod
    def _to_int(value: Any) -> int | None:
        if value in (None, ''):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
