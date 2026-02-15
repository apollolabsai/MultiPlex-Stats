"""
Analytics service that bridges Flask to the existing multiplex_stats package.
"""
import os
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode
import pandas as pd
from sqlalchemy import func, or_

from multiplex_stats import TautulliClient
from multiplex_stats.data_processing import (
    process_daily_data, process_monthly_data, process_history_data,
    aggregate_movie_stats, aggregate_tv_stats, filter_history_by_date
)
from multiplex_stats.timezone_utils import get_local_timezone
from multiplex_stats.visualization import (
    get_daily_chart_data, get_monthly_chart_data,
    get_user_chart_data, get_movie_chart_data, get_tv_chart_data,
    get_category_pie_data, get_server_pie_data, get_platform_pie_data,
    get_concurrent_streams_chart_data
)
from flask_app.services.config_service import ConfigService
from flask_app.services.history_sync_service import HistorySyncService
from flask_app.services.utils import normalize_title, to_int
from flask_app.models import CachedMedia, LifetimeMediaPlayCount, ServerConfig, ViewingHistory


class AnalyticsService:
    """Service to execute analytics pipeline using database configuration."""

    def __init__(self):
        self.cache_dir = os.path.join('instance', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)

    def run_full_analytics(self, run_id: int, daily_trend_days_override: int | None = None) -> Dict[str, Any]:
        """
        Execute full analytics pipeline using database config.

        This method executes the full analytics pipeline but:
        1. Loads config from database instead of config.ini
        2. Caches results for display in the web UI

        Args:
            run_id: Database ID of AnalyticsRun record

        Returns:
            Dictionary with summary stats and chart paths
        """
        # 1. Load configuration from database
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()
        daily_trend_days = daily_trend_days_override or settings.daily_trend_days

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        # 2. Create API clients
        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        # 3. Fetch and process data
        # Daily data (for daily chart)
        daily_data_a = client_a.get_plays_by_date(time_range=daily_trend_days)
        daily_data_b = client_b.get_plays_by_date(time_range=daily_trend_days) if client_b else None
        df_daily = process_daily_data(
            daily_data_a, daily_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        # Monthly data
        monthly_data_a = client_a.get_plays_per_month(time_range=settings.monthly_trend_months)
        monthly_data_b = client_b.get_plays_per_month(time_range=settings.monthly_trend_months) if client_b else None
        df_monthly = process_monthly_data(
            monthly_data_a, monthly_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        # History data - fetch from API for charts (uses history_days setting)
        # The Viewing History table uses the local database instead
        history_data_a = client_a.get_history(days=settings.history_days)
        history_data_b = client_b.get_history(days=settings.history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        # Run incremental sync for the viewing history table (if data exists)
        sync_service = HistorySyncService()
        if sync_service.has_history_data():
            sync_service.start_incremental_sync()

        # Aggregate stats (using filtered data)
        df_movies = aggregate_movie_stats(df_history, top_n=settings.top_movies)
        df_tv = aggregate_tv_stats(df_history, top_n=settings.top_tv_shows)

        # 4. Generate chart data (JSON for Highcharts)
        # Distribution charts (use history_days range)
        dist_daily_a = client_a.get_plays_by_date(time_range=settings.history_days)
        dist_daily_b = client_b.get_plays_by_date(time_range=settings.history_days) if client_b else None
        df_daily_dist = process_daily_data(
            dist_daily_a, dist_daily_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        movie_chart = get_movie_chart_data(df_movies, settings.history_days)
        movie_chart['poster_cards'] = self._build_movie_poster_cards(df_movies)

        tv_chart = get_tv_chart_data(df_tv, settings.history_days)
        tv_chart['poster_cards'] = self._build_tv_poster_cards(df_tv)

        charts_json = {
            'daily': get_daily_chart_data(df_daily, server_a_config.name, server_b_config.name if server_b_config else None),
            'monthly': get_monthly_chart_data(df_monthly, server_a_config.name, server_b_config.name if server_b_config else None),
            'users': get_user_chart_data(
                df_history,
                server_a_config.name,
                server_b_config.name if server_b_config else None,
                settings.history_days,
                top_n=settings.top_users
            ),
            'movies': movie_chart,
            'tv': tv_chart,
            'category': get_category_pie_data(df_daily_dist, settings.history_days),
            'server': get_server_pie_data(
                df_daily_dist, server_a_config.name,
                server_b_config.name if server_b_config else None,
                settings.history_days
            ),
            'platform': get_platform_pie_data(df_history, settings.history_days)
        }

        # 5. Cache chart JSON to disk
        cache_path = os.path.join(self.cache_dir, f'run_{run_id}_charts.json')
        with open(cache_path, 'w') as f:
            json.dump(charts_json, f)

        # 6b. Table data now comes from ViewingHistory database table (no caching needed)

        # 7. Calculate summary statistics
        total_plays = len(df_history)
        total_users = df_history['user'].nunique()  # Count distinct users from all history
        total_movies = len(df_movies)
        total_tv = len(df_tv)
        server_a_plays = len(df_history[df_history['Server'] == server_a_config.name])
        server_b_plays = len(df_history[df_history['Server'] == server_b_config.name]) if server_b_config else 0

        summary = {
            'total_plays': total_plays,
            'total_users': total_users,
            'total_movies': total_movies,
            'total_tv': total_tv,
            'server_a_name': server_a_config.name,
            'server_a_plays': server_a_plays,
            'server_b_name': server_b_config.name if server_b_config else None,
            'server_b_plays': server_b_plays,
            'daily_trend_days': daily_trend_days,
            'monthly_trend_months': settings.monthly_trend_months,
            'distribution_days': settings.history_days,
            'user_chart_days': settings.history_days,
            'movie_chart_days': settings.history_days,
            'tv_chart_days': settings.history_days,
            'history_days': settings.history_days,
            'generated_at': datetime.now(get_local_timezone()).isoformat()
        }

        return {
            'total_plays': total_plays,
            'total_users': total_users,
            'summary': summary
        }

    def get_daily_chart_json(
        self,
        daily_trend_days: int | None = None,
        user_id: int | None = None
    ) -> Dict[str, Any]:
        """
        Generate the daily play count chart JSON for Highcharts.

        Args:
            daily_trend_days: Optional override for daily trend range.
            user_id: Optional user ID to filter results for a specific user.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        daily_days = daily_trend_days or settings.daily_trend_days

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        daily_data_a = client_a.get_plays_by_date(time_range=daily_days, user_id=user_id)
        daily_data_b = client_b.get_plays_by_date(time_range=daily_days, user_id=user_id) if client_b else None
        df_daily = process_daily_data(
            daily_data_a, daily_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        chart_data = get_daily_chart_data(
            df_daily, server_a_config.name, server_b_config.name if server_b_config else None
        )

        return {
            'chart_data': chart_data,
            'daily_trend_days': daily_days,
            'user_id': user_id
        }

    def get_monthly_chart_json(
        self,
        monthly_trend_months: int | None = None,
        user_id: int | None = None
    ) -> Dict[str, Any]:
        """
        Generate the monthly play count chart JSON for Highcharts.

        Args:
            monthly_trend_months: Optional override for monthly trend range.
            user_id: Optional user ID to filter results for a specific user.

        Returns:
            Dictionary with chart data and the month range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        monthly_months = monthly_trend_months or settings.monthly_trend_months

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        monthly_data_a = client_a.get_plays_per_month(time_range=monthly_months, user_id=user_id)
        monthly_data_b = client_b.get_plays_per_month(time_range=monthly_months, user_id=user_id) if client_b else None
        df_monthly = process_monthly_data(
            monthly_data_a, monthly_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        chart_data = get_monthly_chart_data(
            df_monthly, server_a_config.name, server_b_config.name if server_b_config else None
        )

        return {
            'chart_data': chart_data,
            'monthly_trend_months': monthly_months,
            'user_id': user_id
        }

    def get_distribution_charts_json(self, days: int | None = None) -> Dict[str, Any]:
        """
        Generate distribution charts JSON (category, server, platform) for Highcharts.

        Args:
            days: Optional override for distribution range.

        Returns:
            Dictionary with chart data for distribution and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        dist_days = days or settings.history_days

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        dist_daily_a = client_a.get_plays_by_date(time_range=dist_days)
        dist_daily_b = client_b.get_plays_by_date(time_range=dist_days) if client_b else None
        df_daily_dist = process_daily_data(
            dist_daily_a, dist_daily_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        history_data_a = client_a.get_history(days=dist_days)
        history_data_b = client_b.get_history(days=dist_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        return {
            'category': get_category_pie_data(df_daily_dist, dist_days),
            'server': get_server_pie_data(
                df_daily_dist, server_a_config.name,
                server_b_config.name if server_b_config else None,
                dist_days
            ),
            'platform': get_platform_pie_data(df_history, dist_days),
            'distribution_days': dist_days
        }

    def get_user_chart_json(self, days: int | None = None, top_n: int | None = None) -> Dict[str, Any]:
        """
        Generate the user activity chart JSON for Highcharts.

        Args:
            days: Optional override for user chart range.
            top_n: Optional override for number of users to show.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        history_days = days or settings.history_days
        user_count = top_n or settings.top_users

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        history_data_a = client_a.get_history(days=history_days)
        history_data_b = client_b.get_history(days=history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        chart_data = get_user_chart_data(
            df_history,
            server_a_config.name,
            server_b_config.name if server_b_config else None,
            history_days,
            top_n=user_count
        )

        return {
            'chart_data': chart_data,
            'user_chart_days': history_days,
            'top_users': user_count
        }

    def get_movie_chart_json(self, days: int | None = None, top_n: int | None = None) -> Dict[str, Any]:
        """
        Generate the top movies chart JSON for Highcharts.

        Args:
            days: Optional override for movie chart range.
            top_n: Optional override for number of movies to show.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        history_days = days or settings.history_days
        movie_count = top_n or settings.top_movies

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        history_data_a = client_a.get_history(days=history_days)
        history_data_b = client_b.get_history(days=history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        df_movies = aggregate_movie_stats(df_history, top_n=movie_count)
        chart_data = get_movie_chart_data(df_movies, history_days)
        movie_poster_cards = self._build_movie_poster_cards(df_movies)

        return {
            'chart_data': chart_data,
            'movie_chart_days': history_days,
            'top_movies': movie_count,
            'movie_poster_cards': movie_poster_cards,
        }

    def get_tv_chart_json(self, days: int | None = None, top_n: int | None = None) -> Dict[str, Any]:
        """
        Generate the top TV shows chart JSON for Highcharts.

        Args:
            days: Optional override for TV chart range.
            top_n: Optional override for number of TV shows to show.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        history_days = days or settings.history_days
        tv_count = top_n or settings.top_tv_shows

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        history_data_a = client_a.get_history(days=history_days)
        history_data_b = client_b.get_history(days=history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        df_tv = aggregate_tv_stats(df_history, top_n=tv_count)
        chart_data = get_tv_chart_data(df_tv, history_days)
        tv_poster_cards = self._build_tv_poster_cards(df_tv)

        return {
            'chart_data': chart_data,
            'tv_chart_days': history_days,
            'top_tv_shows': tv_count,
            'tv_poster_cards': tv_poster_cards,
        }

    def get_concurrent_streams_json(self, days: int | None = None) -> Dict[str, Any]:
        """
        Generate the concurrent streams area chart JSON for Highcharts.

        Args:
            days: Optional override for day range (default 60).

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        stream_days = days or 60  # Default to 60 days

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        streams_data_a = client_a.get_concurrent_streams_by_stream_type(time_range=stream_days)
        streams_data_b = client_b.get_concurrent_streams_by_stream_type(time_range=stream_days) if client_b else None

        chart_data = get_concurrent_streams_chart_data(
            streams_data_a, streams_data_b,
            server_a_config.name,
            server_b_config.name if server_b_config else None,
            stream_days
        )

        return {
            'chart_data': chart_data,
            'concurrent_streams_days': stream_days
        }

    def _get_user_thumb_map(self) -> dict:
        """
        Get mapping of user_id to user_thumb (avatar URL) from all servers.

        Returns:
            Dictionary mapping user_id to user_thumb URL
        """
        from multiplex_stats import TautulliClient

        user_thumb_map = {}

        # Load server configurations
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            return user_thumb_map

        # Fetch users from Server A
        try:
            client_a = TautulliClient(server_a_config)
            users_response = client_a.get_users()

            if users_response and 'response' in users_response and 'data' in users_response['response']:
                users = users_response['response']['data']
                for user in users:
                    user_id = user.get('user_id')
                    user_thumb = user.get('user_thumb', '')
                    if user_id and user_thumb:
                        # Build full URL for user thumbnail using pms_image_proxy
                        thumb_url = f"{server_a_config.ip_address}/pms_image_proxy?img={user_thumb}&width=40&height=40&fallback=poster"
                        user_thumb_map[str(user_id)] = thumb_url
        except Exception as e:
            print(f"Error fetching users from {server_a_config.name}: {e}")

        # Fetch users from Server B if configured
        if server_b_config:
            try:
                client_b = TautulliClient(server_b_config)
                users_response = client_b.get_users()

                if users_response and 'response' in users_response and 'data' in users_response['response']:
                    users = users_response['response']['data']
                    for user in users:
                        user_id = user.get('user_id')
                        user_thumb = user.get('user_thumb', '')
                        if user_id and user_thumb:
                            # Build full URL for user thumbnail using pms_image_proxy
                            thumb_url = f"{server_b_config.ip_address}/pms_image_proxy?img={user_thumb}&width=40&height=40&fallback=poster"
                            user_thumb_map[str(user_id)] = thumb_url
            except Exception as e:
                print(f"Error fetching users from {server_b_config.name}: {e}")

        return user_thumb_map

    def get_recent_unique_history_posters(
        self,
        limit: int = 20,
        max_scan: int = 1200,
    ) -> List[Dict[str, str]]:
        """
        Get poster URLs for the most recent unique titles in viewing history.

        Uniqueness is by show title for TV episodes and by title/year for movies.
        """
        if limit < 1:
            return []

        servers = ServerConfig.query.filter(ServerConfig.is_active.is_(True)).all()
        server_map = {
            (server.name or '').strip(): server
            for server in servers
            if server.name and server.ip_address
        }
        if not server_map:
            return []

        records = (
            ViewingHistory.query
            .order_by(ViewingHistory.started.desc(), ViewingHistory.id.desc())
            .limit(max_scan)
            .all()
        )

        seen_keys: set[tuple[Any, ...]] = set()
        posters: List[Dict[str, str]] = []

        for record in records:
            thumb = (record.thumb or '').strip()
            if not thumb:
                continue

            media_type = (record.media_type or '').strip().lower()
            is_show = media_type in {'episode', 'tv', 'show'}

            if is_show:
                title = (record.grandparent_title or record.title or record.full_title or '').strip()
                normalized = normalize_title(title)
                if not normalized:
                    continue
                unique_key = ('show', normalized)
                rating_key = to_int(record.grandparent_rating_key) or to_int(record.rating_key)
            else:
                title = (record.title or record.full_title or '').strip()
                normalized = normalize_title(title)
                if not normalized:
                    continue
                unique_key = ('movie', normalized, to_int(record.year))
                rating_key = to_int(record.rating_key)

            if unique_key in seen_keys:
                continue

            server = server_map.get((record.server_name or '').strip())
            if not server:
                continue

            protocol = 'https' if server.use_ssl else 'http'
            params: dict[str, Any] = {
                'img': thumb,
                'width': 220,
                'height': 330,
                'fallback': 'poster',
            }
            if rating_key is not None:
                params['rating_key'] = rating_key

            poster_url = f"{protocol}://{server.ip_address}/pms_image_proxy?{urlencode(params)}"
            posters.append({
                'title': title,
                'poster_url': poster_url,
            })
            seen_keys.add(unique_key)

            if len(posters) >= limit:
                break

        return posters

    def get_top_media_posters_by_play_count(
        self,
        limit: int = 80,
        ranking_pool: int = 400,
        max_scan: int = 20000,
    ) -> List[Dict[str, str]]:
        """
        Get poster URLs for top-played media titles.

        Ranking prefers lifetime merged play counts when available, then falls
        back to cached media play_count.
        """
        if limit < 1:
            return []

        ranking_pool = max(limit, ranking_pool)

        servers = ServerConfig.query.filter(ServerConfig.is_active.is_(True)).all()
        server_map = {
            (server.name or '').strip(): server
            for server in servers
            if server.name and server.ip_address
        }
        if not server_map:
            return []

        ranked_keys: list[tuple[Any, ...]] = []
        seen_keys: set[tuple[Any, ...]] = set()

        lifetime_rows = (
            LifetimeMediaPlayCount.query
            .filter(LifetimeMediaPlayCount.total_plays > 0)
            .order_by(LifetimeMediaPlayCount.total_plays.desc(), LifetimeMediaPlayCount.id.asc())
            .limit(ranking_pool)
            .all()
        )
        for row in lifetime_rows:
            media_type = (row.media_type or '').strip().lower()
            title_normalized = normalize_title(row.title_normalized)
            if not title_normalized:
                continue

            if media_type == 'show':
                key: tuple[Any, ...] = ('show', title_normalized)
            elif media_type == 'movie':
                key = ('movie', title_normalized, to_int(row.year))
            else:
                continue

            if key in seen_keys:
                continue
            ranked_keys.append(key)
            seen_keys.add(key)

        if len(ranked_keys) < ranking_pool:
            cached_rows = (
                CachedMedia.query
                .filter(CachedMedia.play_count > 0)
                .order_by(CachedMedia.play_count.desc(), CachedMedia.id.asc())
                .limit(ranking_pool)
                .all()
            )
            for row in cached_rows:
                media_type = (row.media_type or '').strip().lower()
                title_normalized = normalize_title(row.title)
                if not title_normalized:
                    continue

                if media_type == 'show':
                    key = ('show', title_normalized)
                elif media_type == 'movie':
                    key = ('movie', title_normalized, to_int(row.year))
                else:
                    continue

                if key in seen_keys:
                    continue
                ranked_keys.append(key)
                seen_keys.add(key)
                if len(ranked_keys) >= ranking_pool:
                    break

        if not ranked_keys:
            return []

        target_keys = set(ranked_keys)
        resolved_by_key: dict[tuple[Any, ...], Dict[str, str]] = {}

        records = (
            ViewingHistory.query
            .order_by(ViewingHistory.started.desc(), ViewingHistory.id.desc())
            .limit(max_scan)
            .all()
        )

        for record in records:
            if len(resolved_by_key) >= len(target_keys):
                break

            thumb = (record.thumb or '').strip()
            if not thumb:
                continue

            media_type = (record.media_type or '').strip().lower()
            is_show = media_type in {'episode', 'tv', 'show'}

            if is_show:
                title = (record.grandparent_title or record.title or record.full_title or '').strip()
                normalized = normalize_title(title)
                if not normalized:
                    continue
                key = ('show', normalized)
                rating_key = to_int(record.grandparent_rating_key) or to_int(record.rating_key)
            else:
                title = (record.title or record.full_title or '').strip()
                normalized = normalize_title(title)
                if not normalized:
                    continue
                key = ('movie', normalized, to_int(record.year))
                rating_key = to_int(record.rating_key)

            if key not in target_keys or key in resolved_by_key:
                continue

            server = server_map.get((record.server_name or '').strip())
            if not server:
                continue

            protocol = 'https' if server.use_ssl else 'http'
            params: dict[str, Any] = {
                'img': thumb,
                'width': 220,
                'height': 330,
                'fallback': 'poster',
            }
            if rating_key is not None:
                params['rating_key'] = rating_key

            poster_url = f"{protocol}://{server.ip_address}/pms_image_proxy?{urlencode(params)}"
            resolved_by_key[key] = {
                'title': title,
                'poster_url': poster_url,
            }

        posters: List[Dict[str, str]] = []
        seen_urls: set[str] = set()
        for key in ranked_keys:
            item = resolved_by_key.get(key)
            if not item:
                continue
            url = item.get('poster_url', '')
            if not url or url in seen_urls:
                continue
            posters.append(item)
            seen_urls.add(url)
            if len(posters) >= limit:
                return posters

        # Fill any remaining slots with recent unique posters so hero still looks full.
        if len(posters) < limit:
            recent_fallback = self.get_recent_unique_history_posters(limit=limit * 2, max_scan=max_scan)
            for item in recent_fallback:
                url = item.get('poster_url', '')
                if not url or url in seen_urls:
                    continue
                posters.append(item)
                seen_urls.add(url)
                if len(posters) >= limit:
                    break

        return posters

    def get_cached_charts(self, run_id: int) -> Dict[str, str]:
        """
        Load cached chart HTML from previous run.

        Args:
            run_id: Database ID of AnalyticsRun record

        Returns:
            Dictionary with chart names as keys and HTML strings as values
        """
        cache_path = os.path.join(self.cache_dir, f'run_{run_id}_charts.json')

        if not os.path.exists(cache_path):
            raise FileNotFoundError(f"No cached charts found for run {run_id}")

        with open(cache_path, 'r') as f:
            return json.load(f)

    def get_viewing_history_table_data(self) -> List[Dict[str, Any]]:
        """
        Get viewing history data from the database for display in the dashboard table.

        Returns:
            List of dictionaries for table rows
        """
        # Get server configs for server order mapping
        server_a_config, server_b_config = ConfigService.get_server_configs()
        server_a_name = server_a_config.name if server_a_config else ''
        server_b_name = server_b_config.name if server_b_config else ''

        # Query all records from ViewingHistory, ordered by date descending
        records = ViewingHistory.query.order_by(ViewingHistory.started.desc()).all()

        table_data = []
        for record in records:
            # Format date and time
            date_str = record.date_played.strftime('%Y-%m-%d') if record.date_played else ''
            time_str = record.time_played or ''

            # Create sortable datetime
            sortable_datetime = date_str
            if time_str:
                try:
                    time_obj = datetime.strptime(time_str, '%I:%M%p')
                    sortable_datetime = f"{date_str} {time_obj.strftime('%H:%M')}"
                except:
                    pass

            # Determine server order class
            server_order = 'server-a' if record.server_name == server_a_name else 'server-b' if record.server_name == server_b_name else ''

            # Format title and subtitle based on media type
            media_type = record.media_type or ''
            title = record.full_title or record.title or ''
            subtitle = ''

            if media_type.lower() in ['tv', 'episode']:
                # For TV shows, use show name as title and episode info as subtitle
                if record.grandparent_title:
                    title = record.grandparent_title
                # Check for valid season/episode numbers (not None and not empty string)
                season = record.parent_media_index
                episode = record.media_index
                if season not in (None, '') and episode not in (None, ''):
                    try:
                        subtitle = f"S{int(season):02d}E{int(episode):02d}"
                        # Add episode name if available
                        if record.full_title and record.grandparent_title and record.full_title != record.grandparent_title:
                            if ' - ' in record.full_title:
                                episode_name = record.full_title.split(' - ', 1)[1]
                                subtitle += f" - {episode_name}"
                    except (ValueError, TypeError):
                        pass
            else:
                # For movies, use year as subtitle
                if record.year:
                    subtitle = f"({record.year})"

            # Format quality
            quality = ''
            if record.transcode_decision:
                td = record.transcode_decision.lower()
                if td == 'direct play':
                    quality = 'Direct Play'
                elif td == 'transcode':
                    quality = 'Transcode'
                elif td == 'copy':
                    quality = 'Direct Stream'
                else:
                    quality = record.transcode_decision.title()

            table_data.append({
                'history_id': record.id,
                'date_pt': date_str,
                'time_pt': time_str,
                'sortable_datetime': sortable_datetime,
                'Server': record.server_name or '',
                'server_order': server_order,
                'user': record.user or '',
                'user_thumb': '',  # Could be populated from user cache if needed
                'ip_address': record.ip_address or '',
                'media_type': media_type,
                'title': title,
                'subtitle': subtitle,
                'platform': record.platform or '',
                'product': record.product or '',
                'quality': quality,
                'percent_complete': record.percent_complete or 0
            })

        return table_data

    def get_cached_table_data(self, run_id: int) -> list:
        """
        Legacy method - now returns empty list since we use server-side pagination.

        Args:
            run_id: Database ID of AnalyticsRun record (ignored)

        Returns:
            Empty list - data is now loaded via AJAX
        """
        return []

    def get_viewing_history_paginated(
        self,
        start: int = 0,
        length: int = 50,
        search_value: str = '',
        order_column: int = 0,
        order_dir: str = 'desc'
    ) -> dict:
        """
        Get paginated viewing history data for DataTables server-side processing.

        Args:
            start: Row offset to start from
            length: Number of records to return
            search_value: Search filter string
            order_column: Column index to sort by
            order_dir: Sort direction ('asc' or 'desc')

        Returns:
            Dictionary with DataTables format:
            - draw: Request counter (set by caller)
            - recordsTotal: Total records in database
            - recordsFiltered: Records after filtering
            - data: List of row data
        """
        from flask_app.models import ViewingHistory
        from sqlalchemy import or_

        # Get server configs for server order mapping
        server_a_config, server_b_config = ConfigService.get_server_configs()
        server_a_name = server_a_config.name if server_a_config else ''
        server_b_name = server_b_config.name if server_b_config else ''

        # Base query
        query = ViewingHistory.query

        # Get total count before filtering
        total_records = query.count()

        # Apply search filter
        if search_value:
            search_filter = or_(
                ViewingHistory.user.ilike(f'%{search_value}%'),
                ViewingHistory.full_title.ilike(f'%{search_value}%'),
                ViewingHistory.title.ilike(f'%{search_value}%'),
                ViewingHistory.grandparent_title.ilike(f'%{search_value}%'),
                ViewingHistory.server_name.ilike(f'%{search_value}%'),
                ViewingHistory.platform.ilike(f'%{search_value}%'),
                ViewingHistory.ip_address.ilike(f'%{search_value}%')
            )
            query = query.filter(search_filter)

        # Get filtered count
        filtered_records = query.count()

        # Apply ordering - map column index to column
        column_map = {
            0: ViewingHistory.started,  # Date column sorts by started timestamp
            1: ViewingHistory.started,  # Hidden sortable column
            2: ViewingHistory.server_name,
            3: ViewingHistory.user,
            4: ViewingHistory.full_title,
            5: ViewingHistory.ip_address,
            6: ViewingHistory.platform,
            7: ViewingHistory.transcode_decision,
            8: ViewingHistory.percent_complete
        }

        order_col = column_map.get(order_column, ViewingHistory.started)
        if order_dir == 'asc':
            query = query.order_by(order_col.asc())
        else:
            query = query.order_by(order_col.desc())

        # Apply pagination
        records = query.offset(start).limit(length).all()

        # Format records for response
        data = []
        for record in records:
            # Format date and time
            date_str = record.date_played.strftime('%Y-%m-%d') if record.date_played else ''
            time_str = record.time_played or ''

            # Create sortable datetime
            sortable_datetime = date_str
            if time_str:
                try:
                    time_obj = datetime.strptime(time_str, '%I:%M%p')
                    sortable_datetime = f"{date_str} {time_obj.strftime('%H:%M')}"
                except:
                    pass

            # Determine server order class
            server_order = 'server-a' if record.server_name == server_a_name else 'server-b' if record.server_name == server_b_name else ''

            # Format title and subtitle based on media type
            media_type = record.media_type or ''
            title = record.full_title or record.title or ''
            subtitle = ''

            if media_type.lower() in ['tv', 'episode']:
                if record.grandparent_title:
                    title = record.grandparent_title
                season = record.parent_media_index
                episode = record.media_index
                if season not in (None, '') and episode not in (None, ''):
                    try:
                        subtitle = f"S{int(season):02d}E{int(episode):02d}"
                        if record.full_title and record.grandparent_title and record.full_title != record.grandparent_title:
                            if ' - ' in record.full_title:
                                episode_name = record.full_title.split(' - ', 1)[1]
                                subtitle += f" - {episode_name}"
                    except (ValueError, TypeError):
                        pass
            else:
                if record.year:
                    subtitle = f"({record.year})"

            # Format quality
            quality = ''
            if record.transcode_decision:
                td = record.transcode_decision.lower()
                if td == 'direct play':
                    quality = 'Direct Play'
                elif td == 'transcode':
                    quality = 'Transcode'
                elif td == 'copy':
                    quality = 'Direct Stream'
                else:
                    quality = record.transcode_decision.title()

            data.append({
                'history_id': record.id,
                'date_pt': date_str,
                'time_pt': time_str,
                'sortable_datetime': sortable_datetime,
                'Server': record.server_name or '',
                'server_order': server_order,
                'user': record.user or '',
                'ip_address': record.ip_address or '',
                'location': record.location or '',
                'geo_city': record.geo_city or '',
                'geo_region': record.geo_region or '',
                'geo_country': record.geo_country or '',
                'media_type': media_type,
                'title': title,
                'subtitle': subtitle,
                'platform': record.platform or '',
                'product': record.product or '',
                'quality': quality,
                'percent_complete': record.percent_complete or 0
            })

        return {
            'recordsTotal': total_records,
            'recordsFiltered': filtered_records,
            'data': data
        }

    def _get_location_from_ip(self, ip_address: str) -> str:
        """
        Get location (city, state) from IP address using ip-api.com.

        Args:
            ip_address: IP address to lookup

        Returns:
            Location string in format "City, State" or "Unknown" if lookup fails
        """
        import requests

        # Skip local/private IPs
        if ip_address in ['Unknown', '127.0.0.1', 'localhost'] or ip_address.startswith('192.168.') or ip_address.startswith('10.'):
            return 'Local Network'

        try:
            response = requests.get(f'http://ip-api.com/json/{ip_address}', timeout=2)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    city = data.get('city', '')
                    region = data.get('regionName', '')
                    if city and region:
                        return f"{city}, {region}"
                    elif city:
                        return city
                    elif region:
                        return region
        except Exception as e:
            print(f"Error looking up location for {ip_address}: {e}")

        return 'Unknown'

    @staticmethod
    def _split_movie_title_year(full_title: str) -> tuple[str, Optional[int]]:
        """Split a movie label like 'Title (2024)' into title and year."""
        text = str(full_title or '').strip()
        if not text:
            return '', None

        match = re.match(r'^(?P<title>.+?)\s*\((?P<year>\d{4})\)\s*$', text)
        if not match:
            return text, None

        title = (match.group('title') or '').strip() or text
        year = to_int(match.group('year'))
        return title, year

    def _build_poster_cards(self, df: pd.DataFrame, media_kind: str) -> List[Dict[str, Any]]:
        """
        Build ordered poster card metadata for dashboard display.

        Args:
            df: DataFrame with top media rows (movies or TV).
            media_kind: Either ``'movie'`` or ``'tv'``.

        Output order always follows df rank order (most watched first).
        """
        if df.empty:
            return []

        is_movie = media_kind == 'movie'

        servers = ServerConfig.query.filter(ServerConfig.is_active.is_(True)).all()
        server_map = {
            (server.name or '').strip(): server
            for server in servers
            if server.name and server.ip_address
        }

        cards: List[Dict[str, Any]] = []
        for index, row in enumerate(df.itertuples(index=False), start=1):
            if is_movie:
                full_title = str(getattr(row, 'full_title', '') or '').strip()
                if not full_title:
                    continue
                parsed_title, parsed_year = self._split_movie_title_year(full_title)
                display_title = parsed_title or full_title
            else:
                full_title = str(getattr(row, 'grandparent_title', '') or '').strip()
                if not full_title:
                    continue
                parsed_title, parsed_year = full_title, None
                display_title = full_title

            plays = to_int(getattr(row, 'count', 0)) or 0

            # --- find a ViewingHistory record for the poster thumbnail ---
            if is_movie:
                normalized_candidates = {normalize_title(full_title), normalize_title(parsed_title)}
                normalized_candidates = {item for item in normalized_candidates if item}
                title_filters = []
                for normalized in normalized_candidates:
                    title_filters.append(func.lower(ViewingHistory.full_title) == normalized)
                    title_filters.append(func.lower(ViewingHistory.title) == normalized)

                query = ViewingHistory.query.filter(func.lower(ViewingHistory.media_type) == 'movie')
                if title_filters:
                    query = query.filter(or_(*title_filters))
                if parsed_year is not None:
                    query = query.filter(
                        or_(ViewingHistory.year == parsed_year, ViewingHistory.year.is_(None))
                    )
            else:
                normalized_title = normalize_title(full_title)
                if not normalized_title:
                    continue
                query = (
                    ViewingHistory.query
                    .filter(func.lower(ViewingHistory.media_type).in_(['episode', 'tv', 'show']))
                    .filter(func.lower(ViewingHistory.grandparent_title) == normalized_title)
                )

            record = query.order_by(ViewingHistory.started.desc(), ViewingHistory.id.desc()).first()

            media_id = self._resolve_media_id_for_stream(
                media_type='movie' if is_movie else 'episode',
                title=display_title if is_movie else '',
                grandparent_title='' if is_movie else full_title,
                year=parsed_year,
            )

            poster_url = ''
            if record and record.thumb:
                server = server_map.get((record.server_name or '').strip())
                if server:
                    protocol = 'https' if server.use_ssl else 'http'
                    params: Dict[str, Any] = {
                        'img': record.thumb,
                        'width': 220,
                        'height': 330,
                        'fallback': 'poster',
                    }
                    if is_movie:
                        rating_key = to_int(record.rating_key)
                    else:
                        rating_key = to_int(record.grandparent_rating_key) or to_int(record.rating_key)
                    if rating_key is not None:
                        params['rating_key'] = rating_key
                    poster_url = f"{protocol}://{server.ip_address}/pms_image_proxy?{urlencode(params)}"

            cards.append({
                'rank': index,
                'title': display_title,
                'full_title': full_title,
                'plays': plays,
                'media_id': media_id,
                'poster_url': poster_url,
            })

        return cards

    def _build_movie_poster_cards(self, df_movies: pd.DataFrame) -> List[Dict[str, Any]]:
        return self._build_poster_cards(df_movies, 'movie')

    def _build_tv_poster_cards(self, df_tv: pd.DataFrame) -> List[Dict[str, Any]]:
        return self._build_poster_cards(df_tv, 'tv')

    def _resolve_history_id_for_stream(
        self,
        server_name: str,
        media_type: str,
        rating_key: Any,
        grandparent_rating_key: Any,
        title: str,
        grandparent_title: str
    ) -> Optional[int]:
        """
        Resolve a local ViewingHistory row for an active stream so dashboard links
        can route to the content detail page.
        """
        is_episode = (media_type or '').lower() == 'episode'
        target_key = to_int(grandparent_rating_key if is_episode else rating_key)
        target_title = normalize_title(grandparent_title if is_episode else title)

        base_query = ViewingHistory.query.filter(ViewingHistory.server_name == server_name)

        def pick_best(query) -> Optional[int]:
            candidates = query.order_by(ViewingHistory.started.desc(), ViewingHistory.id.desc()).limit(20).all()
            if not candidates:
                return None
            if target_title:
                for candidate in candidates:
                    candidate_title = (
                        candidate.grandparent_title if is_episode else candidate.title or candidate.full_title
                    )
                    if normalize_title(candidate_title) == target_title:
                        return candidate.id
            return candidates[0].id

        if target_key is not None:
            key_column = ViewingHistory.grandparent_rating_key if is_episode else ViewingHistory.rating_key
            matched_id = pick_best(base_query.filter(key_column == target_key))
            if matched_id is not None:
                return matched_id

        if target_title:
            if is_episode:
                title_query = base_query.filter(func.lower(ViewingHistory.grandparent_title) == target_title)
            else:
                title_query = base_query.filter(
                    or_(
                        func.lower(ViewingHistory.title) == target_title,
                        func.lower(ViewingHistory.full_title) == target_title
                    )
                )
            matched_id = pick_best(title_query)
            if matched_id is not None:
                return matched_id

        return None

    def _resolve_media_id_for_stream(
        self,
        media_type: str,
        title: str,
        grandparent_title: str,
        year: Any = None,
    ) -> Optional[int]:
        """
        Resolve a CachedMedia row for an active stream so dashboard links can
        route through the media-based content detail endpoint.
        """
        is_episode = (media_type or '').lower() == 'episode'
        target_title = normalize_title(grandparent_title if is_episode else title)
        if not target_title:
            return None

        if is_episode:
            record = (
                CachedMedia.query
                .filter(CachedMedia.media_type == 'show')
                .filter(func.lower(CachedMedia.title) == target_title)
                .first()
            )
            return record.id if record else None

        record_query = (
            CachedMedia.query
            .filter(CachedMedia.media_type == 'movie')
            .filter(func.lower(CachedMedia.title) == target_title)
        )

        target_year = to_int(year)
        if target_year is not None:
            exact_year = record_query.filter(CachedMedia.year == target_year).first()
            if exact_year:
                return exact_year.id

        matches = record_query.all()
        if not matches:
            return None

        # Fallback: choose the most recently added record for this movie title.
        best = max(matches, key=lambda item: (item.added_at or -1, item.id))
        return best.id

    def _parse_session(self, session: dict, server_config, server_order: str) -> Dict[str, Any]:
        """Parse a single Tautulli session into a stream dictionary."""
        ip_address = session.get('ip_address', 'Unknown')
        location = self._get_location_from_ip(ip_address)

        media_type = session.get('media_type', '')
        if media_type == 'episode':
            poster_thumb = session.get('grandparent_thumb', session.get('thumb', ''))
        else:
            poster_thumb = session.get('thumb', '')

        rating_key = session.get('rating_key', '')
        poster_url = ''
        if poster_thumb and rating_key:
            poster_url = f"{server_config.ip_address}/pms_image_proxy?img={poster_thumb}&rating_key={rating_key}&width=150&height=225&fallback=poster"

        full_title = session.get('full_title', session.get('title', 'Unknown'))
        grandparent_title = session.get('grandparent_title', '')
        title = full_title
        subtitle = ''

        if media_type == 'episode':
            season = session.get('parent_media_index', '')
            episode = session.get('media_index', '')
            if season and episode:
                try:
                    subtitle = f"S{int(season):02d}E{int(episode):02d}"
                    if full_title and grandparent_title and full_title != grandparent_title:
                        if ' - ' in full_title:
                            episode_name = full_title.split(' - ', 1)[1]
                            subtitle += f" - {episode_name}"
                except (ValueError, TypeError):
                    pass
            title = grandparent_title if grandparent_title else full_title
        elif media_type == 'movie':
            year = session.get('year', '')
            if year:
                try:
                    subtitle = f"({int(year)})"
                except (ValueError, TypeError):
                    pass

        transcode_decision = session.get('transcode_decision', '').lower()
        video_resolution = session.get('stream_video_full_resolution', '')
        if transcode_decision == 'direct play':
            quality = 'Direct Play'
        elif transcode_decision == 'transcode':
            quality = 'Transcode'
        elif transcode_decision == 'copy':
            quality = 'Direct Stream'
        else:
            quality = transcode_decision.title() if transcode_decision else ''

        if quality and video_resolution:
            quality = f"{quality} - {video_resolution}"

        platform = session.get('platform', 'Unknown')
        product = session.get('product', '')

        try:
            bandwidth_kbps = int(session.get('bandwidth', 0) or 0)
            bandwidth_mbps = round(bandwidth_kbps / 1000, 1) if bandwidth_kbps else 0
        except (ValueError, TypeError):
            bandwidth_mbps = 0

        return {
            'server': server_config.name,
            'server_order': server_order,
            'user': session.get('friendly_name', session.get('username', 'Unknown')),
            'title': title,
            'subtitle': subtitle,
            'media_type': media_type,
            'state': session.get('state', 'unknown'),
            'progress_percent': session.get('progress_percent', 0),
            'platform': platform,
            'product': product,
            'quality': quality,
            'bandwidth_mbps': bandwidth_mbps,
            'ip_address': ip_address,
            'location': location,
            'poster_url': poster_url,
            'media_id': self._resolve_media_id_for_stream(
                media_type=media_type,
                title=session.get('title', title),
                grandparent_title=grandparent_title,
                year=session.get('year'),
            )
        }

    def get_current_activity(self) -> list:
        """
        Get current streaming activity from all configured servers.

        Returns:
            List of dictionaries containing current streaming sessions
        """
        from multiplex_stats import TautulliClient

        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            return []

        current_streams = []
        server_configs = [(server_a_config, 'server-a')]
        if server_b_config:
            server_configs.append((server_b_config, 'server-b'))

        for server_config, server_order in server_configs:
            try:
                client = TautulliClient(server_config)
                activity = client.get_activity()

                if activity and 'response' in activity and 'data' in activity['response']:
                    sessions = activity['response']['data'].get('sessions', [])
                    for session in sessions:
                        current_streams.append(
                            self._parse_session(session, server_config, server_order)
                        )
            except Exception as e:
                print(f"Error fetching activity from {server_config.name}: {e}")

        return current_streams

    def get_users_for_filter(self) -> List[Dict[str, Any]]:
        """
        Get list of users for dropdown filter.

        Returns a simplified list of users with user_id and friendly_name
        for use in filter dropdowns. Users are sorted by friendly_name.

        Returns:
            List of dictionaries with 'user_id' and 'friendly_name'
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            return []

        users_by_id: Dict[int, str] = {}

        def fetch_users_from_server(client: TautulliClient):
            try:
                response = client.get_users()
                if response and 'response' in response and 'data' in response['response']:
                    for user in response['response']['data']:
                        user_id = user.get('user_id')
                        friendly_name = user.get('friendly_name', '')
                        if user_id and friendly_name:
                            users_by_id[user_id] = friendly_name
            except Exception as e:
                print(f"Error fetching users for filter: {e}")

        client_a = TautulliClient(server_a_config)
        fetch_users_from_server(client_a)

        if server_b_config:
            client_b = TautulliClient(server_b_config)
            fetch_users_from_server(client_b)

        # Convert to list and sort by friendly_name
        users = [
            {'user_id': uid, 'friendly_name': name}
            for uid, name in users_by_id.items()
        ]
        users.sort(key=lambda x: x['friendly_name'].lower())

        return users

    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Get all users from all configured servers with their statistics.

        Fetches user info from get_users API, play counts from get_library_user_stats,
        and last play dates from the local ViewingHistory database.
        Servers are queried in parallel using a thread pool.

        Returns:
            List of dictionaries containing user information from all servers
        """
        from concurrent.futures import ThreadPoolExecutor
        from multiplex_stats import TautulliClient
        from sqlalchemy import func

        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            return []

        server_a_key = 'server_a_plays'
        server_b_key = 'server_b_plays' if server_b_config else None
        play_keys = [server_a_key]
        if server_b_key:
            play_keys.append(server_b_key)

        def ensure_play_keys(user_data: Dict[str, Any]) -> None:
            for key in play_keys:
                if key not in user_data:
                    user_data[key] = 0

        def fetch_server_data(server_config, server_ip: str, plays_key: str):
            """Fetch users and play counts from a single server (thread-safe)."""
            users: Dict[str, Dict[str, Any]] = {}  # friendly_name -> user_data
            play_counts: Dict[str, int] = {}  # friendly_name -> plays for this server
            library_counts: Dict[str, int] = {}  # friendly_name -> library count

            client = TautulliClient(server_config)

            try:
                users_response = client.get_users()
                if users_response and 'response' in users_response and 'data' in users_response['response']:
                    for user in users_response['response']['data']:
                        friendly_name = user.get('friendly_name', '')
                        if not friendly_name:
                            continue

                        shared_libs = user.get('shared_libraries', [])
                        library_counts[friendly_name] = len(shared_libs) if shared_libs else 0

                        user_thumb = user.get('user_thumb', '')
                        thumb_url = ''
                        if user_thumb:
                            thumb_url = f"{server_ip}/pms_image_proxy?img={user_thumb}&width=40&height=40&fallback=poster"

                        users[friendly_name] = {
                            'user_id': user.get('user_id'),
                            'friendly_name': friendly_name,
                            'username': user.get('username', ''),
                            'email': user.get('email', ''),
                            'total_plays': 0,
                            'last_play': None,
                            'user_thumb': thumb_url,
                            'is_active': user.get('is_active', 1),
                            'library_count': 0,
                        }
            except Exception as e:
                print(f"Error fetching users from {server_config.name}: {e}")

            for section_id in [1, 2]:
                try:
                    stats_response = client.get_library_user_stats(section_id=section_id)
                    if stats_response and 'response' in stats_response and 'data' in stats_response['response']:
                        for stat in stats_response['response']['data']:
                            friendly_name = stat.get('friendly_name', '')
                            plays = stat.get('total_plays', 0)
                            if friendly_name:
                                play_counts[friendly_name] = play_counts.get(friendly_name, 0) + plays

                                if friendly_name not in users:
                                    users[friendly_name] = {
                                        'user_id': stat.get('user_id'),
                                        'friendly_name': friendly_name,
                                        'username': '',
                                        'email': '',
                                        'total_plays': 0,
                                        'last_play': None,
                                        'user_thumb': '',
                                        'is_active': 1,
                                        'library_count': 0,
                                    }
                except Exception as e:
                    print(f"Error fetching library stats (section {section_id}) from {server_config.name}: {e}")

            return users, play_counts, library_counts, plays_key

        # Build task list and run in parallel
        tasks = [(server_a_config, server_a_config.ip_address, server_a_key)]
        if server_b_config:
            tasks.append((server_b_config, server_b_config.ip_address, server_b_key))

        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            results = list(executor.map(lambda t: fetch_server_data(*t), tasks))

        # Merge results from all servers
        users_by_name: Dict[str, Dict[str, Any]] = {}
        for users, play_counts, library_counts, plays_key in results:
            for friendly_name, user_data in users.items():
                if friendly_name not in users_by_name:
                    users_by_name[friendly_name] = user_data
                    ensure_play_keys(users_by_name[friendly_name])
                else:
                    users_by_name[friendly_name]['library_count'] += library_counts.get(friendly_name, 0)

            for friendly_name, plays in play_counts.items():
                if friendly_name in users_by_name:
                    ensure_play_keys(users_by_name[friendly_name])
                    users_by_name[friendly_name]['total_plays'] += plays
                    users_by_name[friendly_name][plays_key] += plays

            # Apply library counts for first occurrence
            for friendly_name, count in library_counts.items():
                if friendly_name in users_by_name:
                    # Only set (not add) if this is the first server that created the user
                    if users_by_name[friendly_name]['library_count'] == 0:
                        users_by_name[friendly_name]['library_count'] = count

        # Get last play dates from ViewingHistory database
        # Query for max(started) grouped by user (username field in ViewingHistory)
        last_plays = ViewingHistory.query.with_entities(
            ViewingHistory.user,
            func.max(ViewingHistory.started).label('last_play')
        ).group_by(ViewingHistory.user).all()

        # Create a lookup dictionary keyed by username
        last_play_by_user = {row.user: row.last_play for row in last_plays if row.user}

        # Get the oldest date in ViewingHistory to use as "before" date for users with no plays
        oldest_history = ViewingHistory.query.with_entities(
            func.min(ViewingHistory.started)
        ).scalar()

        # Update users with last play dates
        # Match by username (ViewingHistory.user) OR friendly_name as fallback
        for friendly_name, user_data in users_by_name.items():
            username = user_data.get('username', '')
            # Try username first, then friendly_name as fallback
            if username and username in last_play_by_user:
                user_data['last_play'] = last_play_by_user[username]
            elif friendly_name in last_play_by_user:
                user_data['last_play'] = last_play_by_user[friendly_name]
            elif oldest_history:
                # User has no plays in history - mark with "before" prefix
                user_data['last_play_before'] = oldest_history

        # Convert to list and sort by total plays descending
        all_users = list(users_by_name.values())
        all_users.sort(key=lambda x: x.get('total_plays', 0), reverse=True)

        return all_users
