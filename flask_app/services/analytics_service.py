"""
Analytics service that bridges Flask to the existing multiplex_stats package.
"""
import os
import json
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd

from multiplex_stats import TautulliClient
from multiplex_stats.data_processing import (
    process_daily_data, process_monthly_data, process_history_data,
    aggregate_user_stats, aggregate_movie_stats, aggregate_tv_stats,
    filter_history_by_date
)
from multiplex_stats.timezone_utils import get_local_timezone
from multiplex_stats.visualization import (
    get_daily_chart_data, get_monthly_chart_data,
    get_user_chart_data, get_movie_chart_data, get_tv_chart_data,
    get_category_pie_data, get_server_pie_data, get_platform_pie_data
)
from flask_app.services.config_service import ConfigService
from flask_app.services.history_sync_service import HistorySyncService
from flask_app.models import ViewingHistory


class AnalyticsService:
    """Service to execute analytics pipeline using database configuration."""

    def __init__(self):
        self.cache_dir = os.path.join('instance', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)

    def run_full_analytics(self, run_id: int, daily_trend_days_override: int | None = None) -> Dict[str, Any]:
        """
        Execute full analytics pipeline using database config.

        This method replicates the logic in run_analytics.py but:
        1. Loads config from database instead of config.ini
        2. Returns chart HTML strings instead of writing dashboard.html
        3. Caches results for display in web UI

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

        # 3. Fetch and process data (following run_analytics.py logic)
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
        df_users = aggregate_user_stats(df_history, top_n=settings.top_users)
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

        charts_json = {
            'daily': get_daily_chart_data(df_daily, server_a_config.name, server_b_config.name if server_b_config else None),
            'monthly': get_monthly_chart_data(df_monthly, server_a_config.name, server_b_config.name if server_b_config else None),
            'users': get_user_chart_data(df_users, settings.history_days),
            'movies': get_movie_chart_data(df_movies, settings.history_days),
            'tv': get_tv_chart_data(df_tv, settings.history_days),
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

    def get_daily_chart_json(self, daily_trend_days: int | None = None) -> Dict[str, Any]:
        """
        Generate the daily play count chart JSON for Highcharts.

        Args:
            daily_trend_days: Optional override for daily trend range.

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

        daily_data_a = client_a.get_plays_by_date(time_range=daily_days)
        daily_data_b = client_b.get_plays_by_date(time_range=daily_days) if client_b else None
        df_daily = process_daily_data(
            daily_data_a, daily_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        chart_data = get_daily_chart_data(
            df_daily, server_a_config.name, server_b_config.name if server_b_config else None
        )

        return {
            'chart_data': chart_data,
            'daily_trend_days': daily_days
        }

    def get_monthly_chart_json(self, monthly_trend_months: int | None = None) -> Dict[str, Any]:
        """
        Generate the monthly play count chart JSON for Highcharts.

        Args:
            monthly_trend_months: Optional override for monthly trend range.

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

        monthly_data_a = client_a.get_plays_per_month(time_range=monthly_months)
        monthly_data_b = client_b.get_plays_per_month(time_range=monthly_months) if client_b else None
        df_monthly = process_monthly_data(
            monthly_data_a, monthly_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        chart_data = get_monthly_chart_data(
            df_monthly, server_a_config.name, server_b_config.name if server_b_config else None
        )

        return {
            'chart_data': chart_data,
            'monthly_trend_months': monthly_months
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

    def get_user_chart_json(self, days: int | None = None) -> Dict[str, Any]:
        """
        Generate the user activity chart JSON for Highcharts.

        Args:
            days: Optional override for user chart range.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        history_days = days or settings.history_days

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        history_data_a = client_a.get_history(days=history_days)
        history_data_b = client_b.get_history(days=history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        df_users = aggregate_user_stats(df_history, top_n=settings.top_users)
        chart_data = get_user_chart_data(df_users, history_days)

        return {
            'chart_data': chart_data,
            'user_chart_days': history_days
        }

    def get_movie_chart_json(self, days: int | None = None) -> Dict[str, Any]:
        """
        Generate the top movies chart JSON for Highcharts.

        Args:
            days: Optional override for movie chart range.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        history_days = days or settings.history_days

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        history_data_a = client_a.get_history(days=history_days)
        history_data_b = client_b.get_history(days=history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        df_movies = aggregate_movie_stats(df_history, top_n=settings.top_movies)
        chart_data = get_movie_chart_data(df_movies, history_days)

        return {
            'chart_data': chart_data,
            'movie_chart_days': history_days
        }

    def get_tv_chart_json(self, days: int | None = None) -> Dict[str, Any]:
        """
        Generate the top TV shows chart JSON for Highcharts.

        Args:
            days: Optional override for TV chart range.

        Returns:
            Dictionary with chart data and the day range used.
        """
        server_a_config, server_b_config = ConfigService.get_server_configs()
        settings = ConfigService.get_analytics_settings()

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        history_days = days or settings.history_days

        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        history_data_a = client_a.get_history(days=history_days)
        history_data_b = client_b.get_history(days=history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        df_tv = aggregate_tv_stats(df_history, top_n=settings.top_tv_shows)
        chart_data = get_tv_chart_data(df_tv, history_days)

        return {
            'chart_data': chart_data,
            'tv_chart_days': history_days
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

    def get_current_activity(self) -> list:
        """
        Get current streaming activity from all configured servers.

        Returns:
            List of dictionaries containing current streaming sessions
        """
        from multiplex_stats import TautulliClient

        # Load server configurations
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            return []

        current_streams = []

        # Fetch activity from Server A
        try:
            client_a = TautulliClient(server_a_config)
            activity_a = client_a.get_activity()

            if activity_a and 'response' in activity_a and 'data' in activity_a['response']:
                sessions = activity_a['response']['data'].get('sessions', [])
                for session in sessions:
                    ip_address = session.get('ip_address', 'Unknown')
                    location = self._get_location_from_ip(ip_address)

                    # Build poster URL for hover preview
                    # For TV episodes, use grandparent_thumb (show poster), otherwise use thumb
                    media_type = session.get('media_type', '')
                    if media_type == 'episode':
                        poster_thumb = session.get('grandparent_thumb', session.get('thumb', ''))
                    else:
                        poster_thumb = session.get('thumb', '')

                    rating_key = session.get('rating_key', '')
                    poster_url = ''
                    if poster_thumb and rating_key:
                        # Use Tautulli's pms_image_proxy to serve the poster (150x225)
                        poster_url = f"{server_a_config.ip_address}/pms_image_proxy?img={poster_thumb}&rating_key={rating_key}&width=150&height=225&fallback=poster"

                    # Format title and subtitle like viewing history table
                    full_title = session.get('full_title', session.get('title', 'Unknown'))
                    grandparent_title = session.get('grandparent_title', '')
                    title = full_title
                    subtitle = ''

                    if media_type == 'episode':
                        # For TV shows, build "S01E04 - Episode Name" subtitle
                        season = session.get('parent_media_index', '')
                        episode = session.get('media_index', '')
                        if season and episode:
                            try:
                                subtitle = f"S{int(season):02d}E{int(episode):02d}"
                                # If full_title is different from grandparent_title, append episode name
                                if full_title and grandparent_title and full_title != grandparent_title:
                                    if ' - ' in full_title:
                                        episode_name = full_title.split(' - ', 1)[1]
                                        subtitle += f" - {episode_name}"
                            except (ValueError, TypeError):
                                pass
                        title = grandparent_title if grandparent_title else full_title
                    elif media_type == 'movie':
                        # For movies, use year as subtitle
                        year = session.get('year', '')
                        if year:
                            try:
                                subtitle = f"({int(year)})"
                            except (ValueError, TypeError):
                                pass

                    # Format quality like viewing history table
                    transcode_decision = session.get('transcode_decision', '').lower()
                    if transcode_decision == 'direct play':
                        quality = 'Direct Play'
                    elif transcode_decision == 'transcode':
                        quality = 'Transcode'
                    elif transcode_decision == 'copy':
                        quality = 'Direct Stream'
                    else:
                        quality = transcode_decision.title() if transcode_decision else ''

                    # Format platform and product like viewing history table
                    platform = session.get('platform', 'Unknown')
                    product = session.get('product', '')

                    current_streams.append({
                        'server': server_a_config.name,
                        'server_order': 'server-a',
                        'user': session.get('friendly_name', session.get('username', 'Unknown')),
                        'title': title,
                        'subtitle': subtitle,
                        'media_type': media_type,
                        'state': session.get('state', 'unknown'),
                        'progress_percent': session.get('progress_percent', 0),
                        'platform': platform,
                        'product': product,
                        'quality': quality,
                        'ip_address': ip_address,
                        'location': location,
                        'poster_url': poster_url
                    })
        except Exception as e:
            print(f"Error fetching activity from {server_a_config.name}: {e}")

        # Fetch activity from Server B if configured
        if server_b_config:
            try:
                client_b = TautulliClient(server_b_config)
                activity_b = client_b.get_activity()

                if activity_b and 'response' in activity_b and 'data' in activity_b['response']:
                    sessions = activity_b['response']['data'].get('sessions', [])
                    for session in sessions:
                        ip_address = session.get('ip_address', 'Unknown')
                        location = self._get_location_from_ip(ip_address)

                        # Build poster URL for hover preview
                        # For TV episodes, use grandparent_thumb (show poster), otherwise use thumb
                        media_type = session.get('media_type', '')
                        if media_type == 'episode':
                            poster_thumb = session.get('grandparent_thumb', session.get('thumb', ''))
                        else:
                            poster_thumb = session.get('thumb', '')

                        rating_key = session.get('rating_key', '')
                        poster_url = ''
                        if poster_thumb and rating_key:
                            # Use Tautulli's pms_image_proxy to serve the poster (150x225)
                            poster_url = f"{server_b_config.ip_address}/pms_image_proxy?img={poster_thumb}&rating_key={rating_key}&width=150&height=225&fallback=poster"

                        # Format title and subtitle like viewing history table
                        full_title = session.get('full_title', session.get('title', 'Unknown'))
                        grandparent_title = session.get('grandparent_title', '')
                        title = full_title
                        subtitle = ''

                        if media_type == 'episode':
                            # For TV shows, build "S01E04 - Episode Name" subtitle
                            season = session.get('parent_media_index', '')
                            episode = session.get('media_index', '')
                            if season and episode:
                                try:
                                    subtitle = f"S{int(season):02d}E{int(episode):02d}"
                                    # If full_title is different from grandparent_title, append episode name
                                    if full_title and grandparent_title and full_title != grandparent_title:
                                        if ' - ' in full_title:
                                            episode_name = full_title.split(' - ', 1)[1]
                                            subtitle += f" - {episode_name}"
                                except (ValueError, TypeError):
                                    pass
                            title = grandparent_title if grandparent_title else full_title
                        elif media_type == 'movie':
                            # For movies, use year as subtitle
                            year = session.get('year', '')
                            if year:
                                try:
                                    subtitle = f"({int(year)})"
                                except (ValueError, TypeError):
                                    pass

                        # Format quality like viewing history table
                        transcode_decision = session.get('transcode_decision', '').lower()
                        if transcode_decision == 'direct play':
                            quality = 'Direct Play'
                        elif transcode_decision == 'transcode':
                            quality = 'Transcode'
                        elif transcode_decision == 'copy':
                            quality = 'Direct Stream'
                        else:
                            quality = transcode_decision.title() if transcode_decision else ''

                        # Format platform and product like viewing history table
                        platform = session.get('platform', 'Unknown')
                        product = session.get('product', '')

                        current_streams.append({
                            'server': server_b_config.name,
                            'server_order': 'server-b',
                            'user': session.get('friendly_name', session.get('username', 'Unknown')),
                            'title': title,
                            'subtitle': subtitle,
                            'media_type': media_type,
                            'state': session.get('state', 'unknown'),
                            'progress_percent': session.get('progress_percent', 0),
                            'platform': platform,
                            'product': product,
                            'quality': quality,
                            'ip_address': ip_address,
                            'location': location,
                            'poster_url': poster_url
                        })
            except Exception as e:
                print(f"Error fetching activity from {server_b_config.name}: {e}")

        return current_streams

    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Get all users from all configured servers with their statistics.

        Fetches user info from get_users API, play counts from get_library_user_stats,
        and last play dates from the local ViewingHistory database.

        Returns:
            List of dictionaries containing user information from all servers
        """
        from multiplex_stats import TautulliClient
        from sqlalchemy import func

        # Load server configurations
        server_a_config, server_b_config = ConfigService.get_server_configs()

        if not server_a_config:
            return []

        # Dictionary to aggregate user data by friendly_name
        users_by_name: Dict[str, Dict[str, Any]] = {}

        def process_server(client: TautulliClient, server_config, server_ip: str):
            """Process users and play counts from a single server."""
            # First, get user list for metadata (username, email, thumb, etc.)
            try:
                users_response = client.get_users()
                if users_response and 'response' in users_response and 'data' in users_response['response']:
                    for user in users_response['response']['data']:
                        friendly_name = user.get('friendly_name', '')
                        if not friendly_name:
                            continue

                        # Count shared libraries from the shared_libraries list
                        shared_libs = user.get('shared_libraries', [])
                        library_count = len(shared_libs) if shared_libs else 0

                        if friendly_name not in users_by_name:
                            # Build user thumb URL
                            user_thumb = user.get('user_thumb', '')
                            thumb_url = ''
                            if user_thumb:
                                thumb_url = f"{server_ip}/pms_image_proxy?img={user_thumb}&width=40&height=40&fallback=poster"

                            users_by_name[friendly_name] = {
                                'user_id': user.get('user_id'),
                                'friendly_name': friendly_name,
                                'username': user.get('username', ''),
                                'email': user.get('email', ''),
                                'total_plays': 0,
                                'last_play': None,  # Unix timestamp of most recent play
                                'user_thumb': thumb_url,
                                'is_active': user.get('is_active', 1),
                                'library_count': library_count,
                            }
                        else:
                            # User already exists from another server, add library count
                            users_by_name[friendly_name]['library_count'] += library_count
            except Exception as e:
                print(f"Error fetching users from {server_config.name}: {e}")

            # Now get play counts from library stats (Movies = section 1, TV = section 2)
            for section_id in [1, 2]:
                try:
                    stats_response = client.get_library_user_stats(section_id=section_id)
                    if stats_response and 'response' in stats_response and 'data' in stats_response['response']:
                        for stat in stats_response['response']['data']:
                            friendly_name = stat.get('friendly_name', '')
                            plays = stat.get('total_plays', 0)

                            if friendly_name in users_by_name:
                                users_by_name[friendly_name]['total_plays'] += plays
                            elif friendly_name:
                                # User exists in library stats but not in users list
                                users_by_name[friendly_name] = {
                                    'user_id': stat.get('user_id'),
                                    'friendly_name': friendly_name,
                                    'username': '',
                                    'email': '',
                                    'total_plays': plays,
                                    'last_play': None,
                                    'user_thumb': '',
                                    'is_active': 1,
                                    'library_count': 0,
                                }
                except Exception as e:
                    print(f"Error fetching library stats (section {section_id}) from {server_config.name}: {e}")

        # Process Server A
        client_a = TautulliClient(server_a_config)
        process_server(client_a, server_a_config, server_a_config.ip_address)

        # Process Server B if configured
        if server_b_config:
            client_b = TautulliClient(server_b_config)
            process_server(client_b, server_b_config, server_b_config.ip_address)

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
