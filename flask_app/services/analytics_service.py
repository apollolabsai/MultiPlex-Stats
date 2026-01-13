"""
Analytics service that bridges Flask to the existing multiplex_stats package.
"""
import os
import json
from typing import Dict, Any
from datetime import datetime

from multiplex_stats import TautulliClient
from multiplex_stats.data_processing import (
    process_daily_data, process_monthly_data, process_history_data,
    aggregate_user_stats, aggregate_movie_stats, aggregate_tv_stats,
    filter_history_by_date
)
from multiplex_stats.visualization import (
    create_daily_bar_chart, create_monthly_bar_chart,
    create_user_bar_chart, create_movie_bar_chart, create_tv_bar_chart,
    create_category_pie_chart, create_server_pie_chart
)
from flask_app.services.config_service import ConfigService


class AnalyticsService:
    """Service to execute analytics pipeline using database configuration."""

    def __init__(self):
        self.cache_dir = os.path.join('instance', 'cache')
        os.makedirs(self.cache_dir, exist_ok=True)

    def run_full_analytics(self, run_id: int) -> Dict[str, Any]:
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

        if not server_a_config:
            raise ValueError("No server configuration found. Please configure at least one server.")

        # 2. Create API clients
        client_a = TautulliClient(server_a_config)
        client_b = TautulliClient(server_b_config) if server_b_config else None

        # 3. Fetch and process data (following run_analytics.py logic)
        # Daily data
        daily_data_a = client_a.get_plays_by_date(time_range=settings.daily_trend_days)
        daily_data_b = client_b.get_plays_by_date(time_range=settings.daily_trend_days) if client_b else None
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

        # History data - fetch max of history_days and history_table_days to ensure we have enough data
        # for both charts and the viewing history table
        max_history_days = max(settings.history_days, settings.history_table_days)
        history_data_a = client_a.get_history(days=max_history_days)
        history_data_b = client_b.get_history(days=max_history_days) if client_b else None
        df_history_full = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        # Filter history data for charts/stats (using history_days setting)
        df_history = filter_history_by_date(df_history_full, settings.history_days)

        # Aggregate stats (using filtered data)
        df_users = aggregate_user_stats(df_history, top_n=settings.top_users)
        df_movies = aggregate_movie_stats(df_history, top_n=settings.top_movies)
        df_tv = aggregate_tv_stats(df_history, top_n=settings.top_tv_shows)

        # 4. Generate visualizations
        fig_daily = create_daily_bar_chart(df_daily, server_a_config.name, server_b_config.name if server_b_config else None)
        fig_monthly = create_monthly_bar_chart(df_monthly, server_a_config.name, server_b_config.name if server_b_config else None)
        fig_users = create_user_bar_chart(df_users, settings.history_days)
        fig_movies = create_movie_bar_chart(df_movies, settings.history_days)
        fig_tv = create_tv_bar_chart(df_tv, settings.history_days)
        fig_category = create_category_pie_chart(df_daily, settings.history_days)
        fig_server = create_server_pie_chart(
            df_daily, server_a_config.name,
            server_b_config.name if server_b_config else None,
            settings.history_days
        )

        # 5. Convert charts to HTML (for embedding in Jinja templates)
        charts_html = {
            'daily': fig_daily.to_html(full_html=False, include_plotlyjs=False),
            'monthly': fig_monthly.to_html(full_html=False, include_plotlyjs=False),
            'users': fig_users.to_html(full_html=False, include_plotlyjs=False),
            'movies': fig_movies.to_html(full_html=False, include_plotlyjs=False),
            'tv': fig_tv.to_html(full_html=False, include_plotlyjs=False),
            'category': fig_category.to_html(full_html=False, include_plotlyjs=False),
            'server': fig_server.to_html(full_html=False, include_plotlyjs=False)
        }

        # 6. Cache chart HTML to disk
        cache_path = os.path.join(self.cache_dir, f'run_{run_id}_charts.json')
        with open(cache_path, 'w') as f:
            json.dump(charts_html, f)

        # 6b. Prepare and cache viewing history table data (using full history, will be filtered by table_days)
        table_data = self._prepare_table_data(df_history_full, settings.history_table_days,
                                               server_a_config.name, server_b_config.name if server_b_config else None)
        table_cache_path = os.path.join(self.cache_dir, f'run_{run_id}_table.json')
        with open(table_cache_path, 'w') as f:
            json.dump(table_data, f)

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
            'history_days': settings.history_days,
            'generated_at': datetime.now().isoformat()
        }

        return {
            'total_plays': total_plays,
            'total_users': total_users,
            'summary': summary
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

    def _prepare_table_data(self, df_history, table_days: int, server_a_name: str, server_b_name: str = None) -> list:
        """
        Prepare viewing history data for DataTables display.

        Args:
            df_history: DataFrame with viewing history
            table_days: Number of days to include in table
            server_a_name: Name of Server A
            server_b_name: Name of Server B (optional)

        Returns:
            List of dictionaries for table rows
        """
        from datetime import datetime, timedelta
        import pandas as pd

        # Get user avatar mapping
        user_thumb_map = self._get_user_thumb_map()

        # Filter to last N days
        df_filtered = df_history.copy()

        # Filter by date_pt column (created by process_history_data)
        if 'date_pt' in df_filtered.columns:
            df_filtered['date_pt_datetime'] = pd.to_datetime(df_filtered['date_pt'])
            cutoff_date = datetime.now() - timedelta(days=table_days)
            df_filtered = df_filtered[df_filtered['date_pt_datetime'] >= cutoff_date]
            # Drop the temporary datetime column
            df_filtered = df_filtered.drop('date_pt_datetime', axis=1)

        # Map dataframe columns to table columns
        table_data = []

        for _, row in df_filtered.iterrows():
            # Get user avatar URL
            user_id = str(row.get('user_id', '')) if pd.notna(row.get('user_id')) else ''
            user_thumb = user_thumb_map.get(user_id, '')

            # Format title based on media type
            media_type_raw = row.get('media_type', '')
            media_type = str(media_type_raw) if pd.notna(media_type_raw) else ''

            full_title_raw = row.get('full_title', '')
            full_title = str(full_title_raw) if pd.notna(full_title_raw) else ''

            grandparent_title_raw = row.get('grandparent_title', '')
            grandparent_title = str(grandparent_title_raw) if pd.notna(grandparent_title_raw) else ''

            # For TV shows, build "S01E04 - Episode Name" subtitle
            subtitle = ''
            if media_type and media_type.lower() in ['tv', 'episode']:
                season = row.get('parent_media_index', '')
                episode = row.get('media_index', '')
                # Check if season and episode are valid numbers
                if pd.notna(season) and pd.notna(episode) and season != '' and episode != '':
                    try:
                        subtitle = f"S{int(season):02d}E{int(episode):02d}"
                        # If full_title is different from grandparent_title, append episode name
                        if full_title and grandparent_title and full_title != grandparent_title:
                            # Extract episode name (usually after " - ")
                            if ' - ' in full_title:
                                episode_name = full_title.split(' - ', 1)[1]
                                subtitle += f" - {episode_name}"
                    except (ValueError, TypeError):
                        # If conversion fails, just leave subtitle empty
                        pass
                title = grandparent_title if grandparent_title else full_title  # Use show name as main title
            else:
                # For movies, use year as subtitle
                year = row.get('year', '')
                if pd.notna(year) and year != '' and year:
                    try:
                        subtitle = f"({int(year)})"
                    except (ValueError, TypeError):
                        # If conversion fails, just leave subtitle empty
                        pass
                title = full_title

            # Format quality (use transcode_decision: direct play, transcode, copy)
            transcode_decision_raw = row.get('transcode_decision', '')
            transcode_decision = str(transcode_decision_raw) if pd.notna(transcode_decision_raw) else ''

            # Format transcode decision for display
            if transcode_decision and transcode_decision.lower() == 'direct play':
                quality = 'Direct Play'
            elif transcode_decision and transcode_decision.lower() == 'transcode':
                quality = 'Transcode'
            elif transcode_decision and transcode_decision.lower() == 'copy':
                quality = 'Direct Stream'
            else:
                quality = transcode_decision.title() if transcode_decision else ''

            # Build table row
            date_pt_raw = row.get('date_pt', '')
            time_pt_raw = row.get('time_pt', '')
            server_raw = row.get('Server', '')
            friendly_name_raw = row.get('friendly_name', '')
            user_raw = row.get('user', '')
            ip_address_raw = row.get('ip_address', '')
            platform_raw = row.get('platform', '')
            product_raw = row.get('product', '')
            percent_complete_raw = row.get('percent_complete', 0)

            # Create a sortable datetime string (YYYY-MM-DD HH:MM format for sorting)
            # Convert time from "9:34pm" to "21:34" format for sorting
            date_pt_str = str(date_pt_raw) if pd.notna(date_pt_raw) else ''
            time_pt_str = str(time_pt_raw) if pd.notna(time_pt_raw) else ''

            # Convert 12-hour time to 24-hour for sorting
            sortable_datetime = date_pt_str
            if time_pt_str:
                try:
                    from datetime import datetime
                    time_obj = datetime.strptime(time_pt_str, '%I:%M%p')
                    sortable_datetime = f"{date_pt_str} {time_obj.strftime('%H:%M')}"
                except:
                    sortable_datetime = date_pt_str

            # Determine server order (A or B)
            server_name = str(server_raw) if pd.notna(server_raw) else ''
            server_order = 'server-a' if server_name == server_a_name else 'server-b' if server_name == server_b_name else ''

            table_row = {
                'date_pt': date_pt_str,
                'time_pt': time_pt_str,
                'sortable_datetime': sortable_datetime,
                'Server': server_name,
                'server_order': server_order,
                'user': str(friendly_name_raw) if pd.notna(friendly_name_raw) else str(user_raw) if pd.notna(user_raw) else '',
                'user_thumb': user_thumb,
                'ip_address': str(ip_address_raw) if pd.notna(ip_address_raw) else '',
                'media_type': media_type,
                'title': title,
                'subtitle': subtitle,
                'platform': str(platform_raw) if pd.notna(platform_raw) else '',
                'product': str(product_raw) if pd.notna(product_raw) else '',
                'quality': quality,
                'percent_complete': int(percent_complete_raw) if pd.notna(percent_complete_raw) else 0
            }
            table_data.append(table_row)

        return table_data

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

    def get_cached_table_data(self, run_id: int) -> list:
        """
        Load cached table data from previous run.

        Args:
            run_id: Database ID of AnalyticsRun record

        Returns:
            List of dictionaries for table rows
        """
        table_cache_path = os.path.join(self.cache_dir, f'run_{run_id}_table.json')

        if not os.path.exists(table_cache_path):
            return []

        with open(table_cache_path, 'r') as f:
            return json.load(f)

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

                    current_streams.append({
                        'server': server_a_config.name,
                        'user': session.get('friendly_name', session.get('username', 'Unknown')),
                        'title': session.get('full_title', session.get('title', 'Unknown')),
                        'media_type': session.get('media_type', 'unknown'),
                        'state': session.get('state', 'unknown'),
                        'progress_percent': session.get('progress_percent', 0),
                        'player': session.get('player', 'Unknown'),
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

                        current_streams.append({
                            'server': server_b_config.name,
                            'user': session.get('friendly_name', session.get('username', 'Unknown')),
                            'title': session.get('full_title', session.get('title', 'Unknown')),
                            'media_type': session.get('media_type', 'unknown'),
                            'state': session.get('state', 'unknown'),
                            'progress_percent': session.get('progress_percent', 0),
                            'player': session.get('player', 'Unknown'),
                            'ip_address': ip_address,
                            'location': location,
                            'poster_url': poster_url
                        })
            except Exception as e:
                print(f"Error fetching activity from {server_b_config.name}: {e}")

        return current_streams
