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
        table_data = self._prepare_table_data(df_history_full, settings.history_table_days)
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

    def _prepare_table_data(self, df_history, table_days: int) -> list:
        """
        Prepare viewing history data for DataTables display.

        Args:
            df_history: DataFrame with viewing history
            table_days: Number of days to include in table

        Returns:
            List of dictionaries for table rows
        """
        from datetime import datetime, timedelta
        import pandas as pd

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
        # The DataFrame has: date, user, media_type, full_title, grandparent_title, ip_address, Server
        table_data = []

        for _, row in df_filtered.iterrows():
            # Build each row with proper column mapping
            # Note: process_history_data creates 'date_pt' column (not 'date')
            table_row = {
                'date_pt': str(row.get('date_pt', '')) if pd.notna(row.get('date_pt')) else '',
                'Server': str(row.get('Server', '')) if pd.notna(row.get('Server')) else '',
                'user': str(row.get('user', '')) if pd.notna(row.get('user')) else '',
                'ip_address': str(row.get('ip_address', '')) if pd.notna(row.get('ip_address')) else '',
                'media_type': str(row.get('media_type', '')) if pd.notna(row.get('media_type')) else '',
                'title': str(row.get('full_title', '')) if pd.notna(row.get('full_title')) else '',
                'show': str(row.get('grandparent_title', '')) if pd.notna(row.get('grandparent_title')) else ''
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
                    current_streams.append({
                        'server': server_a_config.name,
                        'user': session.get('friendly_name', session.get('username', 'Unknown')),
                        'title': session.get('full_title', session.get('title', 'Unknown')),
                        'media_type': session.get('media_type', 'unknown'),
                        'state': session.get('state', 'unknown'),
                        'progress_percent': session.get('progress_percent', 0),
                        'player': session.get('player', 'Unknown'),
                        'ip_address': ip_address,
                        'location': location
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
                        current_streams.append({
                            'server': server_b_config.name,
                            'user': session.get('friendly_name', session.get('username', 'Unknown')),
                            'title': session.get('full_title', session.get('title', 'Unknown')),
                            'media_type': session.get('media_type', 'unknown'),
                            'state': session.get('state', 'unknown'),
                            'progress_percent': session.get('progress_percent', 0),
                            'player': session.get('player', 'Unknown'),
                            'ip_address': ip_address,
                            'location': location
                        })
            except Exception as e:
                print(f"Error fetching activity from {server_b_config.name}: {e}")

        return current_streams
