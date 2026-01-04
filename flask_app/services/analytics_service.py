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
    aggregate_user_stats, aggregate_movie_stats, aggregate_tv_stats
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

        # History data
        history_data_a = client_a.get_history(days=settings.history_days)
        history_data_b = client_b.get_history(days=settings.history_days) if client_b else None
        df_history = process_history_data(
            history_data_a, history_data_b,
            server_a_config.name, server_b_config.name if server_b_config else None
        )

        # Aggregate stats
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

        # 6b. Prepare and cache viewing history table data
        table_data = self._prepare_table_data(df_history, settings.history_table_days if hasattr(settings, 'history_table_days') else 60)
        table_cache_path = os.path.join(self.cache_dir, f'run_{run_id}_table.json')
        with open(table_cache_path, 'w') as f:
            json.dump(table_data, f)

        # 7. Calculate summary statistics
        total_plays = len(df_history)
        total_users = len(df_users)
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

        # Filter to last N days
        cutoff_date = datetime.now() - timedelta(days=table_days)

        # Convert date column to datetime if it's not already
        df_filtered = df_history.copy()
        if 'date' in df_filtered.columns:
            df_filtered['date_pt'] = df_filtered['date']

        # Select and rename columns for the table
        table_columns = {
            'date': 'date_pt',
            'Server': 'Server',
            'user': 'user',
            'ip_address': 'ip_address',
            'media_type': 'media_type',
            'full_title': 'title',
            'grandparent_title': 'show'
        }

        # Build table data
        table_data = []
        for _, row in df_filtered.iterrows():
            table_row = {}
            for df_col, table_col in table_columns.items():
                if df_col in row:
                    value = row[df_col]
                    # Handle NaN/None values
                    if value is None or (isinstance(value, float) and str(value) == 'nan'):
                        value = ''
                    table_row[table_col] = str(value)
                else:
                    table_row[table_col] = ''
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
