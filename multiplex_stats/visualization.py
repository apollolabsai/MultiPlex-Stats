"""
Visualization functions for Tautulli analytics.
"""

import pandas as pd
from typing import Optional

from multiplex_stats.models import MediaColors


# Highcharts Data Functions (return JSON-serializable dicts)
# =============================================================================

def _interpolate_color(color1: str, color2: str, ratio: float) -> str:
    """Interpolate between two hex colors."""
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip('#')
        return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))

    def rgb_to_hex(rgb):
        return '#{:02x}{:02x}{:02x}'.format(*rgb)

    r1, g1, b1 = hex_to_rgb(color1)
    r2, g2, b2 = hex_to_rgb(color2)

    r = int(r1 + (r2 - r1) * ratio)
    g = int(g1 + (g2 - g1) * ratio)
    b = int(b1 + (b2 - b1) * ratio)

    return rgb_to_hex((r, g, b))


def get_daily_chart_data(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    colors: Optional[MediaColors] = None
) -> dict:
    """
    Get data for daily stacked bar chart in Highcharts format.

    Args:
        df: DataFrame with daily data (must have 'Month', 'Count', 'ColorMapping' columns)
        server_a_name: Name of server A
        server_b_name: Name of server B (optional)
        colors: Optional color configuration

    Returns:
        Dictionary with 'categories', 'series', 'totals', 'title'
    """
    if colors is None:
        colors = MediaColors()

    color_map = colors.get_color_map(server_a_name, server_b_name)

    # Get unique dates (categories for x-axis) - convert to strings
    categories = [str(d) for d in sorted(df['Month'].unique())]

    # Build series data for each ColorMapping
    series = []
    for color_key in df['ColorMapping'].unique():
        df_filtered = df[df['ColorMapping'] == color_key]
        data = []
        for cat in categories:
            # Match against string representation
            mask = df_filtered['Month'].astype(str) == cat
            value = df_filtered[mask]['Count'].sum()
            data.append(int(value))

        series.append({
            'name': color_key.replace('_', ' '),
            'data': data,
            'color': color_map.get(color_key, '#ffffff')
        })

    # Calculate totals for annotations
    totals = []
    for cat in categories:
        mask = df['Month'].astype(str) == cat
        totals.append(int(df[mask]['Count'].sum()))

    return {
        'categories': categories,
        'series': series,
        'totals': totals,
        'title': 'Daily Play Counts by Server and Media Type'
    }


def get_monthly_chart_data(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    colors: Optional[MediaColors] = None
) -> dict:
    """
    Get data for monthly stacked bar chart in Highcharts format.

    Args:
        df: DataFrame with monthly data
        server_a_name: Name of server A
        server_b_name: Name of server B (optional)
        colors: Optional color configuration

    Returns:
        Dictionary with 'categories', 'series', 'totals', 'title'
    """
    if colors is None:
        colors = MediaColors()

    color_map = colors.get_color_map(server_a_name, server_b_name)

    # Sort by Month and get categories
    df_sorted = df.sort_values('Month')
    categories = [str(m) for m in df_sorted['Month'].unique()]

    series = []
    for color_key in df_sorted['ColorMapping'].unique():
        df_filtered = df_sorted[df_sorted['ColorMapping'] == color_key]
        data = []
        for cat in categories:
            mask = df_filtered['Month'].astype(str) == cat
            value = df_filtered[mask]['Count'].sum()
            data.append(int(value))

        series.append({
            'name': color_key.replace('_', ' '),
            'data': data,
            'color': color_map.get(color_key, '#ffffff')
        })

    totals = []
    for cat in categories:
        mask = df_sorted['Month'].astype(str) == cat
        totals.append(int(df_sorted[mask]['Count'].sum()))

    return {
        'categories': categories,
        'series': series,
        'totals': totals,
        'title': 'Monthly Play Counts by Server and Media Type'
    }


def get_user_chart_data(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    history_days: int,
    top_n: Optional[int] = None
) -> dict:
    """
    Get data for user stacked bar chart with gradient coloring.

    Args:
        df: DataFrame with history data (must have 'user', 'Server', 'count' columns)
        server_a_name: Name of server A
        server_b_name: Name of server B (optional)
        history_days: Number of days included in the data
        top_n: Optional limit to top N users

    Returns:
        Dictionary with 'categories', 'series', 'title'
    """
    if df.empty:
        return {
            'categories': [],
            'series': [],
            'title': f'Number of Plays by User - {history_days} days'
        }

    grouped = df.groupby(['user', 'Server'])['count'].sum().reset_index()
    totals = grouped.groupby('user')['count'].sum().reset_index(name='total')
    totals = totals.sort_values(by='total', ascending=False)
    if top_n is not None:
        totals = totals.head(top_n)

    users = totals['user'].tolist()
    if not users:
        return {
            'categories': [],
            'series': [],
            'title': f'Number of Plays by User - {history_days} days'
        }

    pivot = grouped.pivot(index='user', columns='Server', values='count').fillna(0)
    pivot = pivot.reindex(users).fillna(0)

    def build_series(server_name: str, color: str) -> dict:
        counts = (
            pivot[server_name].tolist()
            if server_name in pivot.columns
            else [0] * len(users)
        )
        return {
            'name': server_name,
            'data': [int(count) for count in counts],
            'color': color
        }

    series = [
        build_series(server_a_name, '#E6B413')
    ]
    if server_b_name:
        series.append(
            build_series(server_b_name, '#e36414')
        )

    return {
        'categories': users,
        'series': series,
        'title': f'Number of Plays by User - {history_days} days'
    }


def get_movie_chart_data(df: pd.DataFrame, history_days: int) -> dict:
    """
    Get data for top movies bar chart.

    Args:
        df: DataFrame with aggregated movie data (must have 'full_title', 'count' columns)
        history_days: Number of days included in the data

    Returns:
        Dictionary with 'categories', 'data', 'title'
    """
    titles = df['full_title'].tolist()
    counts = df['count'].tolist()

    max_count = max(counts) if counts else 1
    min_count = min(counts) if counts else 0

    data_with_colors = []
    for count in counts:
        ratio = (count - min_count) / (max_count - min_count) if max_count > min_count else 0
        data_with_colors.append({
            'y': int(count),
            'color': _interpolate_color('#ff9800', '#ed542c', ratio)
        })

    return {
        'categories': titles,
        'data': data_with_colors,
        'title': f'Most Popular Movies - {history_days} days'
    }


def get_tv_chart_data(df: pd.DataFrame, history_days: int) -> dict:
    """
    Get data for top TV shows bar chart.

    Args:
        df: DataFrame with aggregated TV data (must have 'grandparent_title', 'count' columns)
        history_days: Number of days included in the data

    Returns:
        Dictionary with 'categories', 'data', 'title'
    """
    titles = df['grandparent_title'].tolist()
    counts = df['count'].tolist()

    max_count = max(counts) if counts else 1
    min_count = min(counts) if counts else 0

    data_with_colors = []
    for count in counts:
        ratio = (count - min_count) / (max_count - min_count) if max_count > min_count else 0
        data_with_colors.append({
            'y': int(count),
            'color': _interpolate_color('#ff9800', '#ed542c', ratio)
        })

    return {
        'categories': titles,
        'data': data_with_colors,
        'title': f'Most Popular TV Shows - {history_days} days'
    }


def get_category_pie_data(df: pd.DataFrame, history_days: int) -> dict:
    """
    Get data for category distribution pie chart.

    Args:
        df: DataFrame with daily data
        history_days: Number of days included in the data

    Returns:
        Dictionary with 'data', 'title'
    """
    df_filtered = df[
        ~df['Category'].str.contains('Total', case=False) &
        ~df['Category'].isin(['Music'])
    ]
    df_category = df_filtered.groupby(['Category'])['Count'].sum().reset_index()

    custom_colors = {'TV': '#e36414', 'Movies': '#e6b413'}

    data = []
    for _, row in df_category.iterrows():
        data.append({
            'name': row['Category'],
            'y': int(row['Count']),
            'color': custom_colors.get(row['Category'], '#ffffff')
        })

    return {
        'data': data,
        'title': f'Breakdown by Category - {history_days} days'
    }


def get_server_pie_data(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    history_days: int
) -> dict:
    """
    Get data for server distribution pie chart.

    Args:
        df: DataFrame with server data
        server_a_name: Name of server A
        server_b_name: Name of server B (optional)
        history_days: Number of days included in the data

    Returns:
        Dictionary with 'data', 'title'
    """
    df_server = df.groupby(['Server'])['Count'].sum().reset_index()

    custom_colors = {server_a_name: '#E6B413'}
    if server_b_name:
        custom_colors[server_b_name] = '#e36414'

    data = []
    for _, row in df_server.iterrows():
        data.append({
            'name': row['Server'],
            'y': int(row['Count']),
            'color': custom_colors.get(row['Server'], '#ffffff')
        })

    return {
        'data': data,
        'title': f'Server Distribution - {history_days} days'
    }


def get_platform_pie_data(df: pd.DataFrame, history_days: int) -> dict:
    """
    Get data for platform distribution pie chart.

    Args:
        df: DataFrame with history data (must have 'platform' column)
        history_days: Number of days included in the data

    Returns:
        Dictionary with 'data', 'title'
    """
    df_platform = df.groupby('platform').size().reset_index(name='Count')
    df_platform = df_platform.sort_values('Count', ascending=False)

    # Use a color palette for platforms
    colors = ['#7cb5ec', '#434348', '#90ed7d', '#f7a35c', '#8085e9',
              '#f15c80', '#e4d354', '#2b908f', '#f45b5b', '#91e8e1']

    data = []
    for i, (_, row) in enumerate(df_platform.iterrows()):
        data.append({
            'name': row['platform'],
            'y': int(row['Count']),
            'color': colors[i % len(colors)]
        })

    return {
        'data': data,
        'title': f'Platform Distribution - {history_days} days'
    }
