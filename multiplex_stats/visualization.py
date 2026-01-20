"""
Visualization functions for Tautulli analytics.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional

from multiplex_stats.models import PlotTheme, MediaColors


def create_daily_bar_chart(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    theme: Optional[PlotTheme] = None,
    colors: Optional[MediaColors] = None
) -> go.Figure:
    """
    Create a stacked bar chart for daily play counts.

    Args:
        df: DataFrame with daily data (must have 'Month', 'Count', 'ColorMapping' columns)
        server_a_name: Name of server A
        server_b_name: Name of server B (optional, can be None for single server)
        theme: Optional plot theme configuration
        colors: Optional color configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()
    if colors is None:
        colors = MediaColors()

    color_map = colors.get_color_map(server_a_name, server_b_name)

    fig = px.bar(
        df,
        x='Month',
        y='Count',
        color='ColorMapping',
        color_discrete_map=color_map,
        title='Daily Play Counts by Server and Media Type'
    )

    # Remove outlines and make stacked
    fig.update_traces(marker_line_color='rgba(0,0,0,0)')
    fig.update_layout(barmode='relative')

    # Calculate totals and add annotations
    totals = df.groupby('Month', as_index=False)['Count'].sum()
    annotations = [
        dict(
            x=row['Month'],
            y=row['Count'],
            text=f"{int(row['Count']):,}",
            showarrow=False,
            xanchor='center',
            yanchor='bottom',
            font=dict(color='white', size=12)
        )
        for _, row in totals.iterrows()
    ]

    # Apply theme
    layout_config = theme.get_layout_config('Daily Play Counts by Server and Media Type')
    layout_config['annotations'] = annotations
    layout_config['xaxis']['title'] = dict(text='Day', font=dict(color='white'))
    fig.update_layout(**layout_config)

    return fig


def create_monthly_bar_chart(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    theme: Optional[PlotTheme] = None,
    colors: Optional[MediaColors] = None
) -> go.Figure:
    """
    Create a stacked bar chart for monthly play counts.

    Args:
        df: DataFrame with monthly data
        server_a_name: Name of server A
        server_b_name: Name of server B
        theme: Optional plot theme configuration
        colors: Optional color configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()
    if colors is None:
        colors = MediaColors()

    color_map = colors.get_color_map(server_a_name, server_b_name)

    fig = px.bar(
        df,
        x='Month',
        y='Count',
        color='ColorMapping',
        color_discrete_map=color_map,
        title='Monthly Play Counts by Server and Media Type'
    )

    # Remove outlines
    fig.update_traces(marker_line_color='rgba(0,0,0,0)')

    # Calculate totals
    total_counts = df.groupby(['Month'])['Count'].sum().reset_index()

    # Add scatter trace for annotations
    annotations_trace = px.scatter(
        total_counts,
        x='Month',
        y=1.02 * total_counts['Count'],
        text='Count',
        color_discrete_sequence=['rgba(0,0,0,0)']
    )

    fig.add_trace(annotations_trace.data[0])
    fig.data[-1].update(texttemplate='%{text:,}')
    fig.update_traces(textfont_color='white')

    # Apply theme
    layout_config = theme.get_layout_config('Monthly Play Counts by Server and Media Type')
    fig.update_layout(**layout_config)

    return fig


def create_user_bar_chart(
    df: pd.DataFrame,
    history_days: int,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a bar chart for user play counts.

    Args:
        df: DataFrame with aggregated user data
        history_days: Number of days included in the data
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    fig = px.bar(
        df,
        y='count',
        x='user',
        color='count',
        color_continuous_scale=['#ff9800', '#ed542b'],
        text='count',
        title=f'Number of Plays by User - {history_days} days'
    )

    fig.update_traces(
        textposition='outside',
        textfont_color='white',
        textfont_size=18,
        marker_line_color='rgba(0,0,0,0)'
    )
    fig.update_layout(coloraxis_showscale=False)

    # Apply theme
    layout_config = theme.get_layout_config(
        f'Number of Plays by User - {history_days} days',
        height=700
    )
    layout_config['xaxis']['tickfont']['size'] = 15
    layout_config['xaxis']['title'] = dict(text='', font=dict(color='white'))
    fig.update_layout(**layout_config)

    return fig


def create_movie_bar_chart(
    df: pd.DataFrame,
    history_days: int,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a bar chart for top movies.

    Args:
        df: DataFrame with aggregated movie data
        history_days: Number of days included in the data
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    fig = px.bar(
        df,
        y='count',
        x='full_title',
        color='count',
        color_continuous_scale=['#ff9800', '#ed542b'],
        text='count',
        title=f'Most Popular Movies - {history_days} days'
    )

    fig.update_traces(
        textposition='outside',
        textfont_color='white',
        textfont_size=18,
        marker_line_color='rgba(0,0,0,0)'
    )
    fig.update_layout(coloraxis_showscale=False)

    # Apply theme
    layout_config = theme.get_layout_config(
        f'Most Popular Movies - {history_days} days',
        height=700
    )
    layout_config['xaxis']['tickfont']['size'] = 15
    layout_config['xaxis']['title'] = dict(text='', font=dict(color='white'))
    fig.update_layout(**layout_config)

    return fig


def create_tv_bar_chart(
    df: pd.DataFrame,
    history_days: int,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a bar chart for top TV shows.

    Args:
        df: DataFrame with aggregated TV data
        history_days: Number of days included in the data
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    fig = px.bar(
        df,
        y='count',
        x='grandparent_title',
        color='count',
        color_continuous_scale=['#ff9800', '#ed542b'],
        text='count',
        title=f'Most Popular TV Shows - {history_days} days'
    )

    fig.update_traces(
        textposition='outside',
        textfont_color='white',
        textfont_size=18,
        marker_line_color='rgba(0,0,0,0)'
    )
    fig.update_layout(coloraxis_showscale=False)

    # Apply theme
    layout_config = theme.get_layout_config(
        f'Most Popular TV Shows - {history_days} days',
        height=700
    )
    layout_config['xaxis']['tickfont']['size'] = 15
    layout_config['xaxis']['title'] = dict(text='', font=dict(color='white'))
    fig.update_layout(**layout_config)

    return fig


def create_category_pie_chart(
    df: pd.DataFrame,
    history_days: int,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a pie chart for category distribution.

    Args:
        df: DataFrame with daily data
        history_days: Number of days included in the data
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    # Filter and group
    df_filtered = df[
        ~df['Category'].str.contains('Total', case=False) &
        ~df['Category'].isin(['Music'])
    ]
    df_category = df_filtered.groupby(['Category'])['Count'].sum().reset_index()
    df_category['Percentage'] = df_category['Count'] / df_category['Count'].sum() * 100

    custom_colors = {
        'TV': '#e36414',
        'Movies': '#f18a3d',
    }

    fig = px.pie(
        df_category,
        values='Count',
        names='Category',
        color='Category',
        color_discrete_map=custom_colors,
        title=f'Breakdown by Category - {history_days} days'
    )

    fig.update_traces(
        texttemplate='%{percent:.1%}',
        textposition='inside',
        textfont_size=16
    )

    # Apply theme
    fig.update_layout(
        plot_bgcolor=theme.plot_bgcolor,
        paper_bgcolor=theme.paper_bgcolor,
        title={'text': f'Breakdown by Category - {history_days} days', 'font': {'color': theme.title_color}},
        legend=dict(font=dict(color=theme.text_color)),
        height=400
    )

    return fig


def create_server_pie_chart(
    df: pd.DataFrame,
    server_a_name: str,
    server_b_name: Optional[str],
    history_days: int,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a pie chart for server distribution.

    Args:
        df: DataFrame with server data
        server_a_name: Name of server A
        server_b_name: Name of server B (optional, can be None for single server)
        history_days: Number of days included in the data
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    df_server = df.groupby(['Server'])['Count'].sum().reset_index()
    df_server['Percentage'] = df_server['Count'] / df_server['Count'].sum() * 100

    custom_colors = {
        server_a_name: '#102baf',
    }
    if server_b_name:
        custom_colors[server_b_name] = '#e36414'

    fig = px.pie(
        df_server,
        values='Count',
        names='Server',
        color='Server',
        color_discrete_map=custom_colors,
        title=f'Server Distribution - {history_days} days'
    )

    fig.update_traces(
        texttemplate='%{percent:.1%}',
        textposition='inside',
        textfont_size=16
    )

    # Apply theme
    fig.update_layout(
        plot_bgcolor=theme.plot_bgcolor,
        paper_bgcolor=theme.paper_bgcolor,
        title={'text': f'Server Distribution - {history_days} days', 'font': {'color': theme.title_color}},
        legend=dict(font=dict(color=theme.text_color)),
        height=400
    )

    return fig


def create_all_time_bar_chart(
    df_pivoted: pd.DataFrame,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a stacked bar chart for all-time statistics.

    Args:
        df_pivoted: Pivoted DataFrame with all-time stats
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    custom_colors = {'Movies': '#fcca46', 'TV': '#fe7f2d'}

    fig = px.bar(
        df_pivoted,
        x=['Movies', 'TV'],
        y=df_pivoted.index,
        color_discrete_map=custom_colors,
        barmode='stack',
        text=df_pivoted['combined_plays'],
        title='All Time Plays'
    )

    # Remove outlines
    fig.update_traces(marker_line_color='rgba(0,0,0,0)')

    # Apply theme
    layout_config = theme.get_layout_config('All Time Plays', height=2000)
    layout_config['xaxis']['tickfont']['size'] = 15
    fig.update_layout(**layout_config)

    return fig


def create_platform_pie_chart(
    df: pd.DataFrame,
    history_days: int,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a pie chart for platform distribution.

    Args:
        df: DataFrame with platform data (must have 'platform' column)
        history_days: Number of days included in the data
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    # Group by platform and count
    df_platform = df.groupby('platform').size().reset_index(name='Count')
    df_platform = df_platform.sort_values('Count', ascending=False)

    # Use a color sequence for multiple platforms
    fig = px.pie(
        df_platform,
        values='Count',
        names='platform',
        color_discrete_sequence=px.colors.qualitative.Set2,
        title=f'Platform Distribution - {history_days} days'
    )

    fig.update_traces(
        texttemplate='%{percent:.1%}',
        textposition='inside',
        textfont_size=16
    )

    # Apply theme
    fig.update_layout(
        plot_bgcolor=theme.plot_bgcolor,
        paper_bgcolor=theme.paper_bgcolor,
        title={'text': f'Platform Distribution - {history_days} days', 'font': {'color': theme.title_color}},
        legend=dict(font=dict(color=theme.text_color)),
        height=400
    )

    return fig


def create_activity_pie_chart(
    df_aggregated: pd.DataFrame,
    theme: Optional[PlotTheme] = None
) -> go.Figure:
    """
    Create a pie chart for current activity.

    Args:
        df_aggregated: Aggregated activity DataFrame
        theme: Optional plot theme configuration

    Returns:
        Plotly figure object
    """
    if theme is None:
        theme = PlotTheme()

    custom_colors = {
        'Apollo': '#0f4c5c',
        'ApolloSS': '#e36414',
    }

    fig = px.pie(
        df_aggregated,
        values='count',
        names='server',
        color='server',
        color_discrete_map=custom_colors,
        title='Current Activity'
    )

    fig.update_traces(textinfo='value', textfont_size=16)

    # Apply theme
    fig.update_layout(
        plot_bgcolor=theme.plot_bgcolor,
        paper_bgcolor=theme.paper_bgcolor,
        title={'text': 'Current Activity', 'font': {'color': theme.title_color}},
        legend=dict(font=dict(color=theme.text_color)),
        height=400
    )

    return fig


# =============================================================================
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


def get_user_chart_data(df: pd.DataFrame, history_days: int) -> dict:
    """
    Get data for user bar chart with gradient coloring.

    Args:
        df: DataFrame with aggregated user data (must have 'user', 'count' columns)
        history_days: Number of days included in the data

    Returns:
        Dictionary with 'categories', 'data', 'title'
    """
    users = df['user'].tolist()
    counts = df['count'].tolist()

    # Calculate gradient colors based on count values
    max_count = max(counts) if counts else 1
    min_count = min(counts) if counts else 0

    data_with_colors = []
    for count in counts:
        ratio = (count - min_count) / (max_count - min_count) if max_count > min_count else 0
        data_with_colors.append({
            'y': int(count),
            'color': _interpolate_color('#ff9800', '#ed542b', ratio)
        })

    return {
        'categories': users,
        'data': data_with_colors,
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
            'color': _interpolate_color('#ff9800', '#ed542b', ratio)
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
            'color': _interpolate_color('#ff9800', '#ed542b', ratio)
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

    custom_colors = {'TV': '#e36414', 'Movies': '#f18a3d'}

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

    custom_colors = {server_a_name: '#102baf'}
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
