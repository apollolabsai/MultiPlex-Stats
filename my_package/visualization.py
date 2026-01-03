"""
Visualization functions for Tautulli analytics.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional

from my_package.models import PlotTheme, MediaColors


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
        color_continuous_scale='oryel',
        text='count',
        title=f'Number of Plays by User - {history_days} days'
    )

    fig.update_traces(
        textposition='outside',
        textfont_color='white',
        textfont_size=18,
        marker_line_color='rgba(0,0,0,0)'
    )

    # Apply theme
    layout_config = theme.get_layout_config(
        f'Number of Plays by User - {history_days} days',
        height=700
    )
    layout_config['xaxis']['tickfont']['size'] = 15
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
        color_continuous_scale='oryel',
        text='count',
        title=f'Most Popular Movies - {history_days} days'
    )

    fig.update_traces(
        textposition='outside',
        textfont_color='white',
        textfont_size=18,
        marker_line_color='rgba(0,0,0,0)'
    )

    # Apply theme
    layout_config = theme.get_layout_config(
        f'Most Popular Movies - {history_days} days',
        height=700
    )
    layout_config['xaxis']['tickfont']['size'] = 15
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
        color_continuous_scale='oryel',
        text='count',
        title=f'Most Popular TV Shows - {history_days} days'
    )

    fig.update_traces(
        textposition='outside',
        textfont_color='white',
        textfont_size=18,
        marker_line_color='rgba(0,0,0,0)'
    )

    # Apply theme
    layout_config = theme.get_layout_config(
        f'Most Popular TV Shows - {history_days} days',
        height=700
    )
    layout_config['xaxis']['tickfont']['size'] = 15
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
        'TV': '#0f4c5c',
        'Movies': '#e36414',
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
        server_a_name: '#0f4c5c',
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
