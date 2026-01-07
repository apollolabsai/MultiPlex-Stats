"""
Data processing functions for Tautulli analytics.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Any


def process_daily_data(
    data_a: dict[str, Any],
    data_b: dict[str, Any] | None,
    server_a_name: str,
    server_b_name: str | None
) -> pd.DataFrame:
    """
    Process daily play data from one or two servers.

    Args:
        data_a: API response from server A
        data_b: API response from server B (optional, can be None)
        server_a_name: Name of server A
        server_b_name: Name of server B (optional, can be None)

    Returns:
        DataFrame with combined and processed daily data in long format
    """
    # Process Server A
    categories_a = data_a['response']['data']['categories']
    series_a = data_a['response']['data']['series']

    data_list = []
    for s in series_a:
        category_name = s['name']
        data_values = s['data']
        data_dict = {
            'Server': server_a_name,
            'Category': category_name,
            **dict(zip(categories_a, data_values))
        }
        data_list.append(data_dict)
    df_a = pd.DataFrame(data_list)

    # Process Server B if it exists
    if data_b and server_b_name:
        categories_b = data_b['response']['data']['categories']
        series_b = data_b['response']['data']['series']

        data_list = []
        for s in series_b:
            category_name = s['name']
            data_values = s['data']
            data_dict = {
                'Server': server_b_name,
                'Category': category_name,
                **dict(zip(categories_b, data_values))
            }
            data_list.append(data_dict)
        df_b = pd.DataFrame(data_list)

        # Combine DataFrames
        all_month_cols = [c for c in df_a.columns if c not in ['Server', 'Category']]
        all_month_cols_b = [c for c in df_b.columns if c not in ['Server', 'Category']]
        all_months = sorted(set(all_month_cols) | set(all_month_cols_b))

        df_a = df_a.reindex(columns=['Server', 'Category'] + all_months, fill_value=0)
        df_b = df_b.reindex(columns=['Server', 'Category'] + all_months, fill_value=0)
        df_combined = pd.concat([df_a, df_b], ignore_index=True)
    else:
        # Single server mode
        all_month_cols = [c for c in df_a.columns if c not in ['Server', 'Category']]
        df_combined = df_a

    # Convert to long format
    df_melted = pd.melt(
        df_combined,
        id_vars=['Server', 'Category'],
        var_name='Month',
        value_name='Count'
    )

    # Filter out unwanted categories
    filter_categories = ['Music', server_a_name]
    if server_b_name:
        filter_categories.append(server_b_name)

    df_plot = df_melted[
        ~df_melted['Category'].isin(filter_categories)
    ].copy()
    df_plot = df_plot[~df_plot['Category'].str.contains('Total', case=False)]

    # Create color mapping column
    df_plot['ColorMapping'] = df_plot['Server'] + '_' + df_plot['Category']

    return df_plot


def process_monthly_data(
    data_a: dict[str, Any],
    data_b: dict[str, Any] | None,
    server_a_name: str,
    server_b_name: str | None
) -> pd.DataFrame:
    """
    Process monthly play data from one or two servers.

    Args:
        data_a: API response from server A
        data_b: API response from server B (optional, can be None)
        server_a_name: Name of server A
        server_b_name: Name of server B (optional, can be None)

    Returns:
        DataFrame with combined and processed monthly data
    """
    # Process Server A
    categories = data_a['response']['data']['categories']
    series = data_a['response']['data']['series']

    data_list = []
    for s in series:
        category_name = s['name']
        data_values = s['data']
        data_dict = {
            'Server': server_a_name,
            'Category': category_name,
            **dict(zip(categories, data_values))
        }
        data_list.append(data_dict)
    df_month_a = pd.DataFrame(data_list)

    # Process Server B if it exists
    if data_b and server_b_name:
        categories = data_b['response']['data']['categories']
        series = data_b['response']['data']['series']

        data_list = []
        for s in series:
            category_name = s['name']
            data_values = s['data']
            data_dict = {
                'Server': server_b_name,
                'Category': category_name,
                **dict(zip(categories, data_values))
            }
            data_list.append(data_dict)
        df_month_b = pd.DataFrame(data_list)

        # Combine DataFrames
        df_combined = pd.concat([df_month_a, df_month_b], ignore_index=True)
    else:
        # Single server mode
        df_combined = df_month_a

    # Convert to long format
    df_melted = pd.melt(
        df_combined,
        id_vars=['Server', 'Category'],
        var_name='Month',
        value_name='Count'
    )

    # Remove Music category
    df_melted = df_melted[df_melted['Category'] != 'Music']

    # Convert Month to datetime and reformat
    df_melted['Month'] = pd.to_datetime(df_melted['Month'], format='%b %Y')
    df_melted['Month'] = df_melted['Month'].dt.strftime('%Y%m')

    # Create color mapping column
    df_melted['ColorMapping'] = df_melted['Server'] + '_' + df_melted['Category']

    return df_melted


def process_history_data(
    data_a: dict[str, Any],
    data_b: dict[str, Any] | None,
    server_a_name: str,
    server_b_name: str | None
) -> pd.DataFrame:
    """
    Process play history data from one or two servers.

    Args:
        data_a: API response from server A
        data_b: API response from server B (optional, can be None)
        server_a_name: Name of server A
        server_b_name: Name of server B (optional, can be None)

    Returns:
        DataFrame with combined and processed history data
    """
    # Extract records from Server A
    records_a = data_a['response']['data']['data']
    df_a = pd.DataFrame(
        records_a,
        columns=["date", "user", "media_type", "full_title", "grandparent_title", "ip_address", "platform", "percent_complete"]
    )
    df_a['Server'] = server_a_name

    # Extract records from Server B if it exists
    if data_b and server_b_name:
        records_b = data_b['response']['data']['data']
        df_b = pd.DataFrame(
            records_b,
            columns=["date", "user", "media_type", "full_title", "grandparent_title", "ip_address", "platform", "percent_complete"]
        )
        df_b['Server'] = server_b_name

        # Combine and clean
        df_combined = pd.concat([df_a, df_b], ignore_index=True)
        df_combined = df_combined.drop_duplicates()
    else:
        # Single server mode
        df_combined = df_a

    # Convert Unix timestamp to datetime
    df_combined['date'] = pd.to_datetime(df_combined['date'], unit='s')

    # Convert to Pacific Time
    df_combined['date_pst'] = (
        df_combined['date']
        .dt.tz_localize(timezone.utc)
        .dt.tz_convert('America/Los_Angeles')
    )

    # Extract date string
    df_combined['date_pt'] = df_combined['date_pst'].dt.strftime('%Y-%m-%d')

    # Clean up columns
    df_combined.rename(columns={'date': 'date_time'}, inplace=True)
    df_combined.drop(columns=['date_pst', 'date_time'], inplace=True)

    # Add count column and normalize media_type
    df_combined['count'] = 1
    df_combined['media_type'] = df_combined['media_type'].replace('episode', 'TV')

    return df_combined


def aggregate_user_stats(df: pd.DataFrame, top_n: int = None) -> pd.DataFrame:
    """
    Aggregate play counts by user.

    Args:
        df: DataFrame with history data
        top_n: Optional limit to top N users (None = all users)

    Returns:
        DataFrame with aggregated user statistics
    """
    grouped = df.groupby(['user'])['count'].sum().reset_index()
    grouped = grouped.sort_values(by='count', ascending=False)

    if top_n is not None:
        return grouped.head(top_n)

    return grouped


def aggregate_movie_stats(df: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
    """
    Aggregate play counts for movies.

    Args:
        df: DataFrame with history data
        top_n: Number of top movies to return

    Returns:
        DataFrame with top N movies by play count
    """
    df_movies = df[df['media_type'] == 'movie'].copy()
    grouped = df_movies.groupby(['full_title'])['count'].sum().reset_index()
    grouped = grouped.sort_values(by='count', ascending=False)
    return grouped.head(top_n)


def aggregate_tv_stats(df: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
    """
    Aggregate play counts for TV shows.

    Args:
        df: DataFrame with history data
        top_n: Number of top shows to return

    Returns:
        DataFrame with top N TV shows by play count
    """
    df_tv = df[df['media_type'] == 'TV'].copy()
    grouped = df_tv.groupby(['grandparent_title'])['count'].sum().reset_index()
    grouped = grouped.sort_values(by='count', ascending=False)
    return grouped.head(top_n)


def process_library_stats(
    movie_data_a: dict[str, Any],
    movie_data_b: dict[str, Any],
    tv_data_a: dict[str, Any],
    tv_data_b: dict[str, Any]
) -> pd.DataFrame:
    """
    Process all-time library statistics.

    Args:
        movie_data_a: Movie stats from server A
        movie_data_b: Movie stats from server B
        tv_data_a: TV stats from server A
        tv_data_b: TV stats from server B

    Returns:
        DataFrame with pivoted all-time statistics
    """
    # Process movie data
    user_info_list_a = [
        {"friendly_name": user["friendly_name"], "total_plays": user["total_plays"]}
        for user in movie_data_a["response"]["data"]
    ]
    df_movies_a = pd.DataFrame(user_info_list_a)

    user_info_list_b = [
        {"friendly_name": user["friendly_name"], "total_plays": user["total_plays"]}
        for user in movie_data_b["response"]["data"]
    ]
    df_movies_b = pd.DataFrame(user_info_list_b)

    df_movies_combined = pd.concat([df_movies_a, df_movies_b], ignore_index=True)
    df_movies_combined['media_type'] = 'Movies'
    df_movies_combined = (
        df_movies_combined
        .groupby(['friendly_name', 'media_type'])['total_plays']
        .sum()
        .reset_index()
    )

    # Process TV data
    user_info_list_a = [
        {"friendly_name": user["friendly_name"], "total_plays": user["total_plays"]}
        for user in tv_data_a["response"]["data"]
    ]
    df_tv_a = pd.DataFrame(user_info_list_a)

    user_info_list_b = [
        {"friendly_name": user["friendly_name"], "total_plays": user["total_plays"]}
        for user in tv_data_b["response"]["data"]
    ]
    df_tv_b = pd.DataFrame(user_info_list_b)

    df_tv_combined = pd.concat([df_tv_a, df_tv_b], ignore_index=True)
    df_tv_combined['media_type'] = 'TV'
    df_tv_combined = (
        df_tv_combined
        .groupby(['friendly_name', 'media_type'])['total_plays']
        .sum()
        .reset_index()
    )

    # Combine and pivot
    df_combined = pd.concat([df_tv_combined, df_movies_combined], ignore_index=True)
    df_pivoted = df_combined.pivot_table(
        index='friendly_name',
        columns='media_type',
        values='total_plays',
        fill_value=0
    )
    df_pivoted['combined_plays'] = df_pivoted['Movies'] + df_pivoted['TV']
    df_pivoted = df_pivoted.sort_values(by='combined_plays', ascending=True)

    return df_pivoted


def process_activity_data(
    data_a: dict[str, Any],
    data_b: dict[str, Any],
    server_a_name: str,
    server_b_name: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process current activity data from two servers.

    Args:
        data_a: Activity data from server A
        data_b: Activity data from server B
        server_a_name: Name of server A
        server_b_name: Name of server B

    Returns:
        Tuple of (detailed activity DataFrame, aggregated activity DataFrame)
    """
    # Process Server A
    stream_count_a = data_a["response"]["data"]["stream_count"]
    sessions_a = data_a["response"]["data"]["sessions"]

    activity_list_a = [
        {
            "stream_count": stream_count_a,
            "media_type": session["media_type"],
            "user": session["user"],
            "ip_address": session["ip_address"],
            "full_title": session["full_title"]
        }
        for session in sessions_a
    ]
    df_a = pd.DataFrame(activity_list_a)
    df_a['server'] = server_a_name
    df_a['count'] = 1

    # Process Server B
    stream_count_b = data_b["response"]["data"]["stream_count"]
    sessions_b = data_b["response"]["data"]["sessions"]

    activity_list_b = [
        {
            "stream_count": stream_count_b,
            "media_type": session["media_type"],
            "user": session["user"],
            "ip_address": session["ip_address"],
            "full_title": session["full_title"]
        }
        for session in sessions_b
    ]
    df_b = pd.DataFrame(activity_list_b)
    df_b['server'] = server_b_name
    df_b['count'] = 1

    # Combine
    df_combined = pd.concat([df_a, df_b], ignore_index=True)
    df_combined = df_combined.sort_values(by='server', ascending=True)

    # Aggregate by server
    df_aggregated = df_combined.groupby(['server'])['count'].sum().reset_index()

    return df_combined, df_aggregated


def filter_history_by_date(df: pd.DataFrame, num_days: int) -> pd.DataFrame:
    """
    Filter history DataFrame by number of days.

    Args:
        df: DataFrame with 'date_pt' column
        num_days: Number of days to include

    Returns:
        Filtered DataFrame
    """
    df = df.copy()
    df['date_pt'] = pd.to_datetime(df['date_pt'])

    cutoff_date = datetime.now() - pd.Timedelta(days=num_days)
    df_filtered = df[df['date_pt'] >= cutoff_date]

    df_filtered['date_pt'] = df_filtered['date_pt'].dt.strftime('%Y-%m-%d')

    return df_filtered


def aggregate_all_time_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate all-time content statistics.

    Args:
        df: DataFrame with history data

    Returns:
        DataFrame with aggregated content statistics
    """
    df = df.copy()

    # Create movie_show column
    df['movie_show'] = np.where(
        df['media_type'].str.lower() == 'tv',
        df['grandparent_title'],
        df['full_title']
    )

    # Select relevant columns
    df = df[['date_pt', 'media_type', 'movie_show']]
    df['count'] = 1

    # Aggregate
    df_grouped = df.groupby(['media_type', 'movie_show'], as_index=False)['count'].sum()
    df_grouped = df_grouped.sort_values(by='count', ascending=False)
    df_grouped = df_grouped.reset_index(drop=True)

    return df_grouped
