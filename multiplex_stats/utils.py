"""
Utility functions for Tautulli analytics.
"""

import pandas as pd
from datetime import datetime
from typing import Optional


def format_dataframe_for_display(
    df: pd.DataFrame,
    num_days: Optional[int] = None,
    selected_user: str = 'All Users',
    selected_title: str = 'All Titles',
    selected_show: str = 'All Shows',
    max_title_length: int = 60
) -> pd.DataFrame:
    """
    Format and filter history DataFrame for display.

    Args:
        df: Input DataFrame with history data
        num_days: Optional number of days to filter
        selected_user: Filter by specific user or 'All Users'
        selected_title: Filter by specific title or 'All Titles'
        selected_show: Filter by specific show or 'All Shows'
        max_title_length: Maximum length for title text

    Returns:
        Formatted and filtered DataFrame
    """
    output_df = df.drop_duplicates().copy()

    # Reorder columns
    column_order = ['date_pt', 'Server', 'user', 'ip_address', 'media_type', 'full_title', 'grandparent_title', 'count']
    output_df = output_df[column_order]

    # Filter by date if specified
    if num_days is not None:
        output_df['date_pt'] = pd.to_datetime(output_df['date_pt'])
        cutoff_date = datetime.now() - pd.Timedelta(days=num_days)
        output_df = output_df[output_df['date_pt'] >= cutoff_date]
        output_df['date_pt'] = output_df['date_pt'].dt.strftime('%Y-%m-%d')

    # Apply filters
    if selected_user != 'All Users':
        output_df = output_df[output_df['user'] == selected_user]

    if selected_title != 'All Titles':
        output_df = output_df[output_df['full_title'] == selected_title]

    if selected_show != 'All Shows':
        output_df = output_df[output_df['grandparent_title'] == selected_show]

    # Truncate title
    output_df['full_title'] = output_df['full_title'].str.slice(0, max_title_length)

    # Sort
    output_df.sort_values(
        by=['user', 'date_pt', 'ip_address'],
        ascending=[False, True, True],
        inplace=True
    )

    # Drop count and rename columns
    output_df.drop(columns=['count'], inplace=True)
    output_df.rename(
        columns={'full_title': 'title', 'grandparent_title': 'show'},
        inplace=True
    )

    return output_df


def get_earliest_date(df: pd.DataFrame, date_column: str = 'date_pt') -> str:
    """
    Get the earliest date from a DataFrame.

    Args:
        df: Input DataFrame
        date_column: Name of the date column

    Returns:
        Earliest date as string
    """
    return df[date_column].min()


def export_to_csv(df: pd.DataFrame, filename: str) -> None:
    """
    Export DataFrame to CSV file.

    Args:
        df: DataFrame to export
        filename: Output filename
    """
    df.to_csv(filename, index=False)
    print(f"Data exported to {filename}")


def mask_api_key(api_key: str) -> str:
    """
    Mask API key for safe display.

    Args:
        api_key: API key to mask

    Returns:
        Masked API key string
    """
    if len(api_key) > 8:
        return f"{api_key[:4]}...{api_key[-4:]}"
    return "***"


def get_unique_values(df: pd.DataFrame, column: str, sort: bool = True) -> list:
    """
    Get unique values from a DataFrame column.

    Args:
        df: Input DataFrame
        column: Column name
        sort: Whether to sort the values

    Returns:
        List of unique values
    """
    values = df[column].unique().tolist()
    if sort:
        values = sorted(values)
    return values
