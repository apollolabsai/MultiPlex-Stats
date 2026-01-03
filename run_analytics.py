"""
MultiPlex Stats - Main Analytics Script

This script:
1. Loads server configuration from config.ini
2. Fetches data from Tautulli API
3. Processes and analyzes the data
4. Creates interactive visualizations
5. Generates a unified dashboard.html
"""

from multiplex_stats import TautulliClient
from multiplex_stats.config_loader import load_config
from multiplex_stats.data_processing import (
    process_daily_data,
    process_monthly_data,
    process_history_data,
    aggregate_user_stats,
    aggregate_movie_stats,
    aggregate_tv_stats,
)
from multiplex_stats.visualization import (
    create_daily_bar_chart,
    create_monthly_bar_chart,
    create_user_bar_chart,
    create_movie_bar_chart,
    create_tv_bar_chart,
    create_category_pie_chart,
    create_server_pie_chart,
)
from datetime import datetime


def main():
    """Main function demonstrating package usage."""

    # ==========================================
    # 1. Load Configuration
    # ==========================================
    print("Loading configuration...")

    try:
        server_a, server_b, settings = load_config()
    except ValueError as e:
        print(f"\n❌ Configuration Error:\n{e}\n")
        return

    if server_b:
        print(f"✓ Loaded configuration for {server_a.name} and {server_b.name}")
    else:
        print(f"✓ Loaded configuration for {server_a.name} (single server mode)")
    print(f"✓ Trend Charts: {settings.daily_trend_days} days (daily), {settings.monthly_trend_months} months (monthly)")
    print(f"✓ History Analysis: {settings.history_days} days")
    print(f"✓ Top Items: {settings.top_movies} movies, {settings.top_tv_shows} shows, {settings.top_users} users")

    # Create API clients
    client_a = TautulliClient(server_a)
    client_b = TautulliClient(server_b) if server_b else None

    # ==========================================
    # 2. Fetch Daily Data
    # ==========================================
    print("\nFetching daily play data...")

    daily_data_a = client_a.get_plays_by_date(time_range=settings.daily_trend_days)
    daily_data_b = client_b.get_plays_by_date(time_range=settings.daily_trend_days) if client_b else None

    # Process daily data
    df_daily = process_daily_data(
        daily_data_a,
        daily_data_b,
        server_a.name,
        server_b.name if server_b else None
    )

    print(f"Daily data shape: {df_daily.shape}")
    print(df_daily.head())

    # Create daily visualization
    fig_daily = create_daily_bar_chart(
        df_daily,
        server_a.name,
        server_b.name if server_b else None
    )

    # ==========================================
    # 3. Fetch Monthly Data
    # ==========================================
    print("\nFetching monthly play data...")

    monthly_data_a = client_a.get_plays_per_month(time_range=settings.monthly_trend_months)
    monthly_data_b = client_b.get_plays_per_month(time_range=settings.monthly_trend_months) if client_b else None

    # Process monthly data
    df_monthly = process_monthly_data(
        monthly_data_a,
        monthly_data_b,
        server_a.name,
        server_b.name if server_b else None
    )

    print(f"Monthly data shape: {df_monthly.shape}")

    # Create monthly visualization
    fig_monthly = create_monthly_bar_chart(
        df_monthly,
        server_a.name,
        server_b.name if server_b else None
    )

    # ==========================================
    # 4. Fetch and Analyze History Data
    # ==========================================
    print("\nFetching play history...")

    history_days = settings.history_days
    history_data_a = client_a.get_history(days=history_days)
    history_data_b = client_b.get_history(days=history_days) if client_b else None

    # Process history data
    df_history = process_history_data(
        history_data_a,
        history_data_b,
        server_a.name,
        server_b.name if server_b else None
    )

    print(f"History data shape: {df_history.shape}")

    # Aggregate user statistics
    df_users = aggregate_user_stats(df_history, top_n=settings.top_users)
    print(f"\nTop {min(5, len(df_users))} users by play count:")
    print(df_users.head())

    # Create user visualization
    fig_users = create_user_bar_chart(df_users, history_days)

    # ==========================================
    # 5. Analyze Top Movies and TV Shows
    # ==========================================
    print("\nAnalyzing top content...")

    # Top movies
    df_movies = aggregate_movie_stats(df_history, top_n=settings.top_movies)
    print(f"\nTop 5 movies:")
    print(df_movies.head())

    fig_movies = create_movie_bar_chart(df_movies, history_days)

    # Top TV shows
    df_tv = aggregate_tv_stats(df_history, top_n=settings.top_tv_shows)
    print(f"\nTop 5 TV shows:")
    print(df_tv.head())

    fig_tv = create_tv_bar_chart(df_tv, history_days)

    # ==========================================
    # 6. Create Distribution Charts
    # ==========================================
    print("\nCreating distribution charts...")

    # Category pie chart
    fig_category = create_category_pie_chart(df_daily, history_days)

    # Server distribution
    fig_server = create_server_pie_chart(
        df_daily,
        server_a.name,
        server_b.name if server_b else None,
        history_days
    )

    # ==========================================
    # 7. Export Data to CSV
    # ==========================================
    print("\nExporting data to CSV...")

    df_history.to_csv("history_data.csv", index=False)
    df_users.to_csv("user_stats.csv", index=False)
    df_movies.to_csv("movie_stats.csv", index=False)
    df_tv.to_csv("tv_stats.csv", index=False)

    print("\nAll data exported successfully!")


    # ==========================================
    # 8. Create Unified Dashboard
    # ==========================================
    print("\n" + "="*50)
    print("Creating unified dashboard...")
    print("="*50)

    # Get HTML for each figure
    daily_html = fig_daily.to_html(full_html=False, include_plotlyjs=False)
    monthly_html = fig_monthly.to_html(full_html=False, include_plotlyjs=False)
    users_html = fig_users.to_html(full_html=False, include_plotlyjs=False)
    movies_html = fig_movies.to_html(full_html=False, include_plotlyjs=False)
    tv_html = fig_tv.to_html(full_html=False, include_plotlyjs=False)
    category_html = fig_category.to_html(full_html=False, include_plotlyjs=False)
    server_html = fig_server.to_html(full_html=False, include_plotlyjs=False)

    # Calculate summary statistics
    total_plays = len(df_history)
    total_users = len(df_users)
    total_movies = len(df_movies)
    total_tv = len(df_tv)
    server_a_plays = len(df_history[df_history['Server'] == server_a.name])
    server_b_plays = len(df_history[df_history['Server'] == server_b.name]) if server_b else 0

    # Get current timestamp
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Build server-specific stats cards
    if server_b:
        server_stats_html = f"""
            <div class="stat-card">
                <div class="stat-number">{server_a_plays:,}</div>
                <div class="stat-label">{server_a.name} Plays</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{server_b_plays:,}</div>
                <div class="stat-label">{server_b.name} Plays</div>
            </div>"""
        footer_servers = f"{server_a.name} & {server_b.name}"
    else:
        server_stats_html = f"""
            <div class="stat-card">
                <div class="stat-number">{server_a_plays:,}</div>
                <div class="stat-label">{server_a.name} Plays</div>
            </div>"""
        footer_servers = server_a.name

    # Build complete HTML
    dashboard_html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MultiPlex Stats - Analytics Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #1e1e1e 0%, #2d2d2d 100%);
            color: #ffffff;
            padding: 20px;
            min-height: 100vh;
        }}

        .container {{
            max-width: 1800px;
            margin: 0 auto;
        }}

        header {{
            text-align: center;
            padding: 40px 20px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 15px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }}

        h1 {{
            font-size: 3em;
            margin-bottom: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}

        .subtitle {{
            font-size: 1.2em;
            color: #aaa;
            margin-bottom: 30px;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .stat-card {{
            background: rgba(255, 255, 255, 0.05);
            padding: 25px;
            border-radius: 12px;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }}

        .stat-number {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }}

        .stat-label {{
            font-size: 0.9em;
            color: #aaa;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .section {{
            margin-bottom: 40px;
        }}

        .section-title {{
            font-size: 1.8em;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid rgba(255, 255, 255, 0.1);
            color: #667eea;
        }}

        .chart-container {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid rgba(255, 255, 255, 0.05);
        }}

        .grid-2 {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}

        footer {{
            text-align: center;
            padding: 30px;
            color: #666;
            font-size: 0.9em;
            margin-top: 50px;
        }}

        .timestamp {{
            color: #888;
            font-size: 0.9em;
            margin-top: 10px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 MultiPlex Stats</h1>
            <div class="subtitle">Plex Server Analytics & Insights</div>
            <div class="timestamp">Last {history_days} Days | Generated: {current_time}</div>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{total_plays:,}</div>
                <div class="stat-label">Total Plays</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{total_users}</div>
                <div class="stat-label">Active Users</div>
            </div>
            {server_stats_html}
            <div class="stat-card">
                <div class="stat-number">{total_movies}</div>
                <div class="stat-label">Unique Movies</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{total_tv}</div>
                <div class="stat-label">Unique TV Shows</div>
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">📈 Activity Trends</h2>
            <div class="chart-container">
                {daily_html}
            </div>
            <div class="chart-container">
                {monthly_html}
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">🍕 Distribution</h2>
            <div class="grid-2">
                <div class="chart-container">
                    {category_html}
                </div>
                <div class="chart-container">
                    {server_html}
                </div>
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">👥 User Activity</h2>
            <div class="chart-container">
                {users_html}
            </div>
        </div>

        <div class="section">
            <h2 class="section-title">🎬 Top Content</h2>
            <div class="chart-container">
                {movies_html}
            </div>
            <div class="chart-container">
                {tv_html}
            </div>
        </div>

        <footer>
            <p>Generated with MultiPlex Stats</p>
            <p>Powered by Plotly • Data from {footer_servers}</p>
        </footer>
    </div>
</body>
</html>
"""

    # Save the dashboard
    with open('dashboard.html', 'w', encoding='utf-8') as f:
        f.write(dashboard_html)

    print("\n" + "="*50)
    print("✅ SUCCESS!")
    print("="*50)
    print("\n📊 Dashboard created: dashboard.html")
    print("📁 CSV data exported: history_data.csv, user_stats.csv, etc.")
    print("\n💡 To view dashboard:")
    print("   → Double-click 'dashboard.html' in Finder")
    print("   → Or run: open dashboard.html")
    print("\n" + "="*50)


if __name__ == "__main__":
    main()
