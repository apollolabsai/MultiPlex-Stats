#!/usr/bin/env python3
"""
MultiPlex Stats - Web Interface Entry Point

Run this script to start the web application:
    python3 run_multiplex_stats.py

Then open your browser to: http://127.0.0.1:8983
"""

from flask_app import create_app

app = create_app()

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 MultiPlex Stats Web Interface")
    print("="*60)
    print("\n📊 Starting Flask server...")
    print("🌐 Open browser to: http://127.0.0.1:8983")
    print("\n💡 Press CTRL+C to stop the server\n")

    app.run(debug=True, host='0.0.0.0', port=8983)
