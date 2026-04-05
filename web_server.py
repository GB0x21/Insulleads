#!/usr/bin/env python3
"""
web_server.py — Production web server for Insulleads dashboard

Starts Flask API with static file serving.
Runs on port 5000 by default (use PORT env var to change).

Usage:
  python web_server.py              # Development server
  gunicorn -w 4 -b 0.0.0.0:5000 web_server:app  # Production
"""

import os
import sys
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("web_server")

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import and initialize Flask app
from web.app import create_app

app = create_app()

# Serve static files (login.html, index.html)
from flask import send_file, send_from_directory

template_dir = PROJECT_ROOT / "web" / "templates"


@app.route('/', methods=['GET'])
def serve_dashboard():
    """Serve main dashboard (protected by auth)."""
    return send_file(template_dir / 'index.html')


@app.route('/login.html', methods=['GET'])
def serve_login():
    """Serve login page."""
    return send_file(template_dir / 'login.html')


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'

    logger.info(f"🚀 Starting Insulleads Web Server on port {port}")
    logger.info(f"   Dashboard: http://localhost:{port}/")
    logger.info(f"   Login: http://localhost:{port}/login.html")
    logger.info(f"   API: http://localhost:{port}/api/")

    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug,
        use_reloader=debug
    )
