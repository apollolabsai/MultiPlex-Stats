"""
Helpers for serving Tautulli-backed images through the Flask app.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from flask_app.models import ServerConfig
from flask_app.services.utils import to_int
from multiplex_stats import TautulliClient


class ImageProxyService:
    """Build and fetch authenticated image proxy URLs."""

    @staticmethod
    def build_url(
        server_name: str,
        image_path: str | None,
        width: int,
        height: int,
        fallback: str = 'poster',
        rating_key: Any = None,
    ) -> str:
        """Build a local image proxy URL for browser consumption."""
        if not server_name or not image_path:
            return ''

        params: dict[str, Any] = {
            'server': server_name,
            'img': image_path,
            'width': width,
            'height': height,
            'fallback': fallback,
        }
        parsed_rating_key = to_int(rating_key)
        if parsed_rating_key is not None:
            params['rating_key'] = parsed_rating_key
        return f"/api/image-proxy?{urlencode(params)}"

    @staticmethod
    def fetch_image(
        server_name: str,
        image_path: str,
        width: int,
        height: int,
        fallback: str = 'poster',
        rating_key: Any = None,
    ) -> tuple[bytes, str]:
        """Fetch image bytes from the configured Tautulli server."""
        resolved_server_name = str(server_name or '').strip()
        resolved_image_path = str(image_path or '').strip()
        if not resolved_server_name:
            raise ValueError('Missing server.')
        if not resolved_image_path:
            raise ValueError('Missing image path.')

        server = ServerConfig.query.filter_by(name=resolved_server_name, is_active=True).first()
        if not server:
            raise LookupError(f'Configured server not found: {resolved_server_name}')

        client = TautulliClient(server.to_multiplex_config())
        return client.pms_image_proxy(
            img=resolved_image_path,
            width=width,
            height=height,
            fallback=(fallback or 'poster').strip() or 'poster',
            rating_key=rating_key,
        )
