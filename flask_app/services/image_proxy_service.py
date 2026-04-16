"""
Helpers for serving Tautulli-backed images through the Flask app.
"""
from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

from flask_app.models import ServerConfig
from flask_app.services.utils import to_int
from multiplex_stats import TautulliClient

logger = logging.getLogger(__name__)


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
    def _build_fetch_candidates(image_path: str, rating_key: Any) -> list[tuple[str, int | None]]:
        """Build ordered fallback candidates for proxy fetches."""
        resolved_image_path = str(image_path or '').strip()
        resolved_rating_key = to_int(rating_key)

        candidates: list[tuple[str, int | None]] = []
        seen: set[tuple[str, int | None]] = set()

        def add_candidate(path: str, key: int | None) -> None:
            normalized_path = str(path or '').strip()
            candidate = (normalized_path, key)
            if not normalized_path or candidate in seen:
                return
            seen.add(candidate)
            candidates.append(candidate)

        add_candidate(resolved_image_path, resolved_rating_key)

        canonical_path = ''
        if resolved_rating_key is not None:
            canonical_path = f'/library/metadata/{resolved_rating_key}/thumb'
            add_candidate(canonical_path, resolved_rating_key)

        add_candidate(resolved_image_path, None)
        if canonical_path:
            add_candidate(canonical_path, None)

        return candidates

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
        resolved_fallback = (fallback or 'poster').strip() or 'poster'

        last_exc: Exception | None = None
        candidates = ImageProxyService._build_fetch_candidates(resolved_image_path, rating_key)
        for candidate_path, candidate_rating_key in candidates:
            try:
                image_bytes, content_type = client.pms_image_proxy(
                    img=candidate_path,
                    width=width,
                    height=height,
                    fallback=resolved_fallback,
                    rating_key=candidate_rating_key,
                )
                if str(content_type or '').lower().startswith('image/'):
                    return image_bytes, content_type
                last_exc = ValueError(f'Unexpected upstream content type: {content_type}')
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Image proxy fetch failed for %s using %s (rating_key=%s): %s",
                    resolved_server_name,
                    candidate_path,
                    candidate_rating_key,
                    exc,
                )

        if last_exc is not None:
            raise last_exc

        raise LookupError(f'Unable to fetch image for {resolved_server_name}')
