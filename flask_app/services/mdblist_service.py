"""
MDBList API client for fetching ratings metadata.

Free tier: 1,000 requests/day (each POST /imdb/movie call counts as 1 request
regardless of batch size). Ratings are refreshed at most every 3 months and
only during a user-triggered media sync.

API docs: https://linaspurinis.github.io/api.mdblist.com/
"""
from datetime import UTC, datetime, timedelta

import requests

from flask_app.models import db, CachedMedia, MediaRating

# Sources returned by MDBList that we want to store.
RATING_SOURCES = {
    'imdb', 'tmdb', 'metacritic', 'metacriticuser',
    'trakt', 'tomatoes', 'tomatoesaudience',
    'letterboxd', 'rogerebert', 'myanimelist',
}

_BATCH_SIZE = 100           # MDBList supports up to 100 IMDb IDs per POST
_REFRESH_DAYS = 90          # Refresh ratings older than 3 months
_API_URL = 'https://mdblist.com/api/'


class MDBListService:
    """Fetch and cache MDBList ratings for media items."""

    def __init__(self, api_key: str):
        self.api_key = api_key.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich_media_ratings(self, progress_callback=None):
        """
        Fetch MDBList ratings for all CachedMedia items that have an imdb_id.

        Items are skipped if their ratings were updated within the last
        _REFRESH_DAYS days.

        Args:
            progress_callback: optional callable(fetched, total) for progress
        """
        if not self.api_key:
            return

        cutoff = datetime.now(UTC) - timedelta(days=_REFRESH_DAYS)

        # Collect items that need enrichment.
        # A media item needs enrichment if it has an imdb_id AND either:
        #   - it has no ratings at all, or
        #   - its newest rating is older than the cutoff.
        from sqlalchemy import func, not_, exists
        from sqlalchemy.orm import aliased

        # Subquery: newest rating date per cached_media_id
        newest_rating = (
            db.session.query(
                MediaRating.cached_media_id,
                func.max(MediaRating.updated_at).label('newest')
            )
            .group_by(MediaRating.cached_media_id)
            .subquery()
        )

        items_needing_refresh = (
            db.session.query(CachedMedia)
            .outerjoin(newest_rating, CachedMedia.id == newest_rating.c.cached_media_id)
            .filter(
                CachedMedia.imdb_id.isnot(None),
                db.or_(
                    newest_rating.c.newest.is_(None),
                    newest_rating.c.newest < cutoff,
                )
            )
            .all()
        )

        total = len(items_needing_refresh)
        if total == 0:
            return

        # Group into batches of _BATCH_SIZE
        fetched = 0
        for batch_start in range(0, total, _BATCH_SIZE):
            batch = items_needing_refresh[batch_start:batch_start + _BATCH_SIZE]
            imdb_ids = [item.imdb_id for item in batch]
            id_to_item = {item.imdb_id: item for item in batch}

            api_results = self._fetch_batch(imdb_ids)

            for result in api_results:
                imdb_id = result.get('imdbid')
                if not imdb_id or imdb_id not in id_to_item:
                    continue
                media_item = id_to_item[imdb_id]
                self._upsert_ratings(media_item.id, result.get('ratings', []))

            db.session.commit()
            fetched += len(batch)

            if progress_callback:
                progress_callback(fetched, total)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_batch(self, imdb_ids: list) -> list:
        """POST a batch of IMDb IDs to MDBList and return the result list."""
        try:
            response = requests.post(
                _API_URL,
                params={
                    'apikey': self.api_key,
                    'append_to_response': 'ratings',
                },
                json={'imdbids': imdb_ids},
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                # Batch endpoint returns a list directly.
                if isinstance(data, list):
                    return data
                # Single-item fallback (non-batch response).
                if isinstance(data, dict) and 'imdbid' in data:
                    return [data]
            return []
        except Exception as e:
            print(f"MDBList batch fetch error: {e}")
            return []

    def _upsert_ratings(self, cached_media_id: int, ratings: list):
        """Insert or update MediaRating rows for a single media item."""
        now = datetime.now(UTC)

        for rating_data in ratings:
            source = rating_data.get('source', '').lower()
            if source not in RATING_SOURCES:
                continue

            value = rating_data.get('value')
            score = rating_data.get('score')
            votes = rating_data.get('votes')
            url = rating_data.get('url')
            popular = rating_data.get('popular')

            # Skip sources where every field is None/null — no useful data.
            if value is None and score is None and votes is None:
                continue

            existing = MediaRating.query.filter_by(
                cached_media_id=cached_media_id,
                source=source,
            ).first()

            if existing:
                existing.value = value
                existing.score = score
                existing.votes = votes
                existing.url = url
                existing.popular = popular
                existing.updated_at = now
            else:
                db.session.add(MediaRating(
                    cached_media_id=cached_media_id,
                    source=source,
                    value=value,
                    score=score,
                    votes=votes,
                    url=url,
                    popular=popular,
                    updated_at=now,
                ))
