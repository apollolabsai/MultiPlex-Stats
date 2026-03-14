"""
MDBList API client for fetching ratings metadata.

Free tier: 1,000 requests/day. Each POST to /imdb/movie or /imdb/show counts
as 1 request regardless of batch size (up to 200 IDs per request).
Ratings are fetched for all items with an IMDb ID on every user-triggered sync.

API docs: https://mdblist.docs.apiary.io/
"""
import logging
from datetime import UTC, datetime

from flask_app.utils.http import logged_session

logger = logging.getLogger('multiplex.mdblist')

from flask_app.models import db, CachedMedia, MediaRating

# Sources returned by MDBList that we want to store.
# Note: Rotten Tomatoes audience is now called 'popcorn' in the API.
RATING_SOURCES = {
    'imdb', 'tmdb', 'metacritic', 'metacriticuser',
    'trakt', 'tomatoes', 'popcorn',
    'letterboxd', 'rogerebert', 'myanimelist',
}

_BATCH_SIZE = 200           # MDBList supports up to 200 IDs per POST
_API_BASE = 'https://api.mdblist.com'

# Endpoint per CachedMedia.media_type value
_TYPE_ENDPOINT = {
    'movie': f'{_API_BASE}/imdb/movie',
    'show':  f'{_API_BASE}/imdb/show',
}


class MDBListService:
    """Fetch and cache MDBList ratings for media items."""

    def __init__(self, api_key: str):
        self.api_key = api_key.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich_media_ratings(self, progress_callback=None) -> dict:
        """
        Fetch MDBList ratings for all CachedMedia items that have an imdb_id.

        Movies and TV shows are batched separately because the API uses
        different endpoints for each type.

        Args:
            progress_callback: optional callable(fetched, total) for progress

        Returns:
            dict with keys: total, fetched, failed_batches, ratings_stored
        """
        result = {'total': 0, 'fetched': 0, 'failed_batches': 0, 'ratings_stored': 0}

        if not self.api_key:
            return result

        items_needing_refresh = (
            db.session.query(CachedMedia)
            .filter(
                CachedMedia.imdb_id.isnot(None),
                CachedMedia.media_type.in_(_TYPE_ENDPOINT.keys()),
            )
            .all()
        )

        total = len(items_needing_refresh)
        result['total'] = total
        if total == 0:
            return result

        fetched = 0
        for batch_start in range(0, total, _BATCH_SIZE):
            batch = items_needing_refresh[batch_start:batch_start + _BATCH_SIZE]

            # Split batch by media type — each type uses a different endpoint
            by_type: dict[str, list] = {}
            for item in batch:
                by_type.setdefault(item.media_type, []).append(item)

            for media_type, items in by_type.items():
                id_to_item = {item.imdb_id: item for item in items}
                api_results, failed = self._fetch_batch(media_type, list(id_to_item.keys()))

                if failed:
                    result['failed_batches'] += 1

                for api_result in api_results:
                    imdb_id = (api_result.get('ids') or {}).get('imdb')
                    if not imdb_id or imdb_id not in id_to_item:
                        continue
                    stored = self._upsert_ratings(id_to_item[imdb_id].id, api_result.get('ratings', []))
                    result['ratings_stored'] += stored

            db.session.commit()
            fetched += len(batch)
            result['fetched'] = fetched

            if progress_callback:
                progress_callback(fetched, total)

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_batch(self, media_type: str, imdb_ids: list) -> tuple[list, bool]:
        """POST a batch of IMDb IDs to the appropriate MDBList endpoint.

        Returns:
            (results, failed) where failed is True if the request did not succeed.
        """
        endpoint = _TYPE_ENDPOINT.get(media_type)
        if not endpoint:
            return [], False
        kind = 'movies' if media_type == 'movie' else 'TV shows'
        logger.info('MDBList Fetching ratings for %d %s', len(imdb_ids), kind)
        try:
            response = logged_session.post(
                endpoint,
                params={'apikey': self.api_key},
                json={'ids': imdb_ids, 'append_to_response': ['ratings']},
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    return data, False
                if isinstance(data, dict) and 'ids' in data:
                    return [data], False
                return [], False
            # Non-200 response — log it
            snippet = response.text[:500] if response.text else '(empty body)'
            logger.warning(
                "MDBList ratings fetch failed for %d %s — HTTP %s: %s",
                len(imdb_ids), kind, response.status_code, snippet,
            )
            return [], True
        except Exception as e:
            logger.error("MDBList ratings fetch error for %d %s: %s", len(imdb_ids), kind, e)
            return [], True

    def _upsert_ratings(self, cached_media_id: int, ratings: list) -> int:
        """Insert or update MediaRating rows for a single media item.

        Returns:
            Number of rating rows written (inserted or updated).
        """
        now = datetime.now(UTC)
        written = 0

        for rating_data in ratings:
            source = rating_data.get('source', '').lower()
            if source not in RATING_SOURCES:
                continue

            value = rating_data.get('value')
            score = rating_data.get('score')
            votes = rating_data.get('votes')
            url = rating_data.get('url')
            popular = rating_data.get('popular')

            # Skip sources where every numeric field is None — no useful data.
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
            written += 1

        return written
