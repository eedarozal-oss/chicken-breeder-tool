import json
from datetime import datetime, timedelta, timezone

from services.db.connection import get_connection


def init_market_listing_cache_table():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_listing_cache (
                token_address TEXT NOT NULL,
                page_offset INTEGER NOT NULL,
                page_size INTEGER NOT NULL,
                fetched_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (token_address, page_offset, page_size)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_listing_cache_fetched_at
            ON market_listing_cache(fetched_at)
            """
        )
        conn.commit()


def get_cached_market_listing_page(token_address, page_offset, page_size, max_age_minutes=10):
    token_address = str(token_address or "").strip().lower()
    page_offset = int(page_offset or 0)
    page_size = int(page_size or 50)

    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT fetched_at, payload_json
            FROM market_listing_cache
            WHERE token_address = ?
              AND page_offset = ?
              AND page_size = ?
            LIMIT 1
            """,
            (token_address, page_offset, page_size),
        ).fetchone()

    if not row:
        return None

    try:
        fetched_at = datetime.fromisoformat(str(row["fetched_at"]))
    except Exception:
        return None

    now = datetime.now(timezone.utc)
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    if now - fetched_at > timedelta(minutes=max_age_minutes):
        return None

    try:
        return json.loads(row["payload_json"])
    except Exception:
        return None


def save_cached_market_listing_page(token_address, page_offset, page_size, payload):
    token_address = str(token_address or "").strip().lower()
    page_offset = int(page_offset or 0)
    page_size = int(page_size or 50)
    payload_json = json.dumps(payload or {})

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO market_listing_cache (
                token_address,
                page_offset,
                page_size,
                fetched_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(token_address, page_offset, page_size) DO UPDATE SET
                fetched_at = excluded.fetched_at,
                payload_json = excluded.payload_json
            """,
            (
                token_address,
                page_offset,
                page_size,
                datetime.now(timezone.utc).isoformat(),
                payload_json,
            ),
        )
        conn.commit()
