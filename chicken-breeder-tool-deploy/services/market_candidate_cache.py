from services.db.connection import get_connection

MARKET_CANDIDATE_CACHE_VERSION = 2

def init_market_candidate_cache_table():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chicken_market_candidates (
                token_id TEXT PRIMARY KEY,
                image TEXT,
                breed_count INTEGER DEFAULT 0,
                total_ip INTEGER DEFAULT 0,
                best_build_name TEXT,
                best_build_count INTEGER DEFAULT 0,
                best_build_total INTEGER DEFAULT 0,
                qualifies_ip INTEGER DEFAULT 0,
                qualifies_gene INTEGER DEFAULT 0,
                qualifies_ultimate INTEGER DEFAULT 0,
                source_updated_at TEXT,
                computed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                market_skip INTEGER DEFAULT 0,
                market_skip_reason TEXT,
                market_checked_at TEXT,
                cache_version INTEGER DEFAULT 0
            )
            """
        )

        existing_columns = {
            str(row[1]).strip().lower()
            for row in conn.execute("PRAGMA table_info(chicken_market_candidates)").fetchall()
        }

        if "cache_version" not in existing_columns:
            conn.execute(
                "ALTER TABLE chicken_market_candidates ADD COLUMN cache_version INTEGER DEFAULT 0"
            )
        
        if "market_skip" not in existing_columns:
            conn.execute(
                "ALTER TABLE chicken_market_candidates ADD COLUMN market_skip INTEGER DEFAULT 0"
            )
        if "market_skip_reason" not in existing_columns:
            conn.execute(
                "ALTER TABLE chicken_market_candidates ADD COLUMN market_skip_reason TEXT"
            )
        if "market_checked_at" not in existing_columns:
            conn.execute(
                "ALTER TABLE chicken_market_candidates ADD COLUMN market_checked_at TEXT"
            )
        if "image" not in existing_columns:
            conn.execute(
                "ALTER TABLE chicken_market_candidates ADD COLUMN image TEXT"
            )
        if "breed_count" not in existing_columns:
            conn.execute(
                "ALTER TABLE chicken_market_candidates ADD COLUMN breed_count INTEGER DEFAULT 0"
            )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_candidates_ip
            ON chicken_market_candidates(qualifies_ip, total_ip DESC)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_candidates_gene
            ON chicken_market_candidates(
                qualifies_gene,
                best_build_count DESC,
                best_build_total DESC,
                total_ip DESC
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_candidates_ultimate
            ON chicken_market_candidates(
                qualifies_ultimate,
                best_build_count DESC,
                total_ip DESC,
                best_build_total DESC
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_candidates_source_updated
            ON chicken_market_candidates(source_updated_at)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_candidates_market_skip
            ON chicken_market_candidates(market_skip, market_skip_reason)
            """
        )

        conn.commit()


def get_market_candidate_cache_row(token_id):
    token_id = str(token_id or "").strip()
    if not token_id:
        return None

    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                token_id,
                image,
                breed_count,
                total_ip,
                best_build_name,
                best_build_count,
                best_build_total,
                qualifies_ip,
                qualifies_gene,
                qualifies_ultimate,
                source_updated_at,
                computed_at,
                market_skip,
                market_skip_reason,
                market_checked_at,
                cache_version
            FROM chicken_market_candidates
            WHERE token_id = ?
            LIMIT 1
            """,
            (token_id,),
        ).fetchone()

    return dict(row) if row else None


def delete_market_candidate_cache_row(token_id):
    token_id = str(token_id or "").strip()
    if not token_id:
        return

    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM chicken_market_candidates
            WHERE token_id = ?
            """,
            (token_id,),
        )
        conn.commit()


def upsert_market_candidate_cache_row(row):
    row = dict(row or {})
    token_id = str(row.get("token_id") or "").strip()
    if not token_id:
        return

    payload = {
        "token_id": token_id,
        "image": str(row.get("image") or "").strip() or None,
        "breed_count": int(row.get("breed_count") or 0),
        "total_ip": int(row.get("total_ip") or 0),
        "best_build_name": str(row.get("best_build_name") or "").strip().lower() or None,
        "best_build_count": int(row.get("best_build_count") or 0),
        "best_build_total": int(row.get("best_build_total") or 0),
        "qualifies_ip": 1 if row.get("qualifies_ip") else 0,
        "qualifies_gene": 1 if row.get("qualifies_gene") else 0,
        "qualifies_ultimate": 1 if row.get("qualifies_ultimate") else 0,
        "source_updated_at": str(row.get("source_updated_at") or "").strip() or None,
        "market_skip": int(row.get("market_skip") or 0),
        "market_skip_reason": str(row.get("market_skip_reason") or "").strip() or None,
        "market_checked_at": str(row.get("market_checked_at") or "").strip() or None,
        "cache_version": int(row.get("cache_version") or MARKET_CANDIDATE_CACHE_VERSION),
    }

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO chicken_market_candidates (
                token_id,
                image,
                breed_count,
                total_ip,
                best_build_name,
                best_build_count,
                best_build_total,
                qualifies_ip,
                qualifies_gene,
                qualifies_ultimate,
                source_updated_at,
                computed_at,
                market_skip,
                market_skip_reason,
                market_checked_at,
                cache_version
            )
            VALUES (
                :token_id,
                :image,
                :breed_count,
                :total_ip,
                :best_build_name,
                :best_build_count,
                :best_build_total,
                :qualifies_ip,
                :qualifies_gene,
                :qualifies_ultimate,
                :source_updated_at,
                CURRENT_TIMESTAMP,
                :market_skip,
                :market_skip_reason,
                :market_checked_at,
                :cache_version
            )
            ON CONFLICT(token_id) DO UPDATE SET
                image = excluded.image,
                breed_count = excluded.breed_count,
                total_ip = excluded.total_ip,
                best_build_name = excluded.best_build_name,
                best_build_count = excluded.best_build_count,
                best_build_total = excluded.best_build_total,
                qualifies_ip = excluded.qualifies_ip,
                qualifies_gene = excluded.qualifies_gene,
                qualifies_ultimate = excluded.qualifies_ultimate,
                source_updated_at = excluded.source_updated_at,
                computed_at = CURRENT_TIMESTAMP,
                market_skip = excluded.market_skip,
                market_skip_reason = excluded.market_skip_reason,
                market_checked_at = excluded.market_checked_at,
                cache_version = excluded.cache_version
            """,
            payload,
        )
        conn.commit()


def mark_market_candidate_skipped(token_id, reason):
    token_id = str(token_id or "").strip()
    reason = str(reason or "").strip().lower()
    if not token_id:
        return

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE chicken_market_candidates
            SET market_skip = 1,
                market_skip_reason = ?,
                market_checked_at = CURRENT_TIMESTAMP
            WHERE token_id = ?
            """,
            (reason or "unknown", token_id),
        )
        conn.commit()


def clear_market_candidate_skip(token_id):
    token_id = str(token_id or "").strip()
    if not token_id:
        return

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE chicken_market_candidates
            SET market_skip = 0,
                market_skip_reason = NULL,
                market_checked_at = CURRENT_TIMESTAMP
            WHERE token_id = ?
            """,
            (token_id,),
        )
        conn.commit()
