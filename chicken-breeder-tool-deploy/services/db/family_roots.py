from datetime import datetime, timezone
from services.db.connection import get_connection

from datetime import datetime, timedelta, timezone

def clear_stale_family_root_summaries(wallet_address: str, max_age_hours: int = 24):
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE chicken_family_roots
            SET owned_root_count = 0,
                total_root_count = 0,
                ownership_percent = 0,
                is_complete = 0,
                root_check_target_count = 0,
                pending_root_check_count = 0,
                last_updated = NULL
            WHERE wallet_address = ?
              AND last_updated IS NOT NULL
              AND last_updated < ?
            """,
            (wallet_address, cutoff_iso),
        )
        conn.commit()
        
def clear_family_roots_for_wallet(wallet_address: str):
    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM chicken_family_root_items
            WHERE wallet_address = ?
            """,
            (wallet_address,),
        )
        conn.execute(
            """
            DELETE FROM chicken_family_roots
            WHERE wallet_address = ?
            """,
            (wallet_address,),
        )
        conn.commit()


def clear_family_roots_for_token(wallet_address: str, token_id: str):
    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM chicken_family_root_items
            WHERE wallet_address = ? AND token_id = ?
            """,
            (wallet_address, str(token_id)),
        )
        conn.execute(
            """
            DELETE FROM chicken_family_roots
            WHERE wallet_address = ? AND token_id = ?
            """,
            (wallet_address, str(token_id)),
        )
        conn.commit()


def upsert_family_root_summary(wallet_address: str, summary: dict):
    now_utc = datetime.now(timezone.utc).isoformat()

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO chicken_family_roots (
                wallet_address,
                token_id,
                owned_root_count,
                total_root_count,
                ownership_percent,
                is_complete,
                root_check_target_count,
                pending_root_check_count,
                last_updated
            )
            VALUES (
                :wallet_address,
                :token_id,
                :owned_root_count,
                :total_root_count,
                :ownership_percent,
                :is_complete,
                :root_check_target_count,
                :pending_root_check_count,
                :last_updated
            )
            ON CONFLICT(wallet_address, token_id) DO UPDATE SET
                owned_root_count = excluded.owned_root_count,
                total_root_count = excluded.total_root_count,
                ownership_percent = excluded.ownership_percent,
                is_complete = excluded.is_complete,
                root_check_target_count = excluded.root_check_target_count,
                pending_root_check_count = excluded.pending_root_check_count,
                last_updated = excluded.last_updated
            """,
            {
                "wallet_address": wallet_address,
                "token_id": str(summary.get("token_id") or ""),
                "owned_root_count": int(summary.get("owned_root_count") or 0),
                "total_root_count": int(summary.get("total_root_count") or 0),
                "ownership_percent": float(summary.get("ownership_percent") or 0),
                "is_complete": int(summary.get("is_complete") or 0),
                "root_check_target_count": int(summary.get("root_check_target_count") or 0),
                "pending_root_check_count": int(summary.get("pending_root_check_count") or 0),
                "last_updated": now_utc,
            },
        )
        conn.commit()


def insert_family_root_items(
    wallet_address: str,
    token_id: str,
    roots,
    owned_root_ids,
    root_status_map=None,
):
    token_id = str(token_id or "").strip()
    if not token_id:
        return

    owned_root_ids = {str(root_id) for root_id in (owned_root_ids or set())}
    root_status_map = root_status_map or {}
    root_rows = []

    for root_id in roots or []:
        root_id = str(root_id).strip()
        if not root_id:
            continue

        item = root_status_map.get(root_id, {})
        root_rows.append(
            (
                wallet_address,
                token_id,
                root_id,
                1 if root_id in owned_root_ids else 0,
                item.get("root_check_status", "unchecked"),
                1 if item.get("is_dead_root") else 0,
                item.get("last_checked_at"),
            )
        )

    if not root_rows:
        return

    with get_connection() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO chicken_family_root_items (
                wallet_address,
                token_id,
                root_token_id,
                is_owned_root,
                root_check_status,
                is_dead_root,
                last_checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            root_rows,
        )
        conn.commit()


def get_family_root_items(wallet_address: str, token_id: str):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                wallet_address,
                token_id,
                root_token_id,
                is_owned_root,
                root_check_status,
                is_dead_root,
                last_checked_at
            FROM chicken_family_root_items
            WHERE wallet_address = ? AND token_id = ?
            ORDER BY CAST(root_token_id AS INTEGER)
            """,
            (wallet_address, str(token_id)),
        ).fetchall()

    return [dict(row) for row in rows]

def upsert_family_root_item(
    wallet_address: str,
    token_id: str,
    root_token_id: str,
    is_owned_root: int = 0,
    root_check_status: str = "unchecked",
    is_dead_root: int = 0,
    last_checked_at: str = None,
):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO chicken_family_root_items (
                wallet_address,
                token_id,
                root_token_id,
                is_owned_root,
                root_check_status,
                is_dead_root,
                last_checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet_address, token_id, root_token_id) DO UPDATE SET
                is_owned_root = excluded.is_owned_root,
                root_check_status = excluded.root_check_status,
                is_dead_root = excluded.is_dead_root,
                last_checked_at = excluded.last_checked_at
            """,
            (
                wallet_address,
                str(token_id),
                str(root_token_id),
                int(is_owned_root or 0),
                root_check_status,
                int(is_dead_root or 0),
                last_checked_at,
            ),
        )
        conn.commit()
