from datetime import datetime, timezone
from services.db.connection import get_connection


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


def insert_family_root_items(wallet_address: str, token_id: str, roots, owned_root_ids):
    token_id = str(token_id or "").strip()
    if not token_id:
        return

    owned_root_ids = {str(root_id) for root_id in (owned_root_ids or set())}
    root_rows = []

    for root_id in roots or []:
        root_id = str(root_id).strip()
        if not root_id:
            continue

        root_rows.append(
            (
                wallet_address,
                token_id,
                root_id,
                1 if root_id in owned_root_ids else 0,
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
                is_owned_root
            )
            VALUES (?, ?, ?, ?)
            """,
            root_rows,
        )
        conn.commit()
