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


def get_cached_ninuno_roots_by_token_ids(token_ids):
    token_ids = [str(token_id).strip() for token_id in (token_ids or []) if str(token_id).strip()]
    if not token_ids:
        return {}

    placeholders = ",".join(["?"] * len(token_ids))

    with get_connection() as conn:
        roots_table_exists = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'chicken_ninuno_roots'
            """
        ).fetchone()

        if not roots_table_exists:
            return {}

        static_table_exists = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'chicken_static'
            """
        ).fetchone()

        if static_table_exists:
            rows = conn.execute(
                f"""
                SELECT
                    nr.token_id,
                    nr.root_token_id,
                    nr.is_complete,
                    COALESCE(cs.is_dead, 0) AS root_is_dead
                FROM chicken_ninuno_roots nr
                LEFT JOIN chicken_static cs
                    ON cs.token_id = nr.root_token_id
                WHERE nr.token_id IN ({placeholders})
                ORDER BY CAST(nr.token_id AS INTEGER), CAST(nr.root_token_id AS INTEGER)
                """,
                token_ids,
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT
                    nr.token_id,
                    nr.root_token_id,
                    nr.is_complete,
                    0 AS root_is_dead
                FROM chicken_ninuno_roots nr
                WHERE nr.token_id IN ({placeholders})
                ORDER BY CAST(nr.token_id AS INTEGER), CAST(nr.root_token_id AS INTEGER)
                """,
                token_ids,
            ).fetchall()

    grouped = {}
    for row in rows:
        token_id = str(row["token_id"] or "").strip()
        if not token_id:
            continue
        grouped.setdefault(token_id, []).append(dict(row))

    return grouped

def build_family_root_summary_from_items(token_id, root_items, owned_token_ids):
    alive_roots = []
    dead_roots = []
    pending_root_check_count = 0
    root_check_target_count = 0

    owned_token_ids = {str(token_id) for token_id in (owned_token_ids or set())}

    for item in root_items or []:
        root_id = str(item.get("root_token_id") or "").strip()
        status = str(item.get("root_check_status") or "").strip().lower()
        is_dead_root = int(item.get("is_dead_root") or 0)

        if not root_id:
            continue

        if status in {"alive_checked", "pending"}:
            root_check_target_count += 1

        if status == "dead_checked" or is_dead_root:
            dead_roots.append(root_id)
            continue

        if status == "pending":
            pending_root_check_count += 1

        alive_roots.append(root_id)

    owned_roots = [root for root in alive_roots if root in owned_token_ids]

    total_root_count = len(alive_roots)
    owned_root_count = len(owned_roots)

    ownership_percent = 0.0
    if total_root_count > 0:
        ownership_percent = round((owned_root_count / total_root_count) * 100, 2)

    return {
        "token_id": str(token_id),
        "owned_root_count": owned_root_count,
        "total_root_count": total_root_count,
        "ownership_percent": ownership_percent,
        "is_complete": 1 if pending_root_check_count == 0 else 0,
        "roots": alive_roots,
        "dead_roots": dead_roots,
        "root_check_target_count": root_check_target_count,
        "pending_root_check_count": pending_root_check_count,
    }

def preload_cached_family_roots_for_wallet(chickens, wallet_address):
    chickens = chickens or []
    owned_token_ids = {
        str(row.get("token_id") or "").strip()
        for row in chickens
        if str(row.get("token_id") or "").strip()
    }
    breedable = [
        row for row in chickens
        if not row.get("is_egg") and str(row.get("state") or "").strip().lower() == "normal"
    ]
    token_ids = [
        str(row.get("token_id") or "").strip()
        for row in breedable
        if str(row.get("token_id") or "").strip()
    ]

    cached_lookup = get_cached_ninuno_roots_by_token_ids(token_ids)
    if not cached_lookup:
        return {"loaded": 0, "tokens": []}

    now_utc = datetime.now(timezone.utc).isoformat()
    loaded_tokens = []

    for token_id in token_ids:
        cached_rows = cached_lookup.get(token_id) or []
        if not cached_rows:
            continue

        root_ids = []
        root_status_map = {}
        token_is_complete = True

        for row in cached_rows:
            root_id = str(row.get("root_token_id") or "").strip()
            if not root_id:
                continue

            if root_id not in root_ids:
                root_ids.append(root_id)

            row_is_complete = int(row.get("is_complete") or 0)
            if row_is_complete != 1:
                token_is_complete = False

            if root_id in owned_token_ids:
                root_status_map[root_id] = {
                    "root_check_status": "skipped",
                    "is_dead_root": False,
                    "last_checked_at": now_utc,
                }
            elif int(row.get("root_is_dead") or 0):
                root_status_map[root_id] = {
                    "root_check_status": "dead_checked",
                    "is_dead_root": True,
                    "last_checked_at": now_utc,
                }
            else:
                root_status_map[root_id] = {
                    "root_check_status": "alive_checked",
                    "is_dead_root": False,
                    "last_checked_at": now_utc,
                }

        if not root_ids:
            continue

        insert_family_root_items(
            wallet_address=wallet_address,
            token_id=token_id,
            roots=root_ids,
            owned_root_ids=owned_token_ids,
            root_status_map=root_status_map,
        )

        stored_items = get_family_root_items(wallet_address, token_id)
        summary = build_family_root_summary_from_items(
            token_id=token_id,
            root_items=stored_items,
            owned_token_ids=owned_token_ids,
        )

        if not token_is_complete:
            summary["is_complete"] = 0

        upsert_family_root_summary(wallet_address, summary)
        loaded_tokens.append(token_id)

    return {
        "loaded": len(loaded_tokens),
        "tokens": loaded_tokens,
    }

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
