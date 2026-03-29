import sqlite3
from datetime import datetime, timedelta, timezone

import requests

DB_PATH = "breeder.db"

TARGET_WALLET = "0x9933199fa3d96d7696d2b2a4cfba48d99e47a079"
MIN_AMOUNT_WEI = 100000000000000000  # 0.1 RON
ACCESS_DAYS = 30
SKYNET_TXS_URL = "https://skynet-api.roninchain.com/ronin/explorer/v2/accounts/{wallet}/txs?offset=0&limit=100"


def get_conn():
    return sqlite3.connect(DB_PATH)


def init_wallet_access_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT NOT NULL,
            source TEXT NOT NULL,
            reference TEXT,
            granted_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wallet_access_wallet
        ON wallet_access(wallet_address)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_access_reference
        ON wallet_access(reference)
        WHERE reference IS NOT NULL AND reference != 'manual'
    """)

    conn.commit()
    conn.close()


def is_valid_wallet(wallet: str) -> bool:
    wallet = (wallet or "").strip().lower()
    return wallet.startswith("0x") and len(wallet) == 42


def has_active_access_in_db(wallet: str) -> bool:
    wallet = (wallet or "").strip().lower()
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1
        FROM wallet_access
        WHERE wallet_address = ?
          AND status = 'active'
          AND expires_at > ?
        LIMIT 1
    """, (wallet, now_iso))
    row = cur.fetchone()
    conn.close()

    return row is not None


def has_active_manual_access_in_db(wallet: str) -> bool:
    wallet = (wallet or "").strip().lower()
    now_iso = datetime.now(timezone.utc).isoformat()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1
        FROM wallet_access
        WHERE wallet_address = ?
          AND source = 'manual'
          AND status = 'active'
          AND expires_at > ?
        LIMIT 1
    """, (wallet, now_iso))
    row = cur.fetchone()
    conn.close()

    return row is not None


def access_reference_exists(reference: str) -> bool:
    if not reference:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1
        FROM wallet_access
        WHERE reference = ?
        LIMIT 1
    """, (reference,))
    row = cur.fetchone()
    conn.close()

    return row is not None


def deactivate_old_payment_access(wallet: str):
    wallet = (wallet or "").strip().lower()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE wallet_access
        SET status = 'inactive',
            updated_at = CURRENT_TIMESTAMP
        WHERE wallet_address = ?
          AND source = 'payment'
          AND status = 'active'
    """, (wallet,))
    conn.commit()
    conn.close()


def save_access_record(wallet: str, source: str, reference: str, granted_at: datetime, notes: str = "", duration_days: int = ACCESS_DAYS):
    wallet = wallet.strip().lower()
    duration_days = max(1, int(duration_days or ACCESS_DAYS))
    expires_at = granted_at + timedelta(days=duration_days)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO wallet_access (
            wallet_address, source, reference, granted_at, expires_at, status, notes
        )
        VALUES (?, ?, ?, ?, ?, 'active', ?)
    """, (
        wallet,
        source,
        reference,
        granted_at.isoformat(),
        expires_at.isoformat(),
        notes,
    ))
    conn.commit()
    conn.close()


def get_latest_active_access_expiry(wallet: str):
    wallet = (wallet or "").strip().lower()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT expires_at
        FROM wallet_access
        WHERE wallet_address = ?
          AND status = 'active'
        ORDER BY expires_at DESC
        LIMIT 1
    """, (wallet,))
    row = cur.fetchone()
    conn.close()

    if not row or not row[0]:
        return None

    try:
        return datetime.fromisoformat(row[0])
    except Exception:
        return None


def grant_manual_access(wallet: str, notes: str = "manual access", duration_days: int = ACCESS_DAYS):
    now = datetime.now(timezone.utc)
    current_expiry = get_latest_active_access_expiry(wallet)
    granted_at = current_expiry if current_expiry and current_expiry > now else now
    reference = f"manual:{wallet}:{int(now.timestamp())}"
    save_access_record(wallet, "manual", reference, granted_at, notes, duration_days=duration_days)


def find_latest_qualifying_payment(wallet: str):
    wallet = (wallet or "").strip().lower()

    url = SKYNET_TXS_URL.format(wallet=TARGET_WALLET)
    response = requests.get(url, timeout=20)
    response.raise_for_status()

    payload = response.json()
    items = payload.get("result", {}).get("items", [])

    cutoff = datetime.now(timezone.utc) - timedelta(days=ACCESS_DAYS)
    latest_match = None

    for tx in items:
        tx_from = str(tx.get("from") or "").strip().lower()
        tx_to = str(tx.get("to") or "").strip().lower()
        tx_status = int(tx.get("status") or 0)
        tx_value_hex = str(tx.get("value") or "0x0")
        tx_hash = str(tx.get("transactionHash") or "").strip()

        try:
            tx_value_wei = int(tx_value_hex, 16)
        except ValueError:
            continue

        block_time = tx.get("blockTime")
        if not block_time:
            continue

        tx_time = datetime.fromtimestamp(int(block_time), tz=timezone.utc)

        if tx_time < cutoff:
            continue

        if tx_from != wallet:
            continue

        if tx_to != TARGET_WALLET:
            continue

        if tx_status != 1:
            continue

        if tx_value_wei < MIN_AMOUNT_WEI:
            continue

        candidate = {
            "tx_hash": tx_hash,
            "from": tx_from,
            "to": tx_to,
            "value": tx_value_wei,
            "timestamp": tx_time,
        }

        if latest_match is None or candidate["timestamp"] > latest_match["timestamp"]:
            latest_match = candidate

    return latest_match


def has_wallet_access(wallet: str) -> bool:
    wallet = (wallet or "").strip().lower()

    if not is_valid_wallet(wallet):
        return False

    if has_active_manual_access_in_db(wallet):
        return True

    try:
        latest_tx = find_latest_qualifying_payment(wallet)
    except Exception:
        return has_active_access_in_db(wallet)

    if not latest_tx:
        return has_active_access_in_db(wallet)

    if not access_reference_exists(latest_tx["tx_hash"]):
        deactivate_old_payment_access(wallet)
        save_access_record(
            wallet=wallet,
            source="payment",
            reference=latest_tx["tx_hash"],
            granted_at=latest_tx["timestamp"],
            notes=f"Qualified payment access: {latest_tx['value']} wei",
        )

    return True


def set_authorized_wallet(wallet: str):
    from flask import session
    session["authorized_wallet"] = wallet.strip().lower()


def is_authorized_wallet(wallet: str) -> bool:
    from flask import session
    return session.get("authorized_wallet", "").strip().lower() == (wallet or "").strip().lower()


def get_wallet_access_expiry_display(wallet: str):
    wallet = (wallet or "").strip().lower()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT expires_at
        FROM wallet_access
        WHERE wallet_address = ?
          AND status = 'active'
        ORDER BY expires_at DESC
        LIMIT 1
    """, (wallet,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    expires_at_raw = row[0]
    try:
        expires_at = datetime.fromisoformat(expires_at_raw)
        return expires_at.strftime("%B %d, %Y %I:%M %p UTC")
    except Exception:
        return expires_at_raw
