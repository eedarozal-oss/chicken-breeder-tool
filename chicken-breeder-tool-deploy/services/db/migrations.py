import json
import os
import sqlite3

from services.db.connection import DB_PATH, get_connection
from services.gene_classifier import classify_gene_profile


BUILD_JSON_COMPACTION_KEY = "compact_chicken_build_json_v1"
BUILD_JSON_BLOAT_THRESHOLD = 1_000_000

BUILD_JSON_FIELDS = [
    "primary_build_matched_slots",
    "primary_build_missing_slots",
    "primary_build_evaluations",
    "recessive_build_matched_slots",
    "recessive_build_missing_slots",
    "recessive_build_evaluations",
]

CHICKEN_CLASSIFICATION_SOURCE_FIELDS = [
    "token_id",
    "ip",
    "instinct",
    "beak",
    "comb",
    "eyes",
    "feet",
    "wings",
    "tail",
    "body",
    "beak_h1",
    "comb_h1",
    "eyes_h1",
    "feet_h1",
    "wings_h1",
    "tail_h1",
    "body_h1",
]


def ensure_column(conn, table_name: str, column_name: str, column_def: str):
    existing = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_names = {row[1] for row in existing}

    if column_name not in existing_names:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def ensure_migration_state_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_migration_state (
            key TEXT PRIMARY KEY,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def migration_state_exists(conn, key):
    row = conn.execute(
        """
        SELECT 1
        FROM app_migration_state
        WHERE key = ?
        """,
        (key,),
    ).fetchone()
    return bool(row)


def mark_migration_state(conn, key):
    conn.execute(
        """
        INSERT OR REPLACE INTO app_migration_state (key, applied_at)
        VALUES (?, CURRENT_TIMESTAMP)
        """,
        (key,),
    )


def oversized_build_json_exists(conn):
    checks = " OR ".join([f"LENGTH({field}) > ?" for field in BUILD_JSON_FIELDS])
    params = [BUILD_JSON_BLOAT_THRESHOLD] * len(BUILD_JSON_FIELDS)
    row = conn.execute(
        f"""
        SELECT 1
        FROM chickens
        WHERE {checks}
        LIMIT 1
        """,
        params,
    ).fetchone()
    return bool(row)


def compact_chicken_build_json(conn):
    if migration_state_exists(conn, BUILD_JSON_COMPACTION_KEY):
        return False

    if not oversized_build_json_exists(conn):
        mark_migration_state(conn, BUILD_JSON_COMPACTION_KEY)
        return False

    columns_sql = ", ".join(CHICKEN_CLASSIFICATION_SOURCE_FIELDS)
    rows = conn.execute(f"SELECT {columns_sql} FROM chickens").fetchall()

    for row in rows:
        record = dict(row)
        classified = classify_gene_profile(record)
        conn.execute(
            """
            UPDATE chickens
            SET
                primary_build = ?,
                primary_build_match_count = ?,
                primary_build_match_total = ?,
                primary_build_matched_slots = ?,
                primary_build_missing_slots = ?,
                primary_build_evaluations = ?,
                recessive_build = ?,
                recessive_build_match_count = ?,
                recessive_build_match_total = ?,
                recessive_build_repeat_bonus = ?,
                recessive_build_matched_slots = ?,
                recessive_build_missing_slots = ?,
                recessive_build_evaluations = ?,
                ultimate_type = ?
            WHERE token_id = ?
            """,
            (
                classified.get("primary_build"),
                classified.get("primary_build_match_count"),
                classified.get("primary_build_match_total"),
                json.dumps(classified.get("primary_build_matched_slots") or []),
                json.dumps(classified.get("primary_build_missing_slots") or []),
                json.dumps(classified.get("primary_build_evaluations") or {}),
                classified.get("recessive_build"),
                classified.get("recessive_build_match_count"),
                classified.get("recessive_build_match_total"),
                classified.get("recessive_build_repeat_bonus", 0) or 0,
                json.dumps(classified.get("recessive_build_matched_slots") or []),
                json.dumps(classified.get("recessive_build_missing_slots") or []),
                json.dumps(classified.get("recessive_build_evaluations") or {}),
                classified.get("ultimate_type"),
                str(record.get("token_id") or ""),
            ),
        )

    mark_migration_state(conn, BUILD_JSON_COMPACTION_KEY)
    return True


def env_flag(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def vacuum_after_compaction():
    if env_flag("APEX_SKIP_AUTO_VACUUM", default=False):
        return

    try:
        with get_connection() as conn:
            conn.execute("VACUUM")
    except Exception:
        pass


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chickens (
                token_id TEXT PRIMARY KEY,
                wallet_address TEXT,
                contract_address TEXT,
                name TEXT,
                nickname TEXT,
                image TEXT,
                token_uri TEXT,

                raw_state TEXT,
                state TEXT,
                is_dead INTEGER,
                is_egg INTEGER,

                breeding_time INTEGER,
                breeding_time_remaining TEXT,
                breed_count INTEGER,

                type TEXT,
                gender TEXT,
                level INTEGER,

                generation_text TEXT,
                generation_num INTEGER,

                parent_1 TEXT,
                parent_2 TEXT,

                instinct TEXT,

                beak TEXT,
                comb TEXT,
                eyes TEXT,
                feet TEXT,
                wings TEXT,
                tail TEXT,
                body TEXT,

                beak_h1 TEXT,
                beak_h2 TEXT,
                beak_h3 TEXT,
                comb_h1 TEXT,
                comb_h2 TEXT,
                comb_h3 TEXT,
                eyes_h1 TEXT,
                eyes_h2 TEXT,
                eyes_h3 TEXT,
                feet_h1 TEXT,
                feet_h2 TEXT,
                feet_h3 TEXT,
                wings_h1 TEXT,
                wings_h2 TEXT,
                wings_h3 TEXT,
                tail_h1 TEXT,
                tail_h2 TEXT,
                tail_h3 TEXT,
                body_h1 TEXT,
                body_h2 TEXT,
                body_h3 TEXT,

                gene_profile_loaded INTEGER,
                gene_last_updated TEXT,

                primary_build TEXT,
                primary_build_match_count INTEGER,
                primary_build_match_total INTEGER,

                primary_build_matched_slots TEXT,
                primary_build_missing_slots TEXT,
                primary_build_evaluations TEXT,

                recessive_build TEXT,
                recessive_build_match_count INTEGER,
                recessive_build_match_total INTEGER,
                recessive_build_repeat_bonus INTEGER,

                recessive_build_matched_slots TEXT,
                recessive_build_missing_slots TEXT,
                recessive_build_evaluations TEXT,

                ultimate_type TEXT,

                innate_attack INTEGER,
                innate_defense INTEGER,
                innate_speed INTEGER,
                innate_health INTEGER,
                innate_ferocity INTEGER,
                innate_cockrage INTEGER,
                innate_evasion INTEGER,

                ip INTEGER,
                last_updated TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chicken_family_roots (
                wallet_address TEXT NOT NULL,
                token_id TEXT NOT NULL,
                owned_root_count INTEGER DEFAULT 0,
                total_root_count INTEGER DEFAULT 0,
                ownership_percent REAL DEFAULT 0,
                is_complete INTEGER DEFAULT 0,
                root_check_target_count INTEGER DEFAULT 0,
                pending_root_check_count INTEGER DEFAULT 0,
                last_updated TEXT,
                PRIMARY KEY (wallet_address, token_id),
                FOREIGN KEY (token_id) REFERENCES chickens(token_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chicken_family_root_items (
                wallet_address TEXT NOT NULL,
                token_id TEXT NOT NULL,
                root_token_id TEXT NOT NULL,
                is_owned_root INTEGER DEFAULT 0,
                root_check_status TEXT DEFAULT 'unchecked',
                is_dead_root INTEGER DEFAULT 0,
                last_checked_at TEXT,
                PRIMARY KEY (wallet_address, token_id, root_token_id),
                FOREIGN KEY (token_id) REFERENCES chickens(token_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_sync_state (
                wallet_address TEXT PRIMARY KEY,
                last_synced_at TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_chicken_family_root_items_wallet_token
            ON chicken_family_root_items(wallet_address, token_id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_chicken_family_root_items_wallet_root
            ON chicken_family_root_items(wallet_address, root_token_id)
            """
        )

        ensure_column(conn, "chickens", "instinct", "TEXT")
        ensure_column(conn, "chickens", "beak", "TEXT")
        ensure_column(conn, "chickens", "comb", "TEXT")
        ensure_column(conn, "chickens", "eyes", "TEXT")
        ensure_column(conn, "chickens", "feet", "TEXT")
        ensure_column(conn, "chickens", "wings", "TEXT")
        ensure_column(conn, "chickens", "tail", "TEXT")
        ensure_column(conn, "chickens", "body", "TEXT")

        for prefix in ["beak", "comb", "eyes", "feet", "wings", "tail", "body"]:
            ensure_column(conn, "chickens", f"{prefix}_h1", "TEXT")
            ensure_column(conn, "chickens", f"{prefix}_h2", "TEXT")
            ensure_column(conn, "chickens", f"{prefix}_h3", "TEXT")

        ensure_column(conn, "chickens", "gene_profile_loaded", "INTEGER")
        ensure_column(conn, "chickens", "gene_last_updated", "TEXT")

        ensure_column(conn, "chickens", "primary_build", "TEXT")
        ensure_column(conn, "chickens", "primary_build_match_count", "INTEGER")
        ensure_column(conn, "chickens", "primary_build_match_total", "INTEGER")

        ensure_column(conn, "chickens", "primary_build_matched_slots", "TEXT")
        ensure_column(conn, "chickens", "primary_build_missing_slots", "TEXT")
        ensure_column(conn, "chickens", "primary_build_evaluations", "TEXT")

        ensure_column(conn, "chickens", "recessive_build", "TEXT")
        ensure_column(conn, "chickens", "recessive_build_match_count", "INTEGER")
        ensure_column(conn, "chickens", "recessive_build_match_total", "INTEGER")
        ensure_column(conn, "chickens", "recessive_build_repeat_bonus", "INTEGER")

        ensure_column(conn, "chickens", "recessive_build_matched_slots", "TEXT")
        ensure_column(conn, "chickens", "recessive_build_missing_slots", "TEXT")
        ensure_column(conn, "chickens", "recessive_build_evaluations", "TEXT")

        ensure_column(conn, "chickens", "ultimate_type", "TEXT")

        ensure_column(conn, "chicken_family_roots", "root_check_target_count", "INTEGER DEFAULT 0")
        ensure_column(conn, "chicken_family_roots", "pending_root_check_count", "INTEGER DEFAULT 0")

        ensure_column(conn, "chicken_family_root_items", "root_check_status", "TEXT DEFAULT 'unchecked'")
        ensure_column(conn, "chicken_family_root_items", "is_dead_root", "INTEGER DEFAULT 0")
        ensure_column(conn, "chicken_family_root_items", "last_checked_at", "TEXT")

        try:
            ensure_migration_state_table(conn)
            compacted_build_json = compact_chicken_build_json(conn)
        except sqlite3.OperationalError:
            conn.rollback()
            compacted_build_json = False

        conn.commit()

    if compacted_build_json:
        vacuum_after_compaction()
