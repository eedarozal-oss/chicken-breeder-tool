import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from services.db.connection import DB_PATH
from flask import Flask, render_template, request, redirect, url_for, session, send_file
import os
from io import BytesIO
from openpyxl import Workbook
from services.ronin_api import fetch_all_owned_chickens
from services.metadata_parser import parse_chicken_record
from services.match_rules import find_potential_matches, is_generation_gap_allowed
from services.family_roots import (
    resolve_family_roots_for_all,
    complete_ninuno_via_lineage_with_resume,
    initialize_simple_family_roots_for_wallet,
)
from services.chicken_enricher import enrich_chicken_records
from services.build_eval import evaluate_build, count_added_missing_traits
from services.wallet_access import get_wallet_access_expiry_display
from services.primary_build_classifier import classify_primary_build
from services.database import (
    init_db,
    upsert_chicken,
    get_chickens_by_wallet,
    clear_family_roots_for_wallet,
    clear_family_roots_for_token,
    clear_stale_family_root_summaries,
    upsert_family_root_summary,
    insert_family_root_items,
    get_static_chickens_by_token_ids,
    preload_cached_family_roots_for_wallet,
    get_wallet_last_synced_at,
    upsert_wallet_last_synced_at,
    delete_wallet_chickens_not_in_tokens,
)
from services.ultimate_breeding import (
    is_ultimate_eligible,
    get_ultimate_type_display,
    get_ultimate_build_display,
    filter_and_sort_ultimate_candidates,
)
from services.wallet_access import (
    init_wallet_access_db,
    is_valid_wallet,
    has_wallet_access,
    set_authorized_wallet,
    is_authorized_wallet,
    get_wallet_access_expiry_display,
    grant_manual_access,
    get_wallet_access_rows,
    format_wallet_access_rows,
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-fallback-secret")

CONTRACTS = [
    "0xee9436518030616bc315665678738a4348463df4",
    "0x322b3d98ddbd589dc2e8dd83659bb069828231e0",
]

MATCH_SETTINGS = {
    "max_generation_gap": 3,
    "max_ip_diff": 10,
    "max_breed_count_diff": 1,
}

init_db()
init_wallet_access_db()

OWNER_ADMIN_PASSWORD = os.environ.get("OWNER_ADMIN_PASSWORD", "").strip()
OWNER_WHITELIST_ROUTE = "/owner/grant-access"

STATIC_EXPORT_DB_PATH = Path(__file__).resolve().parent / "cache" / "chicken_static_export.db"

def quote_sqlite_identifier(name):
    return '"' + str(name).replace('"', '""') + '"'

def sync_static_export_tables_to_main_db(source_path=None):
    source_path = Path(source_path or STATIC_EXPORT_DB_PATH)

    if not source_path.exists():
        raise FileNotFoundError(f"Static export DB not found: {source_path}")

    synced = []

    with sqlite3.connect(source_path) as source_conn, sqlite3.connect(DB_PATH) as dest_conn:
        source_conn.row_factory = sqlite3.Row
        dest_conn.row_factory = sqlite3.Row

        table_rows = source_conn.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()

        if not table_rows:
            raise ValueError("No user tables found in the static export DB.")

        for row in table_rows:
            table_name = str(row["name"] or "").strip()
            create_sql = row["sql"]

            if not table_name or not create_sql:
                continue

            quoted_table = quote_sqlite_identifier(table_name)

            dest_conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            dest_conn.execute(create_sql)

            source_rows = source_conn.execute(f"SELECT * FROM {quoted_table}").fetchall()

            if source_rows:
                column_names = [col[1] for col in source_conn.execute(f"PRAGMA table_info({quoted_table})").fetchall()]
                quoted_columns = ", ".join(quote_sqlite_identifier(col) for col in column_names)
                placeholders = ", ".join(["?"] * len(column_names))

                dest_conn.executemany(
                    f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})",
                    [tuple(row[col] for col in column_names) for row in source_rows],
                )

            index_rows = source_conn.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type = 'index'
                  AND tbl_name = ?
                  AND sql IS NOT NULL
                ORDER BY name
                """,
                (table_name,),
            ).fetchall()

            for index_row in index_rows:
                index_sql = index_row["sql"]
                if index_sql:
                    try:
                        dest_conn.execute(index_sql)
                    except sqlite3.OperationalError:
                        pass

            row_count = dest_conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
            synced.append({
                "table": table_name,
                "row_count": row_count,
            })

        dest_conn.commit()

    return synced

def get_required_static_cache_tables():
    return ["chicken_static"]


def get_missing_static_cache_tables():
    required_tables = get_required_static_cache_tables()

    with sqlite3.connect(DB_PATH) as conn:
        existing_rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()

    existing_names = {str(row[0]).strip().lower() for row in existing_rows}
    return [
        table_name
        for table_name in required_tables
        if table_name.strip().lower() not in existing_names
    ]


def ensure_static_cache_tables_loaded():
    missing_tables = get_missing_static_cache_tables()

    if not missing_tables:
        return {
            "loaded": False,
            "synced_tables": [],
            "missing_tables": [],
        }

    if not STATIC_EXPORT_DB_PATH.exists():
        return {
            "loaded": False,
            "synced_tables": [],
            "missing_tables": missing_tables,
        }

    sync_results = sync_static_export_tables_to_main_db()

    return {
        "loaded": True,
        "synced_tables": sync_results,
        "missing_tables": missing_tables,
    }


EGG_STATIC_CACHE_FIELDS = [
    "parent_1",
    "parent_2",
]

HATCHED_STATIC_CACHE_FIELDS = [
    "parent_1",
    "parent_2",
    "gender",
    "generation_text",
    "generation_num",
    "instinct",
    "beak",
    "comb",
    "eyes",
    "feet",
    "wings",
    "tail",
    "body",
    "beak_h1",
    "beak_h2",
    "beak_h3",
    "comb_h1",
    "comb_h2",
    "comb_h3",
    "eyes_h1",
    "eyes_h2",
    "eyes_h3",
    "feet_h1",
    "feet_h2",
    "feet_h3",
    "wings_h1",
    "wings_h2",
    "wings_h3",
    "tail_h1",
    "tail_h2",
    "tail_h3",
    "body_h1",
    "body_h2",
    "body_h3",
    "innate_attack",
    "innate_defense",
    "innate_speed",
    "innate_health",
    "innate_ferocity",
    "innate_cockrage",
    "innate_evasion",
]

def merge_static_chicken_cache(record, static_row):
    if not static_row:
        return record

    is_live_egg = bool(record.get("is_egg")) or str(record.get("type") or "").strip().lower() == "egg"
    allowed_fields = EGG_STATIC_CACHE_FIELDS if is_live_egg else HATCHED_STATIC_CACHE_FIELDS

    for field in allowed_fields:
        if field not in static_row:
            continue

        current_value = record.get(field)
        if current_value not in (None, ""):
            continue

        static_value = static_row.get(field)
        if static_value is None or static_value == "":
            continue

        record[field] = static_value

    return record

def needs_recessive_enrichment(chicken):
    return not chicken.get("gene_profile_loaded")


def enrich_missing_recessive_data_in_batches(chickens, wallet, page_key, batch_size=5, prioritized_token_id=None):
    missing = [row for row in chickens if is_breedable(row) and needs_recessive_enrichment(row)]

    if not missing:
        session[f"{page_key}_cursor_{wallet}"] = 0
        return {
            "loaded": 0,
            "remaining": 0,
        }

    prioritized = []
    remaining = missing

    if prioritized_token_id:
        prioritized = [
            row for row in missing
            if str(row.get("token_id") or "") == str(prioritized_token_id)
        ]
        remaining = [
            row for row in missing
            if str(row.get("token_id") or "") != str(prioritized_token_id)
        ]

    cursor_key = f"{page_key}_cursor_{wallet}"
    cursor = safe_int(session.get(cursor_key), 0)
    if cursor is None:
        cursor = 0

    if remaining:
        if cursor >= len(remaining):
            cursor = 0
        rotated = remaining[cursor:] + remaining[:cursor]
    else:
        rotated = []

    selected_batch = prioritized[:1]
    remaining_slots = max(0, batch_size - len(selected_batch))
    selected_batch.extend(rotated[:remaining_slots])

    if not selected_batch:
        return {
            "loaded": 0,
            "remaining": len(missing),
        }

    enriched = enrich_chicken_records(selected_batch)
    loaded_count = 0

    for chicken in enriched:
        upsert_chicken(chicken)
        if chicken.get("gene_profile_loaded"):
            loaded_count += 1

    if remaining:
        next_cursor = cursor + remaining_slots
        if next_cursor >= len(remaining):
            next_cursor = 0
        session[cursor_key] = next_cursor
    else:
        session[cursor_key] = 0

    refreshed = get_chickens_by_wallet(wallet)
    remaining_after = sum(1 for row in refreshed if is_breedable(row) and needs_recessive_enrichment(row))

    return {
        "loaded": loaded_count,
        "remaining": remaining_after,
    }

def has_cached_recessive_ready_data(record):
    return bool(
        record.get("gene_profile_loaded")
        or record.get("recessive_build")
        or any(
            str(record.get(field) or "").strip()
            for field in [
                "beak_h1", "beak_h2", "beak_h3",
                "comb_h1", "comb_h2", "comb_h3",
                "eyes_h1", "eyes_h2", "eyes_h3",
                "feet_h1", "feet_h2", "feet_h3",
                "wings_h1", "wings_h2", "wings_h3",
                "tail_h1", "tail_h2", "tail_h3",
                "body_h1", "body_h2", "body_h3",
            ]
        )
    )

ITEM_IMAGE_URLS = {
    "Soulknot": "https://app.chickensaga.com/images/crafting/SOULKNOT.webp",
    "Gregor's Gift": "https://app.chickensaga.com/images/crafting/GREGOR%27S%20GIFT.webp",
    "Mendel's Memento": "https://app.chickensaga.com/images/crafting/MENDEL%27S%20MEMENTO.webp",
    "Quentin's Talon": "https://app.chickensaga.com/images/crafting/QUENTIN%27S%20TALON.webp",
    "Dragon's Whip": "https://app.chickensaga.com/images/crafting/DRAGON%27S%20WHIP.webp",
    "Chibidei's Curse": "https://app.chickensaga.com/images/crafting/CHIBIDEI%27S%20CURSE.webp",
    "All-seeing Seed": "https://app.chickensaga.com/images/crafting/ALL-SEEING%20SEED.webp",
    "Chim Lac's Curio": "https://app.chickensaga.com/images/crafting/CHIM%20LAC%27S%20CURIO.webp",
    "Suave Scissors": "https://app.chickensaga.com/images/crafting/SUAVE%20SCISSORS.webp",
    "Simurgh's Sovereign": "https://app.chickensaga.com/images/crafting/SIMURGH%27S%20SOVEREIGN.webp",
    "St. Elmo's Fire": "https://app.chickensaga.com/images/crafting/ST%20ELMO%27S%20FIRE.webp",
    "Cocktail's Beak": "https://app.chickensaga.com/images/crafting/DIP%27S%20BEAK.webp",
    "Pos2 Pellet": "https://app.chickensaga.com/images/crafting/POS2%27S%20PELLET.webp",
    "Fetzzz Feet": "https://app.chickensaga.com/images/crafting/FETZZZ%20FEET.webp",
    "Vananderen's Vitality": "https://app.chickensaga.com/images/crafting/VANANDEREN%27S%20VITALITY.webp",
    "Pinong's Bird": "https://app.chickensaga.com/images/crafting/PINONG%27S%20BIRD.webp",
    "Ouchie's Ornament": "https://app.chickensaga.com/images/crafting/OUCHIE%27S%20ORNAMENT.webp",
    "Lockedin State": "https://app.chickensaga.com/images/crafting/LOCKEDIN%20STATE.webp",
}

def get_item_image_url(item_name):
    return ITEM_IMAGE_URLS.get(str(item_name or "").strip(), "")

def is_breedable(chicken):
    return (not chicken.get("is_egg")) and str(chicken.get("state") or "").strip().lower() == "normal"


def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def require_authorized_wallet(wallet):
    wallet = (wallet or "").strip().lower()
    return wallet and is_authorized_wallet(wallet)


def wallet_needs_daily_refresh(wallet):
    wallet = (wallet or "").strip().lower()
    if not wallet:
        return False

    last_synced_at = get_wallet_last_synced_at(wallet)
    if not last_synced_at:
        return True

    return last_synced_at.astimezone(timezone.utc).date() < datetime.now(timezone.utc).date()


def sync_wallet_data(wallet):
    ensure_static_cache_tables_loaded()

    raw_items = fetch_all_owned_chickens(wallet, CONTRACTS)
    parsed_records = [parse_chicken_record(wallet, item) for item in raw_items]

    current_token_ids = [
        str(row.get("token_id") or "").strip()
        for row in parsed_records
        if str(row.get("token_id") or "").strip()
    ]
    delete_wallet_chickens_not_in_tokens(wallet, current_token_ids)

    static_lookup = get_static_chickens_by_token_ids([row.get("token_id") for row in parsed_records])
    cached_recessive_candidates = []

    for record in parsed_records:
        token_id = str(record.get("token_id") or "").strip()
        merge_static_chicken_cache(record, static_lookup.get(token_id))

        primary_build_data = classify_primary_build(record)
        record.update({
            "primary_build": primary_build_data.get("primary_build"),
            "primary_build_match_count": primary_build_data.get("primary_build_match_count"),
            "primary_build_match_total": primary_build_data.get("primary_build_match_total"),
            "ultimate_type": primary_build_data.get("ultimate_type"),
        })

        upsert_chicken(record)

        if has_cached_recessive_ready_data(record):
            cached_recessive_candidates.append(dict(record))

    if cached_recessive_candidates:
        try:
            enriched_records = enrich_chicken_records(cached_recessive_candidates)
            for enriched in enriched_records or []:
                upsert_chicken(enriched)
        except Exception:
            pass

    chickens = get_chickens_by_wallet(wallet)

    preload_cached_family_roots_for_wallet(
        chickens=chickens,
        wallet_address=wallet,
    )

    initialize_simple_family_roots_for_wallet(
        chickens=chickens,
        wallet_address=wallet,
        contract_addresses=CONTRACTS,
    )

    upsert_wallet_last_synced_at(wallet)
    clear_breeding_planner_for_wallet(wallet)

    return get_chickens_by_wallet(wallet)


def get_wallet_chickens(wallet, ensure_loaded=False, force_refresh=False):
    if wallet:
        clear_stale_family_root_summaries(wallet, max_age_hours=24)

    chickens = get_chickens_by_wallet(wallet)
    should_refresh = False

    if ensure_loaded:
        should_refresh = force_refresh or not chickens or wallet_needs_daily_refresh(wallet)

    if should_refresh:
        chickens = sync_wallet_data(wallet)
        if wallet:
            clear_stale_family_root_summaries(wallet, max_age_hours=24)
            chickens = get_chickens_by_wallet(wallet)

    return chickens


def needs_gene_enrichment(chicken):
    return not chicken.get("primary_build") and not chicken.get("recessive_build")


def enrich_missing_gene_data_in_batches(chickens, wallet, page_key, batch_size=5, prioritized_token_id=None):
    missing = [row for row in (chickens or []) if needs_gene_enrichment(row)]

    cursor_key = f"{page_key}_cursor_{wallet}"

    if not missing:
        session[cursor_key] = 0
        return {
            "loaded": 0,
            "remaining": 0,
        }

    prioritized = []
    remaining = list(missing)

    if prioritized_token_id:
        prioritized = [
            row for row in missing
            if str(row.get("token_id") or "") == str(prioritized_token_id)
        ]
        remaining = [
            row for row in missing
            if str(row.get("token_id") or "") != str(prioritized_token_id)
        ]

    cursor = safe_int(session.get(cursor_key), 0) or 0

    if remaining:
        if cursor >= len(remaining):
            cursor = 0
        rotated = remaining[cursor:] + remaining[:cursor]
    else:
        rotated = []

    selected_batch = prioritized[:1]
    remaining_slots = max(0, batch_size - len(selected_batch))
    selected_batch.extend(rotated[:remaining_slots])

    if not selected_batch:
        remaining_after = len(missing)
        session[cursor_key] = 0
        return {
            "loaded": 0,
            "remaining": remaining_after,
        }

    enriched = enrich_chicken_records(selected_batch)

    for chicken in enriched:
        upsert_chicken(chicken)

    if remaining:
        next_cursor = cursor + remaining_slots
        if next_cursor >= len(remaining):
            next_cursor = 0
        session[cursor_key] = next_cursor
    else:
        session[cursor_key] = 0

    refreshed = get_chickens_by_wallet(wallet)
    remaining_after = sum(1 for row in refreshed if needs_gene_enrichment(row))

    return {
        "loaded": len(selected_batch),
        "remaining": remaining_after,
    }


def get_effective_ip_stat(chicken, stat_name):
    value_field = IP_STAT_VALUE_FIELDS[stat_name]
    raw_value = chicken.get(value_field)

    if raw_value is not None and raw_value != "":
        return safe_int(raw_value, 0) or 0

    fallback_field = IP_STAT_FALLBACK_FIELDS.get(stat_name)
    if fallback_field:
        return safe_int(chicken.get(fallback_field), 0) or 0

    return 0

def get_weakest_ip_stat_info(chicken):
    stat_labels = {
        "attack": "Attack",
        "defense": "Defense",
        "hp": "Health",
        "speed": "Speed",
        "evasion": "Evasion",
        "ferocity": "Ferocity",
        "cockrage": "Cockrage",
    }

    weakest_stat_name = None
    weakest_stat_value = None

    for stat_name in IP_STAT_PRIORITY:
        stat_value = get_effective_ip_stat(chicken, stat_name)

        if weakest_stat_value is None or stat_value < weakest_stat_value:
            weakest_stat_name = stat_name
            weakest_stat_value = stat_value

    if weakest_stat_name is None or weakest_stat_value is None:
        return {
            "name": "",
            "label": "",
            "value": None,
            "display": "",
        }

    return {
        "name": weakest_stat_name,
        "label": stat_labels[weakest_stat_name],
        "value": weakest_stat_value,
        "display": f"{stat_labels[weakest_stat_name]}: {weakest_stat_value}",
    }

def build_ip_complement_profile(selected_chicken, candidate):
    score = 0
    weighted_score = 0
    useful_stat_count = 0
    detail_rows = []

    for priority_index, stat_name in enumerate(IP_STAT_PRIORITY):
        selected_value = get_effective_ip_stat(selected_chicken, stat_name)
        candidate_value = get_effective_ip_stat(candidate, stat_name)

        selected_deficit = max(0, 40 - selected_value)

        if selected_deficit <= 0:
            contribution = 0
        elif candidate_value < 25:
            contribution = 0
        else:
            contribution = min(selected_deficit, candidate_value)

        if contribution > 0:
            useful_stat_count += 1

        priority_weight = len(IP_STAT_PRIORITY) - priority_index
        weighted_score += contribution * priority_weight
        score += contribution

        detail_rows.append({
            "stat": stat_name,
            "selected_value": selected_value,
            "candidate_value": candidate_value,
            "selected_deficit": selected_deficit,
            "contribution": contribution,
            "priority_weight": priority_weight,
        })

    return {
        "score": score,
        "weighted_score": weighted_score,
        "useful_stat_count": useful_stat_count,
        "details": detail_rows,
    }


def get_ip_item_reason(stat_name):
    reason_map = {
        "attack": "Best when this parent is the stronger Attack source for the pair.",
        "defense": "Best when this parent is the stronger Defense source for the pair.",
        "hp": "Best when this parent is the stronger Health source for the pair.",
        "speed": "Best when this parent is the stronger Speed source for the pair.",
        "evasion": "Best when this parent is the stronger Evasion source for the pair.",
        "ferocity": "Best when this parent is the stronger Ferocity source for the pair.",
        "cockrage": "Best when this parent is the stronger Cockrage source for the pair.",
    }
    return reason_map[stat_name]


def get_ip_pair_stat_candidates(parent, other_parent):
    candidates = []

    for priority_index, stat_name in enumerate(IP_STAT_PRIORITY):
        parent_value = get_effective_ip_stat(parent, stat_name)
        other_value = get_effective_ip_stat(other_parent, stat_name)

        if parent_value < 25:
            continue

        if parent_value <= other_value:
            continue

        advantage = parent_value - other_value
        priority_weight = len(IP_STAT_PRIORITY) - priority_index
        weighted_advantage = (advantage * 100) + priority_weight

        candidates.append({
            "stat": stat_name,
            "name": IP_STAT_ITEM_NAMES[stat_name],
            "reason": get_ip_item_reason(stat_name),
            "parent_value": parent_value,
            "other_value": other_value,
            "advantage": advantage,
            "weighted_advantage": weighted_advantage,
            "is_single_target": True,
        })

    candidates.sort(
        key=lambda item: (
            -(item["weighted_advantage"] or 0),
            -(item["advantage"] or 0),
            IP_STAT_PRIORITY.index(item["stat"]),
        )
    )

    return candidates


def get_ip_item_candidates(parent, other_parent=None):
    if other_parent is None:
        ranked = get_top_base_stat_field(parent)
        candidates = []

        for field, value in ranked:
            if value < 25:
                continue
            item_name, reason = STAT_ITEM_RULES[field]
            candidates.append({
                "name": item_name,
                "reason": reason,
            })

        broad_count = sum(1 for _, value in ranked if value >= 32)
        if broad_count >= 3 and candidates:
            candidates.append({
                "name": "Soulknot",
                "reason": "Best when this parent is strong across several innate stats.",
            })

        return candidates

    pair_candidates = get_ip_pair_stat_candidates(parent, other_parent)

    if pair_candidates:
        broad_count = sum(
            1 for stat_name in IP_STAT_PRIORITY
            if get_effective_ip_stat(parent, stat_name) >= 32
            and get_effective_ip_stat(parent, stat_name) > get_effective_ip_stat(other_parent, stat_name)
        )

        soulknot_item = {
            "name": "Soulknot",
            "reason": "Best when this parent is strong across several usable stats.",
        }

        result = [
            {
                "name": item["name"],
                "reason": item["reason"],
            }
            for item in pair_candidates
        ]

        if broad_count >= 4:
            return [soulknot_item] + result

        if broad_count >= 3:
            result.append(soulknot_item)

        return result

    return []


TRAIT_SLOT_ORDER = ["beak", "comb", "eyes", "feet", "wings", "tail", "body"]
IP_STAT_PRIORITY = [
    "attack",
    "defense",
    "hp",
    "speed",
    "evasion",
    "ferocity",
    "cockrage",
]

IP_STAT_ITEM_NAMES = {
    "attack": "Cocktail's Beak",
    "defense": "Pos2 Pellet",
    "hp": "Vananderen's Vitality",
    "speed": "Fetzzz Feet",
    "evasion": "Lockedin State",
    "ferocity": "Ouchie's Ornament",
    "cockrage": "Pinong's Bird",
}

IP_STAT_VALUE_FIELDS = {
    "attack": "innate_attack",
    "defense": "innate_defense",
    "hp": "innate_health",
    "speed": "innate_speed",
    "evasion": "innate_evasion",
    "ferocity": "innate_ferocity",
    "cockrage": "innate_cockrage",
}

IP_STAT_FALLBACK_FIELDS = {
    "evasion": "innate_speed",
    "ferocity": "innate_attack",
    "cockrage": "innate_defense",
}
STAT_ITEM_RULES = {
    "innate_attack": ("Cocktail's Beak", "Best when this parent is contributing strong Attack inheritance."),
    "innate_defense": ("Pos2 Pellet", "Best when this parent is contributing strong Defense inheritance."),
    "innate_speed": ("Fetzzz Feet", "Best when this parent is contributing strong Speed inheritance."),
    "innate_health": ("Vananderen's Vitality", "Best when this parent is contributing strong Health inheritance."),
}

TRAIT_ITEM_RULES = {
    "beak": ("Chim Lac's Curio", "Best when this parent is supplying a needed Beak trait."),
    "comb": ("Suave Scissors", "Best when this parent is supplying a needed Comb trait."),
    "eyes": ("All-seeing Seed", "Best when this parent is supplying a needed Eyes trait."),
    "feet": ("Quentin's Talon", "Best when this parent is supplying a needed Feet trait."),
    "wings": ("Simurgh's Sovereign", "Best when this parent is supplying a needed Wings trait."),
    "tail": ("Dragon's Whip", "Best when this parent is supplying a needed Tail trait."),
    "body": ("Chibidei's Curse", "Best when this parent is supplying a needed Body trait."),
}

DUPLICATE_ALLOWED_ITEMS = {
    "Gregor's Gift",
    "Mendel's Memento",
    "Soulknot",
}

CHICKEN_IMAGE_URL_TEMPLATE = "https://chicken-api-ivory.vercel.app/api/image/{token_id}.png"

BUILD_INSTINCT_TIERS = {
    "damager": ["reckless", "aggressive", "blazing"],
    "runner": ["blazing", "swift", "reckless"],
    "ninja": ["aggressive", "reckless", "blazing"],
    "tank": ["enduring", "steadfast", "bulwark"],
    "jack": ["balanced", "adaptive", "vicious", "unyielding", "versatile"],
}


def get_chicken_image_url(chicken):
    token_id = str(chicken.get("token_id") or "").strip()
    if not token_id:
        return ""
    existing = str(chicken.get("image") or "").strip()
    if existing:
        return existing
    return CHICKEN_IMAGE_URL_TEMPLATE.format(token_id=token_id)


def enrich_chicken_media(chicken):
    row = dict(chicken)
    row["image"] = get_chicken_image_url(row)
    return row


def normalize_instinct_name(value):
    return str(value or "").strip().lower()


def get_instinct_tier_rank(instinct_name, build_type):
    tiers = BUILD_INSTINCT_TIERS.get(str(build_type or "").strip().lower(), [])
    instinct_key = normalize_instinct_name(instinct_name)
    for index, entry in enumerate(tiers):
        if instinct_key == entry:
            return index
    return len(tiers) + 1


def build_prefers_instinct(chicken, build_type):
    build_source = str(chicken.get("build_source_display") or "").strip().lower()
    if build_source != "primary":
        return False

    return get_instinct_tier_rank(chicken.get("instinct"), build_type) <= len(BUILD_INSTINCT_TIERS.get(build_type, []))


def get_top_base_stat_field(chicken):
    stat_fields = ["innate_attack", "innate_defense", "innate_speed", "innate_health"]
    ranked = []
    for field in stat_fields:
        ranked.append((field, safe_int(chicken.get(field), 0) or 0))
    ranked.sort(key=lambda x: (-x[1], x[0]))
    return ranked


def recommend_ip_item(parent, other_parent=None):
    ranked = get_top_base_stat_field(parent)
    broad_count = sum(1 for _, value in ranked if value >= 32)

    if broad_count >= 4:
        return {
            "name": "Soulknot",
            "reason": "Best when this parent is strong across several innate stats.",
        }

    top_field = ranked[0][0]
    item_name, reason = STAT_ITEM_RULES[top_field]
    return {
        "name": item_name,
        "reason": reason,
    }


def get_build_supply_slots(parent, other_parent, build_type):
    if not build_type:
        return []

    parent_eval = evaluate_build(parent, build_type)
    other_eval = evaluate_build(other_parent, build_type)

    supplied = set(other_eval.get("missing_slots", [])) & set(parent_eval.get("matched_slots", []))
    return [slot for slot in TRAIT_SLOT_ORDER if slot in supplied]


def recommend_gene_item(parent, other_parent, build_type):
    build_source = str(parent.get("build_source_display") or "").strip().lower()

    if build_source == "recessive":
        return {
            "name": "Mendel's Memento",
            "reason": "Best when this parent is being valued for recessive build inheritance.",
        }

    supplied_slots = get_build_supply_slots(parent, other_parent, build_type)
    if supplied_slots:
        item_name, reason = TRAIT_ITEM_RULES[supplied_slots[0]]
        return {
            "name": item_name,
            "reason": reason,
        }

    instinct_rank = get_instinct_tier_rank(parent.get("instinct"), build_type)
    if instinct_rank <= len(BUILD_INSTINCT_TIERS.get(str(build_type or "").strip().lower(), [])):
        return {
            "name": "St. Elmo's Fire",
            "reason": "Best when no primary trait edge is available and this parent has a strong instinct fit for the target build.",
        }

    primary_match_count = safe_int(parent.get("primary_build_match_count"), 0) or 0
    if build_source == "primary" and primary_match_count >= 4:
        return {
            "name": "Gregor's Gift",
            "reason": "Best when this parent is contributing a strong primary build.",
        }

    return None


def recommend_ultimate_item(parent, other_parent=None):
    primary_count = safe_int(parent.get("primary_build_match_count"), 0) or 0
    if primary_count >= 4:
        return {
            "name": "Gregor's Gift",
            "reason": "Best when this parent is contributing a strong primary build.",
        }

    ranked = get_top_base_stat_field(parent)
    broad_count = sum(1 for _, value in ranked if value >= 32)
    if broad_count >= 3:
        return {
            "name": "Soulknot",
            "reason": "Best when this parent is strong across several innate stats.",
        }

    top_field = ranked[0][0]
    item_name, reason = STAT_ITEM_RULES[top_field]
    return {
        "name": item_name,
        "reason": reason,
    }


def choose_non_duplicate_item(primary_item, fallback_items, blocked_name):
    if not primary_item:
        return None

    if primary_item["name"] != blocked_name:
        return primary_item

    for item in fallback_items:
        if item["name"] == blocked_name:
            continue

        if item["name"] in DUPLICATE_ALLOWED_ITEMS:
            continue

        return item

    return None

def pair_has_usable_ip_items(left_chicken, right_chicken):
    left_candidates = get_ip_item_candidates(left_chicken, right_chicken)
    right_candidates = get_ip_item_candidates(right_chicken, left_chicken)

    left_item, right_item = resolve_pair_item_recommendations(left_candidates, right_candidates)

    return bool(left_item) and bool(right_item)

def get_gene_item_candidates(parent, other_parent, build_type):
    candidates = []

    build_source = str(parent.get("build_source_display") or "").strip().lower()
    if build_source == "recessive":
        candidates.append({
            "name": "Mendel's Memento",
            "reason": "Best when this parent is being valued for recessive build inheritance.",
        })
    else:
        supplied_slots = get_build_supply_slots(parent, other_parent, build_type)
        for slot in supplied_slots:
            item_name, reason = TRAIT_ITEM_RULES[slot]
            candidates.append({
                "name": item_name,
                "reason": reason,
            })

        instinct_rank = get_instinct_tier_rank(parent.get("instinct"), build_type)
        if instinct_rank <= len(BUILD_INSTINCT_TIERS.get(str(build_type or "").strip().lower(), [])):
            candidates.append({
                "name": "St. Elmo's Fire",
                "reason": "Best when no primary trait edge is available and this parent has a strong instinct fit for the target build.",
            })

        primary_match_count = safe_int(parent.get("primary_build_match_count"), 0) or 0
        if build_source == "primary" and primary_match_count >= 4:
            candidates.append({
                "name": "Gregor's Gift",
                "reason": "Best when this parent is contributing a strong primary build.",
            })

    return candidates


def get_ultimate_item_candidates(parent, other_parent=None):
    candidates = []

    primary_count = safe_int(parent.get("primary_build_match_count"), 0) or 0
    if primary_count >= 4:
        candidates.append({
            "name": "Gregor's Gift",
            "reason": "Best when this parent is contributing a strong primary build.",
        })

    ranked = get_top_base_stat_field(parent)
    broad_count = sum(1 for _, value in ranked if value >= 32)
    if broad_count >= 3:
        candidates.append({
            "name": "Soulknot",
            "reason": "Best when this parent is strong across several innate stats.",
        })

    for field, value in ranked:
        if value < 25:
            continue
        item_name, reason = STAT_ITEM_RULES[field]
        candidates.append({
            "name": item_name,
            "reason": reason,
        })

    if not candidates:
        candidates.append({
            "name": "Soulknot",
            "reason": "Fallback when no strong single-stat boost is available.",
        })

    return candidates


def resolve_pair_item_recommendations(left_candidates, right_candidates):
    left_item = left_candidates[0] if left_candidates else None
    right_item = right_candidates[0] if right_candidates else None

    if not left_item or not right_item:
        return left_item, right_item

    if left_item["name"] != right_item["name"]:
        return left_item, right_item

    if left_item["name"] in DUPLICATE_ALLOWED_ITEMS:
        return left_item, right_item

    right_item = choose_non_duplicate_item(
        primary_item=right_item,
        fallback_items=right_candidates[1:],
        blocked_name=left_item["name"],
    )

    return left_item, right_item

def get_breeding_planner_session_key(wallet):
    wallet = (wallet or "").strip().lower()
    return f"breeding_planner_queue:{wallet}"

def clear_breeding_planner_for_wallet(wallet):
    wallet = (wallet or "").strip().lower()
    if not wallet:
        return

    session.pop(get_breeding_planner_session_key(wallet), None)
    session.modified = True

def build_planner_pair_key(left_token_id, right_token_id):
    left = str(left_token_id or "").strip()
    right = str(right_token_id or "").strip()
    pair = sorted([left, right])
    return f"{pair[0]}::{pair[1]}" if len(pair) == 2 else ""


def get_breeding_planner_queue(wallet):
    queue = session.get(get_breeding_planner_session_key(wallet)) or []
    if not isinstance(queue, list):
        return []
    cleaned = []
    for row in queue:
        if not isinstance(row, dict):
            continue
        pair_key = str(row.get("pair_key") or "").strip()
        left = row.get("left") or {}
        right = row.get("right") or {}
        if not pair_key or not left.get("token_id") or not right.get("token_id"):
            continue
        cleaned.append(row)
    return cleaned


def save_breeding_planner_queue(wallet, queue_rows):
    session[get_breeding_planner_session_key(wallet)] = queue_rows
    session.modified = True


def get_breeding_planner_token_ids(wallet):
    token_ids = set()
    for row in get_breeding_planner_queue(wallet):
        left = row.get("left") or {}
        right = row.get("right") or {}
        if left.get("token_id"):
            token_ids.add(str(left.get("token_id")))
        if right.get("token_id"):
            token_ids.add(str(right.get("token_id")))
    return token_ids

def filter_out_planner_tokens(rows, wallet):
    queued_ids = get_breeding_planner_token_ids(wallet)
    if not queued_ids:
        return list(rows or [])
    return [row for row in (rows or []) if str(row.get("token_id") or "") not in queued_ids]


def planner_pair_exists(wallet, left_token_id, right_token_id):
    pair_key = build_planner_pair_key(left_token_id, right_token_id)
    return any(
        str(row.get("pair_key") or "") == pair_key
        for row in get_breeding_planner_queue(wallet)
    )


def build_planner_queue_row(mode, left, right, left_item=None, right_item=None, pair_quality="", build_type=""):
    left = enrich_chicken_media(dict(left or {}))
    right = enrich_chicken_media(dict(right or {}))
    left_token_id = str(left.get("token_id") or "").strip()
    right_token_id = str(right.get("token_id") or "").strip()
    pair_key = build_planner_pair_key(left_token_id, right_token_id)
    mode_text = str(mode or "").strip().title()
    build_text = str(build_type or "").strip().title()

    left_summary = left.get("weakest_stat_display") or left.get("build_match_display") or left.get("ultimate_build_match_display") or ""
    right_summary = right.get("weakest_stat_display") or right.get("build_match_display") or right.get("ultimate_build_match_display") or ""

    if str(mode or "").strip().lower() == "ip":
        weakest_info = get_weakest_ip_stat_info(left)
        if weakest_info.get("display"):
            left_summary = weakest_info["display"]
        if weakest_info.get("name"):
            right_summary = f"{weakest_info['label']}: {get_effective_ip_stat(right, weakest_info['name'])}"

    elif str(mode or "").strip().lower() == "gene":
        current_build_type = str(build_type or "").strip().lower()
        build_label = str(build_type or "").strip().title()

        if current_build_type:
            left_gene = enrich_gene_display(left, current_build_type)
            right_gene = enrich_gene_display(right, current_build_type)

            if left_gene.get("build_match_display"):
                left_summary = f"{left_gene['build_match_display']} ({build_label})"
            if right_gene.get("build_match_display"):
                right_summary = f"{right_gene['build_match_display']} ({build_label})"

    elif str(mode or "").strip().lower() == "ultimate":
        left_ultimate = enrich_ultimate_display(left)
        right_ultimate = enrich_ultimate_display(right)

        left_build = str(left_ultimate.get("ultimate_build_display") or "").strip().title()
        right_build = str(right_ultimate.get("ultimate_build_display") or "").strip().title()

        if left_ultimate.get("ultimate_build_match_display"):
            left_summary = (
                f"{left_ultimate['ultimate_build_match_display']} ({left_build})"
                if left_build else left_ultimate["ultimate_build_match_display"]
            )

        if right_ultimate.get("ultimate_build_match_display"):
            right_summary = (
                f"{right_ultimate['ultimate_build_match_display']} ({right_build})"
                if right_build else right_ultimate["ultimate_build_match_display"]
            )

    
    return {
        "pair_key": pair_key,
        "mode": str(mode or "").strip().lower(),
        "mode_label": f"{mode_text} Breeding" if mode_text else "Breeding",
        "build_type": str(build_type or "").strip().lower(),
        "build_label": build_text,
        "pair_quality": str(pair_quality or "").strip(),
        "left": {
            "token_id": left_token_id,
            "image": left.get("image") or "",
            "label": f"#{left_token_id}" if left_token_id else "",
            "ip": left.get("ip"),
            "breed_count": left.get("breed_count"),
            "generation_text": left.get("generation_text") or "",
            "ninuno": left.get("ownership_percent") or 0,
            "summary": left_summary,
        },
        "right": {
            "token_id": right_token_id,
            "image": right.get("image") or "",
            "label": f"#{right_token_id}" if right_token_id else "",
            "ip": right.get("ip"),
            "breed_count": right.get("breed_count"),
            "generation_text": right.get("generation_text") or "",
            "ninuno": right.get("ownership_percent") or 0,
            "summary": right_summary,
        },
        "left_item": left_item or None,
        "right_item": right_item or None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def build_planner_summary(queue_rows):
    queue_rows = list(queue_rows or [])
    count = len(queue_rows)
    return {
        "count": count,
        "label": "Breeding Planner",
        "note": (
            "Queued pairs are removed from available matching pools until you remove them or refresh the wallet."
            if count else
            "You can manually review a match or use Auto Match, then add the pair to the breeding planner."
        ),
        "count_label": "Queued Pairs",
    }



def build_pair_quality_hint(raw_value):
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = 0
    if value >= 5:
        return "Excellent match"
    if value >= 4:
        return "Strong match"
    if value >= 2:
        return "Good match"
    return "Situational"


def count_ip_stats_at_or_above(chicken, threshold):
    return sum(
        1
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(chicken, stat_name) >= threshold
    )


def get_pair_combined_ip_stats(left_chicken, right_chicken):
    return {
        stat_name: max(
            get_effective_ip_stat(left_chicken, stat_name),
            get_effective_ip_stat(right_chicken, stat_name),
        )
        for stat_name in IP_STAT_PRIORITY
    }


def get_gene_pair_completion(selected_eval, candidate_eval):
    selected_eval = selected_eval or {}
    candidate_eval = candidate_eval or {}
    selected_slots = set(selected_eval.get("matched_slots", []))
    candidate_slots = set(candidate_eval.get("matched_slots", []))
    combined_slots = selected_slots | candidate_slots
    match_total = safe_int(selected_eval.get("match_total"), 0) or safe_int(candidate_eval.get("match_total"), 0) or 0
    return {
        "combined_count": len(combined_slots),
        "combined_total": match_total,
        "selected_count": safe_int(selected_eval.get("match_count"), 0) or 0,
        "candidate_count": safe_int(candidate_eval.get("match_count"), 0) or 0,
    }


def get_gene_pair_completion_from_row(row):
    row = row or {}
    selected_eval = row.get("selected_eval") or {}
    candidate_eval = row.get("candidate_eval") or {}
    if selected_eval or candidate_eval:
        return get_gene_pair_completion(selected_eval, candidate_eval)

    left = row.get("left") or {}
    right = row.get("right") or {}
    build_type = str(row.get("build_type") or "").strip().lower()
    if build_type and left and right:
        left_resolution = get_gene_build_resolution(left, build_type)
        right_resolution = get_gene_build_resolution(right, build_type)
        return get_gene_pair_completion(
            left_resolution.get("eval") or evaluate_build(left, build_type),
            right_resolution.get("eval") or evaluate_build(right, build_type),
        )

    return {
        "combined_count": 0,
        "combined_total": 0,
        "selected_count": 0,
        "candidate_count": 0,
    }


def build_ip_pair_quality(selected_chicken, candidate, row=None):
    if not selected_chicken or not candidate:
        return "Poor"

    selected_weakest = get_weakest_ip_stat_info(selected_chicken)
    candidate_weakest = get_weakest_ip_stat_info(candidate)

    selected_fixed_value = get_effective_ip_stat(candidate, selected_weakest.get("name")) if selected_weakest.get("name") else 0
    candidate_fixed_value = get_effective_ip_stat(selected_chicken, candidate_weakest.get("name")) if candidate_weakest.get("name") else 0

    combined_stats = get_pair_combined_ip_stats(selected_chicken, candidate)
    all_combined_at_30 = all(value >= 30 for value in combined_stats.values()) if combined_stats else False
    all_combined_at_25 = all(value >= 25 for value in combined_stats.values()) if combined_stats else False

    ip_difference = get_ip_difference(selected_chicken, candidate)
    each_has_four_at_30 = (
        count_ip_stats_at_or_above(selected_chicken, 30) >= 4
        and count_ip_stats_at_or_above(candidate, 30) >= 4
    )

    if (
        all_combined_at_30
        and selected_fixed_value >= 30
        and candidate_fixed_value >= 30
        and ip_difference is not None
        and ip_difference < 10
        and each_has_four_at_30
    ):
        return "Excellent match"

    if (
        all_combined_at_25
        and selected_fixed_value >= 25
        and candidate_fixed_value >= 25
        and ip_difference is not None
        and ip_difference < 10
    ):
        return "Strong match"

    if (
        selected_fixed_value >= 25
        and candidate_fixed_value >= 25
        and ip_difference is not None
        and ip_difference <= 10
    ):
        return "Good match"

    if selected_fixed_value >= 25:
        return "Situational"

    return "Poor"


def build_gene_pair_quality(row):
    metrics = get_gene_pair_completion_from_row(row)
    combined_count = metrics["combined_count"]
    combined_total = metrics["combined_total"]
    selected_count = metrics["selected_count"]
    candidate_count = metrics["candidate_count"]

    both_have_three = selected_count >= 3 and candidate_count >= 3

    if (combined_total == 7 and combined_count == 7 or combined_total == 5 and combined_count == 5) and both_have_three:
        return "Excellent match"

    if (combined_total == 7 and combined_count == 6 or combined_total == 5 and combined_count == 4) and both_have_three:
        return "Strong match"

    if (combined_total == 7 and combined_count == 5) or (combined_total == 5 and combined_count == 4):
        return "Good match"

    if (combined_total == 7 and combined_count == 4) or (combined_total == 5 and combined_count == 3):
        return "Situational"

    return "Poor"


def build_ultimate_pair_quality(row):
    row = row or {}
    left = row.get("left") or {}
    right = row.get("right") or {}
    candidate = row.get("candidate") or {}

    selected_type = str(row.get("selected_ultimate_type") or left.get("ultimate_type") or "").strip().lower()
    candidate_type = str(row.get("candidate_ultimate_type") or candidate.get("ultimate_type") or right.get("ultimate_type") or "").strip().lower()

    selected_build = str(row.get("selected_build") or left.get("primary_build") or "").strip().lower()
    candidate_build = str(row.get("candidate_build") or candidate.get("primary_build") or right.get("primary_build") or "").strip().lower()

    selected_count = safe_int(row.get("selected_build_match_count"), 0)
    selected_total = safe_int(row.get("selected_build_match_total"), 0)
    candidate_count = safe_int(row.get("candidate_build_match_count"), 0)
    candidate_total = safe_int(row.get("candidate_build_match_total"), 0)

    if not candidate_count:
        candidate_count = safe_int(candidate.get("primary_build_match_count"), 0) or safe_int(right.get("primary_build_match_count"), 0)
    if not candidate_total:
        candidate_total = safe_int(candidate.get("primary_build_match_total"), 0) or safe_int(right.get("primary_build_match_total"), 0)
    if not selected_count:
        selected_count = safe_int(left.get("primary_build_match_count"), 0)
    if not selected_total:
        selected_total = safe_int(left.get("primary_build_match_total"), 0)

    if not selected_type and left:
        selected_type = str(left.get("ultimate_type") or "").strip().lower()
    if not candidate_type and candidate:
        candidate_type = str(candidate.get("ultimate_type") or "").strip().lower()

    if selected_type == "both" and candidate_type == "both" and selected_build and selected_build == candidate_build:
        return "Excellent match"

    if "both" in {selected_type, candidate_type}:
        return "Strong match"

    pair_types = {selected_type, candidate_type}
    if pair_types == {"ip_only", "gene_only"}:
        ip_side = left if selected_type == "ip_only" else right if candidate_type == "ip_only" else candidate if candidate_type == "ip_only" else {}
        gene_count = selected_count if selected_type == "gene_only" else candidate_count
        gene_total = selected_total if selected_type == "gene_only" else candidate_total
        ip_value = safe_int(ip_side.get("ip"), 0) or 0

        if ip_value >= 270 and ((gene_total == 7 and gene_count >= 6) or (gene_total == 5 and gene_count == 5)):
            return "Good match"

        if ip_value < 270 and ((gene_total == 7 and gene_count == 5) or (gene_total == 5 and gene_count == 5)):
            return "Situational"

    return "Situational"


def export_breeding_planner_excel(queue_rows):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Breeding Planner"
    headers = [
        "Mode", "Build", "Pair Quality",
        "Left Chicken", "Left IP", "Left Breed Count", "Left Ninuno", "Left Item",
        "Right Chicken", "Right IP", "Right Breed Count", "Right Ninuno", "Right Item",
    ]
    sheet.append(headers)
    for row in queue_rows:
        left = row.get("left") or {}
        right = row.get("right") or {}
        left_item = row.get("left_item") or {}
        right_item = row.get("right_item") or {}
        sheet.append([
            row.get("mode_label") or "",
            row.get("build_label") or "",
            row.get("pair_quality") or "",
            left.get("label") or "", left.get("ip") or "", left.get("breed_count") if left.get("breed_count") is not None else "", left.get("ninuno") or 0, left_item.get("name") or "",
            right.get("label") or "", right.get("ip") or "", right.get("breed_count") if right.get("breed_count") is not None else "", right.get("ninuno") or 0, right_item.get("name") or "",
        ])
    for column_cells in sheet.columns:
        length = max(len(str(cell.value or "")) for cell in column_cells)
        sheet.column_dimensions[column_cells[0].column_letter].width = min(length + 4, 24)
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output

def format_wallet_short(wallet):
    wallet = str(wallet or "").strip()
    if len(wallet) <= 14:
        return wallet
    return f"{wallet[:6]}...{wallet[-4:]}"


def format_wallet_updated_at(dt_value):
    if not dt_value:
        return ""

    try:
        local_dt = dt_value.astimezone()
    except Exception:
        local_dt = dt_value

    month = local_dt.strftime("%b")
    day = local_dt.day
    year = local_dt.year
    hour = local_dt.strftime("%I").lstrip("0") or "0"
    minute = local_dt.strftime("%M")
    am_pm = local_dt.strftime("%p")
    return f"{month} {day}, {year} {hour}:{minute} {am_pm}"


def build_count_distribution_rows(pairs):
    pairs = list(pairs or [])
    total = sum(max(0, int(item.get("count", 0) or 0)) for item in pairs)
    rows = []

    for item in pairs:
        count = max(0, int(item.get("count", 0) or 0))
        percent = round((count / total) * 100, 1) if total else 0
        rows.append({
            "key": str(item.get("key") or "").strip().lower(),
            "label": str(item.get("label") or "").strip(),
            "count": count,
            "percent": percent,
        })

    return rows


def build_available_chickens_dashboard(chickens, breedable_chickens):
    chickens = list(chickens or [])
    breedable_chickens = list(breedable_chickens or [])

    total_count = len(chickens)
    breedable_count = len(breedable_chickens)
    egg_count = sum(
        1
        for row in chickens
        if bool(row.get("is_egg")) or str(row.get("type") or "").strip().lower() == "egg"
    )
    unavailable_count = max(0, total_count - breedable_count - egg_count)

    composition = build_count_distribution_rows([
        {"key": "breedable", "label": "Breedable", "count": breedable_count},
        {"key": "eggs", "label": "Eggs", "count": egg_count},
        {"key": "unavailable", "label": "Unavailable", "count": unavailable_count},
    ])

    generation_counts = {}
    for row in breedable_chickens:
        label = str(row.get("generation_text") or "").strip() or "Unknown"
        sort_value = safe_int(row.get("generation_num"), 999999)
        current = generation_counts.get(label, {"count": 0, "sort_value": sort_value})
        current["count"] += 1
        current["sort_value"] = min(current["sort_value"], sort_value)
        generation_counts[label] = current

    generation_distribution = build_count_distribution_rows([
        {
            "key": label.lower(),
            "label": label,
            "count": data["count"],
        }
        for label, data in sorted(
            generation_counts.items(),
            key=lambda item: (item[1]["sort_value"], item[0].lower())
        )
    ])

    breed_count_counts = {}
    for row in breedable_chickens:
        breed_count = safe_int(row.get("breed_count"), 0)
        label = str(breed_count if breed_count is not None else 0)
        breed_count_counts[label] = breed_count_counts.get(label, 0) + 1

    breed_count_distribution = build_count_distribution_rows([
        {
            "key": f"breed-{label}",
            "label": label,
            "count": count,
        }
        for label, count in sorted(
            breed_count_counts.items(),
            key=lambda item: safe_int(item[0], 999999)
        )
    ])

    ninuno_counts = {
        "complete_100": 0,
        "above_0": 0,
        "partial": 0,
        "recalculate": 0,
    }

    for row in breedable_chickens:
        ownership = float(row.get("ownership_percent") or 0)
        total_root_count = safe_int(row.get("total_root_count"), 0) or 0
        is_complete = bool(row.get("is_complete"))

        if not total_root_count:
            ninuno_counts["recalculate"] += 1
        elif not is_complete:
            ninuno_counts["partial"] += 1
        elif ownership == 100.0:
            ninuno_counts["complete_100"] += 1
        elif ownership > 0:
            ninuno_counts["above_0"] += 1
        else:
            ninuno_counts["above_0"] += 1

    ninuno_distribution = build_count_distribution_rows([
        {"key": "complete_100", "label": "100% Ninuno", "count": ninuno_counts["complete_100"]},
        {"key": "above_0", "label": "Above 0%", "count": ninuno_counts["above_0"]},
        {"key": "partial", "label": "Partial", "count": ninuno_counts["partial"]},
        {"key": "recalculate", "label": "Recalculate", "count": ninuno_counts["recalculate"]},
    ])

    highest_ip_row = None
    if breedable_chickens:
        highest_ip_row = max(
            breedable_chickens,
            key=lambda row: (
                safe_int(row.get("ip"), -1) or -1,
                -(safe_int(row.get("breed_count"), 999999) or 999999),
                -(safe_int(row.get("generation_num"), 999999) or 999999),
            )
        )

    lowest_breed_count_row = None
    if breedable_chickens:
        lowest_breed_count_row = min(
            breedable_chickens,
            key=lambda row: (
                safe_int(row.get("breed_count"), 999999) or 999999,
                -(safe_int(row.get("ip"), -1) or -1),
                safe_int(row.get("generation_num"), 999999) or 999999,
            )
        )

    return {
        "composition": composition,
        "generation_distribution": generation_distribution,
        "breed_count_distribution": breed_count_distribution,
        "ninuno_distribution": ninuno_distribution,
        "highest_ip_row": enrich_chicken_media(highest_ip_row) if highest_ip_row else None,
        "lowest_breed_count_row": enrich_chicken_media(lowest_breed_count_row) if lowest_breed_count_row else None,
    }

def build_wallet_summary(wallet, chickens, access_expiry=None):
    chickens = list(chickens or [])

    total_count = len(chickens)
    breedable_count = sum(1 for row in chickens if is_breedable(row))
    egg_count = sum(
        1
        for row in chickens
        if bool(row.get("is_egg")) or str(row.get("type") or "").strip().lower() == "egg"
    )
    unavailable_count = max(0, total_count - breedable_count - egg_count)
    planner_count = len(get_breeding_planner_queue(wallet))

    last_synced_at = get_wallet_last_synced_at(wallet)
    updated_display = format_wallet_updated_at(last_synced_at)

    return {
        "wallet_full": wallet,
        "wallet_short": format_wallet_short(wallet),
        "total_count": total_count,
        "breedable_count": breedable_count,
        "egg_count": egg_count,
        "unavailable_count": unavailable_count,
        "planner_count": planner_count,
        "access_expiry": access_expiry or "",
        "updated_display": updated_display,
    }

@app.context_processor
def inject_breeding_item_helpers():
    return {
        "recommend_ip_item": recommend_ip_item,
        "recommend_gene_item": recommend_gene_item,
        "recommend_ultimate_item": recommend_ultimate_item,
        "get_ip_item_candidates": get_ip_item_candidates,
        "get_gene_item_candidates": get_gene_item_candidates,
        "get_ultimate_item_candidates": get_ultimate_item_candidates,
        "resolve_pair_item_recommendations": resolve_pair_item_recommendations,
        "get_item_image_url": get_item_image_url,
        "build_ip_pair_quality": build_ip_pair_quality,
        "build_gene_pair_quality": build_gene_pair_quality,
        "build_ultimate_pair_quality": build_ultimate_pair_quality,
        "planner_pair_exists": lambda wallet, left_token_id, right_token_id: planner_pair_exists(wallet, left_token_id, right_token_id),
    }


GENE_PRIMARY_MIN_MATCH = 2
GENE_PRIMARY_QUALIFIED_MATCH = 5
GENE_RECESSIVE_MIN_MATCH = 4


def _normalize_gene_eval(result):
    result = dict(result or {})
    return {
        "build": result.get("build") or "",
        "label": result.get("label") or "",
        "match_count": safe_int(result.get("match_count"), 0) or 0,
        "match_total": safe_int(result.get("match_total"), 0) or 0,
        "matched_slots": list(result.get("matched_slots") or []),
        "missing_slots": list(result.get("missing_slots") or []),
    }


def _get_gene_target_eval(chicken, build_type, source_type):
    row = dict(chicken or {})
    build_key = str(build_type or "").strip().lower()

    if not build_key:
        return _normalize_gene_eval({})

    if source_type == "recessive":
        evaluations = row.get("recessive_build_evaluations") or {}
        stored = evaluations.get(build_key) if isinstance(evaluations, dict) else None
        if stored:
            return _normalize_gene_eval(stored)

        if str(row.get("recessive_build") or "").strip().lower() == build_key:
            return _normalize_gene_eval({
                "build": build_key,
                "label": build_key.title(),
                "match_count": row.get("recessive_build_match_count"),
                "match_total": row.get("recessive_build_match_total"),
                "matched_slots": row.get("recessive_build_matched_slots") or [],
                "missing_slots": row.get("recessive_build_missing_slots") or [],
            })

        return _normalize_gene_eval({})

    evaluations = row.get("primary_build_evaluations") or {}
    stored = evaluations.get(build_key) if isinstance(evaluations, dict) else None
    if stored:
        return _normalize_gene_eval(stored)

    fallback = evaluate_build(row, build_key)
    return _normalize_gene_eval(fallback)


def get_gene_build_target_info(chicken, build_type):
    row = dict(chicken or {})
    build_key = str(build_type or "").strip().lower()

    empty = {
        "eligible": False,
        "source": "",
        "display_source": "",
        "display_match": "",
        "effective_eval": _normalize_gene_eval({}),
        "primary_eval": _normalize_gene_eval({}),
        "recessive_eval": _normalize_gene_eval({}),
        "primary_count": 0,
        "recessive_count": 0,
        "sort_source_rank": 9,
        "sort_match_count": 0,
        "sort_match_total": 0,
    }

    if not build_key:
        return empty

    primary_eval = _get_gene_target_eval(row, build_key, "primary")
    recessive_eval = _get_gene_target_eval(row, build_key, "recessive")

    primary_count = primary_eval["match_count"]
    primary_total = primary_eval["match_total"]
    recessive_count = recessive_eval["match_count"]
    recessive_total = recessive_eval["match_total"]

    primary_build = str(row.get("primary_build") or "").strip().lower()
    recessive_build = str(row.get("recessive_build") or "").strip().lower()

    primary_qualified = primary_build == build_key and primary_count >= GENE_PRIMARY_QUALIFIED_MATCH
    recessive_qualified = recessive_build == build_key and recessive_count >= GENE_RECESSIVE_MIN_MATCH
    primary_supported = primary_count >= GENE_PRIMARY_MIN_MATCH

    source = ""
    display_source = ""
    display_match = ""
    effective_eval = primary_eval
    sort_source_rank = 9
    sort_match_count = 0
    sort_match_total = 0

    if primary_qualified:
        source = "primary"
        display_source = "Primary"
        display_match = f"{primary_count}/{primary_total}" if primary_total else ""
        effective_eval = primary_eval
        sort_source_rank = 0
        sort_match_count = primary_count
        sort_match_total = primary_total
    elif recessive_qualified and primary_supported:
        source = "mixed"
        display_source = "Primary + Recessive"
        primary_display = f"{primary_count}/{primary_total}" if primary_total else ""
        recessive_display = f"{recessive_count}/{recessive_total}" if recessive_total else ""
        display_match = " + ".join(part for part in [primary_display, recessive_display] if part)
        effective_eval = primary_eval
        sort_source_rank = 1
        sort_match_count = max(primary_count, recessive_count)
        sort_match_total = max(primary_total, recessive_total)
    elif recessive_qualified:
        source = "recessive"
        display_source = "Recessive"
        display_match = f"{recessive_count}/{recessive_total}" if recessive_total else ""
        effective_eval = recessive_eval
        sort_source_rank = 2
        sort_match_count = recessive_count
        sort_match_total = recessive_total
    elif primary_supported:
        source = "primary"
        display_source = "Primary"
        display_match = f"{primary_count}/{primary_total}" if primary_total else ""
        effective_eval = primary_eval
        sort_source_rank = 0
        sort_match_count = primary_count
        sort_match_total = primary_total

    return {
        "eligible": bool(source),
        "source": source,
        "display_source": display_source,
        "display_match": display_match,
        "effective_eval": effective_eval,
        "primary_eval": primary_eval,
        "recessive_eval": recessive_eval,
        "primary_count": primary_count,
        "recessive_count": recessive_count,
        "sort_source_rank": sort_source_rank,
        "sort_match_count": sort_match_count,
        "sort_match_total": sort_match_total,
    }


def enrich_gene_display(chicken, build_type):
    row = dict(chicken)

    row["build_source_display"] = ""
    row["build_match_display"] = ""
    row["gene_sort_source_rank"] = 9
    row["gene_sort_match_count"] = 0
    row["gene_sort_match_total"] = 0
    row["gene_effective_source"] = ""

    if not build_type:
        return row

    target_info = get_gene_build_target_info(row, build_type)
    row["build_source_display"] = target_info["display_source"]
    row["build_match_display"] = target_info["display_match"]
    row["gene_sort_source_rank"] = target_info["sort_source_rank"]
    row["gene_sort_match_count"] = target_info["sort_match_count"]
    row["gene_sort_match_total"] = target_info["sort_match_total"]
    row["gene_effective_source"] = target_info["source"]

    return enrich_chicken_media(row)


def chicken_matches_gene_build(chicken, build_type):
    if not build_type:
        return False

    return get_gene_build_target_info(chicken, build_type)["eligible"]


def make_empty_state(kicker, title, body):
    return {
        "kicker": kicker,
        "title": title,
        "body": body,
    }


def build_ultimate_available_empty_state():
    return make_empty_state(
        "No candidates ready",
        "No ultimate-eligible chickens found.",
        "Load more breedable chickens or review your wallet inventory before returning to Ultimate matching.",
    )


def build_ultimate_match_empty_state(auto_match=False, ninuno_mode="all", breed_diff=None):
    if breed_diff is not None and breed_diff <= 1:
        return make_empty_state(
            "Breed count too tight",
            "No ultimate pair passed the current breed-count difference rule.",
            "Increase the allowed breed-count difference to widen the partner pool.",
        )
    if str(ninuno_mode or "all") != "all":
        return make_empty_state(
            "Ninuno filter active",
            "No ultimate pair survived the current Ninuno filter.",
            "Relax the Ninuno filter to include more candidates.",
        )
    if auto_match:
        return make_empty_state(
            "No valid auto-match",
            "Auto Match could not find a usable ultimate pair.",
            "Try wider popup filters or choose a chicken manually.",
        )
    return make_empty_state(
        "No matches found",
        "No valid ultimate pair was found for the selected chicken.",
        "Try another chicken or review a larger available pool.",
    )

def make_empty_state(kicker, title, body):
    return {
        "kicker": kicker,
        "title": title,
        "body": body,
    }


def build_gene_available_empty_state(build_type, ninuno_100_only=False):
    if not build_type:
        return make_empty_state(
            "Choose a build",
            "Select a build to see available chickens.",
            "Pick your target build first to load compatible gene candidates.",
        )
    if ninuno_100_only:
        return make_empty_state(
            "Build plus Ninuno filter",
            "No chickens matched this build under the 100% Ninuno filter.",
            "Turn off the 100% Ninuno filter or try a different build.",
        )
    return make_empty_state(
        "Build has no candidates",
        "No chickens were found for this build.",
        "Try another build or refresh the wallet if you expect more candidates.",
    )


def build_gene_match_empty_state(build_type, ninuno_100_only=False, auto_match=False, same_instinct=False, min_build_count=None):
    if min_build_count is not None and min_build_count >= 4:
        return make_empty_state(
            "Build threshold too high",
            "No gene pair passed the current build-match requirement.",
            "Lower the minimum build-match count to widen the pool.",
        )
    if same_instinct:
        return make_empty_state(
            "Same instinct required",
            "No gene pair survived the same-instinct filter.",
            "Turn off Same Instinct to allow more compatible partners.",
        )
    if ninuno_100_only:
        return make_empty_state(
            "100% Ninuno only",
            "No gene pair survived the Ninuno filter.",
            "Turn off the 100% Ninuno filter to allow more possible partners.",
        )
    if auto_match:
        return make_empty_state(
            "No valid auto-match",
            "Auto Match could not find a usable gene pair.",
            "Try a different build, wider popup filters, or choose a chicken manually.",
        )
    build_label = str(build_type or "selected").title()
    return make_empty_state(
        "No matches found",
        f"No valid {build_label} gene pair was found for the selected chicken.",
        "Try another chicken or a different build target.",
    )


def enrich_ultimate_display(chicken):
    row = dict(chicken)

    row["ultimate_type_display"] = get_ultimate_type_display(row)
    row["ultimate_build_display"] = get_ultimate_build_display(row)

    primary_count = safe_int(row.get("primary_build_match_count"), 0) or 0
    primary_total = safe_int(row.get("primary_build_match_total"), 0) or 0
    row["ultimate_build_match_display"] = f"{primary_count}/{primary_total}" if primary_total else ""

    return enrich_chicken_media(row)


def build_gene_potential_matches(selected_chicken, breedable_chickens, build_type):
    if not selected_chicken or not build_type:
        return []

    selected_token_id = str(selected_chicken.get("token_id") or "")
    selected_target_info = get_gene_build_target_info(selected_chicken, build_type)
    selected_eval = selected_target_info["effective_eval"]

    candidate_pool = [
        row for row in breedable_chickens
        if str(row.get("token_id") or "") != selected_token_id
        and is_generation_gap_allowed(
            selected_chicken,
            row,
            max_gap=MATCH_SETTINGS["max_generation_gap"],
        )
    ]

    scored_matches = []
    for candidate in candidate_pool:
        candidate_target_info = get_gene_build_target_info(candidate, build_type)
        if not candidate_target_info["eligible"]:
            continue

        candidate_eval = candidate_target_info["effective_eval"]
        candidate_source = candidate_target_info["source"]

        scored_matches.append({
            "candidate": candidate,
            "candidate_eval": candidate_eval,
            "candidate_target_info": candidate_target_info,
            "added_missing_traits": count_added_missing_traits(selected_eval, candidate_eval),
            "instinct_rank": get_instinct_tier_rank(candidate.get("instinct"), build_type) if candidate_source == "primary" else 999,
        })

    scored_matches.sort(
        key=lambda row: (
            -(row["added_missing_traits"] or 0),
            -(row["candidate_target_info"]["sort_match_count"] or 0),
            row["candidate_target_info"]["sort_source_rank"],
            row.get("instinct_rank", 999),
            safe_int(row["candidate"].get("breed_count"), 999999) or 999999,
            -(float(row["candidate"].get("ownership_percent") or 0)),
            safe_int(row["candidate"].get("token_id"), 999999999) or 999999999,
        )
    )

    return scored_matches

def pick_best_ip_auto_match(breedable_chickens, enable_ip_diff=False, ip_diff=None):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in breedable_chickens:
        selected_token_id = str(selected.get("token_id") or "")
        candidate_pool = [
            row for row in breedable_chickens
            if str(row.get("token_id") or "") != selected_token_id
        ]

        if enable_ip_diff and ip_diff is not None:
            selected_ip = safe_int(selected.get("ip"))
            if selected_ip is not None:
                candidate_pool = [
                    row for row in candidate_pool
                    if safe_int(row.get("ip")) is not None
                    and abs(safe_int(row.get("ip")) - selected_ip) <= ip_diff
                ]

        matches = find_potential_matches(selected, candidate_pool, settings=MATCH_SETTINGS)

        matches = [
            row for row in matches
            if row.get("evaluation", {}).get("is_ip_recommended")
            and row.get("evaluation", {}).get("is_breed_count_recommended")
            and pair_has_usable_ip_items(selected, row.get("candidate"))
        ]

        if not matches:
            continue

        top = matches[0]
        top_eval = top.get("evaluation") or {}
        top_candidate = top.get("candidate") or {}
        ranking = (
            -(safe_int(top_eval.get("match_count"), 0) or 0),
            -(safe_int(top_eval.get("compatible_stat_count"), 0) or 0),
            safe_int(top_candidate.get("breed_count"), 999999) or 999999,
            -(float(top_candidate.get("ownership_percent") or 0)),
            safe_int(top_candidate.get("token_id"), 999999999) or 999999999,
            safe_int(selected.get("breed_count"), 999999) or 999999,
            -(float(selected.get("ownership_percent") or 0)),
            -(safe_int(selected.get("ip"), 0) or 0),
            safe_int(selected.get("token_id"), 999999999) or 999999999,
        )

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches


def pick_best_gene_auto_match(breedable_chickens, build_type):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in breedable_chickens:
        matches = build_gene_potential_matches(selected, breedable_chickens, build_type)
        if not matches:
            continue

        top = matches[0]
        top_candidate = top.get("candidate") or {}
        ranking = (
            -(top.get("added_missing_traits") or 0),
            -(top.get("candidate_eval", {}).get("match_count") or 0),
            top.get("instinct_rank", 999),
            safe_int(top_candidate.get("breed_count"), 999999) or 999999,
            -(float(top_candidate.get("ownership_percent") or 0)),
            safe_int(top_candidate.get("token_id"), 999999999) or 999999999,
            safe_int(selected.get("breed_count"), 999999) or 999999,
            -(float(selected.get("ownership_percent") or 0)),
            safe_int(selected.get("token_id"), 999999999) or 999999999,
        )

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches


def pick_best_ultimate_auto_match(breedable_chickens):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in breedable_chickens:
        selected_token_id = str(selected.get("token_id") or "")
        candidate_pool = [
            row for row in breedable_chickens
            if str(row.get("token_id") or "") != selected_token_id
            and is_generation_gap_allowed(
                selected,
                row,
                max_gap=MATCH_SETTINGS["max_generation_gap"],
            )
        ]

        matches = filter_and_sort_ultimate_candidates(selected, candidate_pool)
        if not matches:
            continue

        top = matches[0]
        top_candidate = top.get("candidate") or {}
        ranking = (
            -(safe_int(top_candidate.get("primary_build_match_count"), 0) or 0),
            safe_int(top_candidate.get("breed_count"), 999999) or 999999,
            -(float(top_candidate.get("ownership_percent") or 0)),
            -(safe_int(top_candidate.get("ip"), 0) or 0),
            safe_int(top_candidate.get("token_id"), 999999999) or 999999999,
            safe_int(selected.get("breed_count"), 999999) or 999999,
            -(float(selected.get("ownership_percent") or 0)),
            -(safe_int(selected.get("ip"), 0) or 0),
            safe_int(selected.get("token_id"), 999999999) or 999999999,
        )

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches



def normalize_auto_ninuno_filter(value):
    value = str(value or "all").strip().lower()
    if value in {"100", "100%", "100_only", "complete"}:
        return "100"
    if value in {"gt0", ">0", "not0", "above0", "positive"}:
        return "gt0"
    return "all"


def chicken_passes_auto_ninuno_filter(chicken, mode):
    mode = normalize_auto_ninuno_filter(mode)
    ownership = float(chicken.get("ownership_percent") or 0)
    if mode == "100":
        return bool(chicken.get("is_complete")) and ownership == 100.0
    if mode == "gt0":
        return ownership > 0
    return True


def parse_build_match_count(value):
    raw = str(value or "").strip()
    if not raw:
        return 0

    best = 0
    for part in raw.split("+"):
        part = part.strip()
        if "/" in part:
            left, _, _ = part.partition("/")
            best = max(best, safe_int(left, 0) or 0)
        else:
            best = max(best, safe_int(part, 0) or 0)
    return best


def build_ip_available_auto_candidates(breedable_chickens, ip_diff=None, breed_diff=None, ninuno_mode="all"):
    pair_rows = []
    for index, source in enumerate(breedable_chickens or []):
        if not chicken_passes_auto_ninuno_filter(source, ninuno_mode):
            continue
        for candidate in (breedable_chickens or [])[index + 1:]:
            if not chicken_passes_auto_ninuno_filter(candidate, ninuno_mode):
                continue
            if ip_diff is not None:
                source_ip = safe_int(source.get("ip"))
                candidate_ip = safe_int(candidate.get("ip"))
                if source_ip is None or candidate_ip is None or abs(candidate_ip - source_ip) > ip_diff:
                    continue
            if breed_diff is not None:
                source_breed = safe_int(source.get("breed_count"))
                candidate_breed = safe_int(candidate.get("breed_count"))
                if source_breed is None or candidate_breed is None or abs(candidate_breed - source_breed) > breed_diff:
                    continue
            forward = [
                row for row in find_potential_matches(source, [candidate], settings=MATCH_SETTINGS)
                if row.get("evaluation", {}).get("is_ip_recommended")
                and row.get("evaluation", {}).get("is_breed_count_recommended")
                and pair_has_usable_ip_items(source, row.get("candidate"))
            ]
            reverse = [
                row for row in find_potential_matches(candidate, [source], settings=MATCH_SETTINGS)
                if row.get("evaluation", {}).get("is_ip_recommended")
                and row.get("evaluation", {}).get("is_breed_count_recommended")
                and pair_has_usable_ip_items(candidate, row.get("candidate"))
            ]
            if forward:
                chosen_left = dict(source)
                chosen_right = dict(candidate)
                chosen_match = forward[0]
            elif reverse:
                chosen_left = dict(candidate)
                chosen_right = dict(source)
                chosen_match = reverse[0]
            else:
                continue
            weakest_info = get_weakest_ip_stat_info(chosen_left)
            chosen_left["weakest_stat_display"] = weakest_info["display"]
            selected_item_candidates = get_ip_item_candidates(chosen_left, chosen_right)
            right_item_candidates = get_ip_item_candidates(chosen_right, chosen_left)
            left_item, right_item = resolve_pair_item_recommendations(selected_item_candidates, right_item_candidates)
            pair_rows.append({
                "left": chosen_left,
                "right": chosen_right,
                "left_item": left_item,
                "right_item": right_item,
                "selected_weakest_stat_display": f"{weakest_info['label']}: {get_effective_ip_stat(chosen_right, weakest_info['name'])}" if weakest_info["name"] else "",
                "ip_difference": chosen_match.get("evaluation", {}).get("ip_difference"),
                "ranking": (
                    -(safe_int(chosen_match.get("evaluation", {}).get("match_count"), 0) or 0),
                    -(safe_int(chosen_match.get("evaluation", {}).get("compatible_stat_count"), 0) or 0),
                    safe_int(chosen_right.get("breed_count"), 999999) or 999999,
                    -(float(chosen_right.get("ownership_percent") or 0)),
                    safe_int(chosen_right.get("token_id"), 999999999) or 999999999,
                    safe_int(chosen_left.get("breed_count"), 999999) or 999999,
                    -(float(chosen_left.get("ownership_percent") or 0)),
                    -(safe_int(chosen_left.get("ip"), 0) or 0),
                    safe_int(chosen_left.get("token_id"), 999999999) or 999999999,
                ),
            })
    pair_rows.sort(key=lambda row: row["ranking"])
    return pair_rows


def build_gene_available_auto_candidates(breedable_chickens, build_type, min_build_count=None, breed_diff=None, same_instinct=False, ninuno_mode="all"):
    pair_rows = []
    for index, source in enumerate(breedable_chickens or []):
        if not chicken_passes_auto_ninuno_filter(source, ninuno_mode):
            continue
        if min_build_count is not None and parse_build_match_count(source.get("build_match_display")) < min_build_count:
            continue
        for candidate in (breedable_chickens or [])[index + 1:]:
            if not chicken_passes_auto_ninuno_filter(candidate, ninuno_mode):
                continue
            if min_build_count is not None and parse_build_match_count(candidate.get("build_match_display")) < min_build_count:
                continue
            if breed_diff is not None:
                source_breed = safe_int(source.get("breed_count"))
                candidate_breed = safe_int(candidate.get("breed_count"))
                if source_breed is None or candidate_breed is None or abs(candidate_breed - source_breed) > breed_diff:
                    continue
            if same_instinct and normalize_instinct_name(source.get("instinct")) != normalize_instinct_name(candidate.get("instinct")):
                continue
            if not is_generation_gap_allowed(source, candidate, max_gap=MATCH_SETTINGS["max_generation_gap"]):
                continue
            forward = build_gene_potential_matches(source, [source, candidate], build_type)
            reverse = build_gene_potential_matches(candidate, [source, candidate], build_type)
            if forward:
                chosen_left = dict(source)
                chosen_right = dict(candidate)
                chosen_match = forward[0]
            elif reverse:
                chosen_left = dict(candidate)
                chosen_right = dict(source)
                chosen_match = reverse[0]
            else:
                continue
            left_item_candidates = get_gene_item_candidates(chosen_left, chosen_right, build_type)
            right_item_candidates = get_gene_item_candidates(chosen_right, chosen_left, build_type)
            left_item, right_item = resolve_pair_item_recommendations(left_item_candidates, right_item_candidates)
            pair_rows.append({
                "left": chosen_left,
                "right": chosen_right,
                "left_item": left_item,
                "right_item": right_item,
                "build_type": build_type,
                "selected_eval": chosen_match.get("selected_eval"),
                "candidate_eval": chosen_match.get("candidate_eval"),
                "combined_match_count": chosen_match.get("combined_match_count", 0),
                "combined_match_total": chosen_match.get("combined_match_total", 0),
                "selected_build_match_count": chosen_match.get("selected_build_match_count", 0),
                "candidate_build_match_count": chosen_match.get("candidate_build_match_count", 0),
                "same_instinct": normalize_instinct_name(chosen_left.get("instinct")) == normalize_instinct_name(chosen_right.get("instinct")),
                "added_missing_traits": chosen_match.get("added_missing_traits") or 0,
                "ranking": (
                    -(chosen_match.get("combined_match_count") or 0),
                    -(chosen_match.get("added_missing_traits") or 0),
                    -(chosen_match.get("candidate_eval", {}).get("match_count") or 0),
                    chosen_match.get("instinct_rank", 999),
                    safe_int(chosen_right.get("breed_count"), 999999) or 999999,
                    -(float(chosen_right.get("ownership_percent") or 0)),
                    safe_int(chosen_right.get("token_id"), 999999999) or 999999999,
                    safe_int(chosen_left.get("breed_count"), 999999) or 999999,
                    -(float(chosen_left.get("ownership_percent") or 0)),
                    safe_int(chosen_left.get("token_id"), 999999999) or 999999999,
                ),
            })
    pair_rows.sort(key=lambda row: row["ranking"])
    return pair_rows


def build_ultimate_available_auto_candidates(breedable_chickens, breed_diff=None, ninuno_mode="all"):
    pair_rows = []
    for index, source in enumerate(breedable_chickens or []):
        if not chicken_passes_auto_ninuno_filter(source, ninuno_mode):
            continue
        for candidate in (breedable_chickens or [])[index + 1:]:
            if not chicken_passes_auto_ninuno_filter(candidate, ninuno_mode):
                continue
            if breed_diff is not None:
                source_breed = safe_int(source.get("breed_count"))
                candidate_breed = safe_int(candidate.get("breed_count"))
                if source_breed is None or candidate_breed is None or abs(candidate_breed - source_breed) > breed_diff:
                    continue
            if not is_generation_gap_allowed(source, candidate, max_gap=MATCH_SETTINGS["max_generation_gap"]):
                continue
            forward = filter_and_sort_ultimate_candidates(source, [candidate])
            reverse = filter_and_sort_ultimate_candidates(candidate, [source])
            if forward:
                chosen_left = dict(source)
                chosen_right = dict(candidate)
                chosen_match = forward[0]
            elif reverse:
                chosen_left = dict(candidate)
                chosen_right = dict(source)
                chosen_match = reverse[0]
            else:
                continue
            left_item_candidates = get_ultimate_item_candidates(chosen_left, chosen_right)
            right_item_candidates = get_ultimate_item_candidates(chosen_right, chosen_left)
            left_item, right_item = resolve_pair_item_recommendations(left_item_candidates, right_item_candidates)
            pair_rows.append({
                "left": chosen_left,
                "right": chosen_right,
                "left_item": left_item,
                "right_item": right_item,
                "build_complement": chosen_match.get("build_complement"),
                "ranking": (
                    -(safe_int(chosen_right.get("primary_build_match_count"), 0) or 0),
                    safe_int(chosen_right.get("breed_count"), 999999) or 999999,
                    -(float(chosen_right.get("ownership_percent") or 0)),
                    -(safe_int(chosen_right.get("ip"), 0) or 0),
                    safe_int(chosen_right.get("token_id"), 999999999) or 999999999,
                    safe_int(chosen_left.get("breed_count"), 999999) or 999999,
                    -(float(chosen_left.get("ownership_percent") or 0)),
                    -(safe_int(chosen_left.get("ip"), 0) or 0),
                    safe_int(chosen_left.get("token_id"), 999999999) or 999999999,
                ),
            })
    pair_rows.sort(key=lambda row: row["ranking"])
    return pair_rows


def pick_multi_pairs_from_candidates(pair_candidates, target_count):
    used = set()
    results = []
    target_count = max(0, safe_int(target_count, 0) or 0)
    for row in pair_candidates or []:
        left_id = str((row.get("left") or {}).get("token_id") or "")
        right_id = str((row.get("right") or {}).get("token_id") or "")
        if not left_id or not right_id or left_id in used or right_id in used:
            continue
        results.append(row)
        used.add(left_id)
        used.add(right_id)
        if len(results) >= target_count:
            break
    return results

def sort_key_text(value):
    return str(value or "").strip().lower()


def sort_key_int(value, default=0):
    parsed = safe_int(value, default)
    return parsed if parsed is not None else default


def sort_key_build_match(value):
    raw = str(value or "").strip()
    if not raw:
        return (0, 0)

    best = (0, 0)
    for part in raw.split("+"):
        part = part.strip()
        if "/" not in part:
            continue
        left, _, right = part.partition("/")
        candidate = (
            safe_int(left, 0) or 0,
            safe_int(right, 0) or 0,
        )
        if candidate > best:
            best = candidate
    return best


def get_gene_build_source_rank(value):
    source = str(value or "").strip().lower()
    if source == "primary":
        return 0
    if source in {"primary + recessive", "mixed"}:
        return 1
    if source == "recessive":
        return 2
    return 9


def get_ultimate_type_rank(value):
    text = str(value or "").strip().lower()
    if text == "both":
        return 0
    if text == "gene only":
        return 1
    if text == "ip only":
        return 2
    return 9


def sort_ip_available_chickens(rows, sort_by="ip", sort_dir="desc"):
    reverse = (sort_dir == "desc")

    if sort_by == "breed_count":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "generation":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("generation_num"), 999999),
                sort_key_int(row.get("breed_count"), 999999),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    rows.sort(
        key=lambda row: (
            sort_key_int(row.get("ip"), 0),
            -sort_key_int(row.get("breed_count"), 999999),
            -sort_key_int(row.get("generation_num"), 999999),
            -sort_key_int(row.get("token_id"), 999999999),
        ),
        reverse=reverse,
    )


def sort_gene_available_chickens(rows, sort_by="build_source", sort_dir="asc"):
    reverse = (sort_dir == "desc")

    if sort_by == "build_match":
        rows.sort(
            key=lambda row: (
                sort_key_build_match(row.get("build_match_display")),
                -sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("ip"), 0),
                -sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "generation":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("generation_num"), 999999),
                sort_key_int(row.get("breed_count"), 999999),
                -sort_key_int(row.get("ip"), 0),
                get_gene_build_source_rank(row.get("build_source_display")),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "breed_count":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("ip"), 0),
                get_gene_build_source_rank(row.get("build_source_display")),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "ip":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("ip"), 0),
                -sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("breed_count"), 999999),
                -get_gene_build_source_rank(row.get("build_source_display")),
                -sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    rows.sort(
        key=lambda row: (
            get_gene_build_source_rank(row.get("build_source_display")),
            sort_key_build_match(row.get("build_match_display")),
            -sort_key_int(row.get("generation_num"), 999999),
            -sort_key_int(row.get("breed_count"), 999999),
            sort_key_int(row.get("ip"), 0),
            -sort_key_int(row.get("token_id"), 999999999),
        ),
        reverse=reverse,
    )


def sort_ultimate_available_chickens(rows, sort_by="ultimate_type", sort_dir="asc"):
    reverse = (sort_dir == "desc")

    if sort_by == "build":
        rows.sort(
            key=lambda row: (
                sort_key_text(row.get("ultimate_build_display")),
                sort_key_build_match(row.get("ultimate_build_match_display")),
                sort_key_int(row.get("ip"), 0),
                -sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("breed_count"), 999999),
                -sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "build_match":
        rows.sort(
            key=lambda row: (
                sort_key_build_match(row.get("ultimate_build_match_display")),
                sort_key_int(row.get("ip"), 0),
                -sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("breed_count"), 999999),
                -sort_key_text(row.get("ultimate_build_display")),
                -sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "ip":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("ip"), 0),
                sort_key_build_match(row.get("ultimate_build_match_display")),
                -sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("breed_count"), 999999),
                -get_ultimate_type_rank(row.get("ultimate_type_display")),
                -sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "generation":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("generation_num"), 999999),
                sort_key_int(row.get("breed_count"), 999999),
                -sort_key_int(row.get("ip"), 0),
                get_ultimate_type_rank(row.get("ultimate_type_display")),
                sort_key_text(row.get("ultimate_build_display")),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    if sort_by == "breed_count":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("ip"), 0),
                get_ultimate_type_rank(row.get("ultimate_type_display")),
                sort_key_text(row.get("ultimate_build_display")),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return

    rows.sort(
        key=lambda row: (
            get_ultimate_type_rank(row.get("ultimate_type_display")),
            sort_key_text(row.get("ultimate_build_display")),
            sort_key_build_match(row.get("ultimate_build_match_display")),
            -sort_key_int(row.get("ip"), 0),
            sort_key_int(row.get("generation_num"), 999999),
            sort_key_int(row.get("breed_count"), 999999),
            sort_key_int(row.get("token_id"), 999999999),
        ),
        reverse=reverse,
    )

def get_ip_difference(chicken_a, chicken_b):
    ip_a = safe_int((chicken_a or {}).get("ip"))
    ip_b = safe_int((chicken_b or {}).get("ip"))

    if ip_a is None or ip_b is None:
        return None

    return abs(ip_a - ip_b)

def pick_best_ip_auto_match_from_pool(pool, ip_diff=10, breed_diff=1):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in pool:
        selected_token_id = str(selected.get("token_id") or "")
        candidate_pool = [
            row for row in pool
            if str(row.get("token_id") or "") != selected_token_id
        ]

        if ip_diff is not None:
            selected_ip = safe_int(selected.get("ip"))
            if selected_ip is not None:
                candidate_pool = [
                    row for row in candidate_pool
                    if safe_int(row.get("ip")) is not None
                    and abs(safe_int(row.get("ip")) - selected_ip) <= ip_diff
                ]

        if breed_diff is not None:
            selected_breed = safe_int(selected.get("breed_count"))
            if selected_breed is not None:
                candidate_pool = [
                    row for row in candidate_pool
                    if safe_int(row.get("breed_count")) is not None
                    and abs(safe_int(row.get("breed_count")) - selected_breed) <= breed_diff
                ]

        matches = find_potential_matches(selected, candidate_pool, settings=MATCH_SETTINGS)

        matches = [
            row for row in matches
            if row.get("evaluation", {}).get("is_ip_recommended")
            and row.get("evaluation", {}).get("is_breed_count_recommended")
            and pair_has_usable_ip_items(selected, row.get("candidate"))
        ]

        if not matches:
            continue

        top = matches[0]
        top_eval = top.get("evaluation") or {}
        top_candidate = top.get("candidate") or {}

        ranking = (
            -(safe_int(top_eval.get("match_count"), 0) or 0),
            -(safe_int(top_eval.get("compatible_stat_count"), 0) or 0),
            safe_int(top_candidate.get("breed_count"), 999999) or 999999,
            -(float(top_candidate.get("ownership_percent") or 0)),
            safe_int(top_candidate.get("token_id"), 999999999) or 999999999,
            safe_int(selected.get("breed_count"), 999999) or 999999,
            -(float(selected.get("ownership_percent") or 0)),
            -(safe_int(selected.get("ip"), 0) or 0),
            safe_int(selected.get("token_id"), 999999999) or 999999999,
        )

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches

def build_ip_multi_matches(breedable_chickens, ip_diff=10, breed_diff=1, ninuno_filter="all", target_count=1):
    pool = list(breedable_chickens or [])
    results = []

    pool = [
        row for row in pool
        if chicken_passes_auto_ninuno_filter(row, ninuno_filter)
    ]

    while len(pool) >= 2 and len(results) < target_count:
        selected, matches = pick_best_ip_auto_match(
            pool,
            enable_ip_diff=(ip_diff is not None),
            ip_diff=ip_diff,
        )

        if not selected or not matches:
            break

        filtered_matches = []
        for row in matches:
            candidate = row.get("candidate") or {}

            selected_breed = safe_int(selected.get("breed_count"))
            candidate_breed = safe_int(candidate.get("breed_count"))

            if breed_diff is not None:
                if selected_breed is None or candidate_breed is None:
                    continue
                if abs(candidate_breed - selected_breed) > breed_diff:
                    continue

            if not pair_has_usable_ip_items(selected, candidate):
                continue

            filtered_matches.append(row)

        if not filtered_matches:
            used_selected_id = str(selected.get("token_id") or "")
            pool = [
                row for row in pool
                if str(row.get("token_id") or "") != used_selected_id
            ]
            continue

        top = filtered_matches[0]
        candidate = top.get("candidate") or {}

        weakest_info = get_weakest_ip_stat_info(selected)
        selected_weakest_stat_display = ""
        if weakest_info.get("name"):
            selected_weakest_stat_display = (
                f"{weakest_info['label']}: "
                f"{get_effective_ip_stat(candidate, weakest_info['name'])}"
            )

        left_candidates = get_ip_item_candidates(selected, candidate)
        right_candidates = get_ip_item_candidates(candidate, selected)
        left_item, right_item = resolve_pair_item_recommendations(left_candidates, right_candidates)

        results.append({
            "left": selected,
            "right": candidate,
            "left_item": left_item,
            "right_item": right_item,
            "selected_weakest_stat_display": selected_weakest_stat_display,
        })

        used_ids = {
            str(selected.get("token_id") or ""),
            str(candidate.get("token_id") or ""),
        }
        pool = [
            row for row in pool
            if str(row.get("token_id") or "") not in used_ids
        ]

    return results

@app.route("/", methods=["GET", "POST"])
def index():
    error = None
    success = None
    redirect_url = None
    wallet = request.values.get("wallet_address", "").strip().lower()

    if request.method == "POST":
        if not wallet:
            error = "Enter a wallet address to continue."
        elif not is_valid_wallet(wallet):
            error = "Enter a valid 0x wallet address."
        else:
            try:
                if not has_wallet_access(wallet):
                    error = "This wallet has no active access. Send at least 0.1 RON to 0x9933199Fa3D96D7696d2B2A4CfBa48d99E47a079 to gain access."
                else:
                    set_authorized_wallet(wallet)
                    get_wallet_chickens(wallet, ensure_loaded=True)

                    expiry_display = get_wallet_access_expiry_display(wallet)
                    if expiry_display:
                        success = f"Wallet approved. Access is active until {expiry_display}."
                    else:
                        success = "Wallet approved. Access is active for 30 days."

                    redirect_url = url_for("landing_page", wallet_address=wallet)

            except Exception as exc:
                error = f"Failed to validate wallet access: {exc}"

    elif wallet:
        if is_valid_wallet(wallet) and is_authorized_wallet(wallet):
            return redirect(url_for("landing_page", wallet_address=wallet))

    return render_template(
        "index.html",
        wallet=wallet,
        error=error,
        success=success,
        redirect_url=redirect_url,
    )


@app.route(OWNER_WHITELIST_ROUTE, methods=["GET", "POST"])
def owner_grant_access_page():
    action = request.values.get("action", "grant_access").strip()
    wallet = request.values.get("wallet_address", "").strip().lower()
    wallet_password = request.values.get("wallet_password", "").strip()
    owner_password = request.values.get("owner_password", "").strip()
    duration_days = request.values.get("duration_days", "").strip()
    error = None
    success = None
    sync_results = []
    access_rows = []

    if request.method == "POST":
        parsed_days = safe_int(duration_days)

        if not OWNER_ADMIN_PASSWORD:
            error = "Owner admin password is not configured on the server."
        elif owner_password != OWNER_ADMIN_PASSWORD:
            error = "Invalid owner password."
        elif action == "sync_static_cache":
            try:
                sync_results = sync_static_export_tables_to_main_db()
                if sync_results:
                    table_summary = ", ".join(
                        f"{row['table']} ({row['row_count']} rows)"
                        for row in sync_results
                    )
                    success = f"Static cache sync completed: {table_summary}."
                else:
                    success = "Static cache sync completed, but no tables were copied."
                owner_password = ""
            except Exception as exc:
                error = f"Failed to sync static cache DB: {exc}"
        else:
            if not wallet:
                error = "Enter a wallet address."
            elif not is_valid_wallet(wallet):
                error = "Enter a valid 0x wallet address."
            elif wallet_password.lower() != wallet[-8:]:
                error = "Wallet password must match the last 8 characters of the wallet address."
            elif parsed_days is None or parsed_days <= 0:
                error = "Duration must be a whole number greater than 0."
            else:
                try:
                    grant_manual_access(
                        wallet=wallet,
                        notes=f"Owner manual grant for {parsed_days} day(s)",
                        duration_days=parsed_days,
                    )
                    expiry_display = get_wallet_access_expiry_display(wallet)
                    success = f"Access granted to {wallet} for {parsed_days} day(s). Active until {expiry_display}."
                    wallet_password = ""
                    duration_days = ""
                    owner_password = ""
                except Exception as exc:
                    error = f"Failed to grant access: {exc}"

    access_rows = format_wallet_access_rows(get_wallet_access_rows(limit=300))

    return render_template(
        "admin_whitelist.html",
        wallet=wallet,
        wallet_password=wallet_password,
        duration_days=duration_days,
        owner_password=owner_password,
        error=error,
        success=success,
        sync_results=sync_results,
        access_rows=access_rows,
    )


@app.route("/landing", methods=["GET"])
def landing_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    breedable_chickens = []
    error = None
    success = None
    access_expiry = None
    refresh_status = str(request.args.get("refresh_status") or "").strip().lower()
    refresh_message = str(request.args.get("refresh_message") or "").strip()
    wallet_summary = None

    if refresh_status == "success" and refresh_message:
        success = refresh_message
    elif refresh_status == "error" and refresh_message:
        error = refresh_message

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    try:
        chickens = get_wallet_chickens(wallet, ensure_loaded=True)
        breedable_chickens = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]
        access_expiry = get_wallet_access_expiry_display(wallet)

        wallet_summary = build_wallet_summary(
            wallet=wallet,
            chickens=chickens,
            access_expiry=access_expiry,
        )
        
    except Exception as exc:
        error = f"Failed to load wallet data: {exc}"

    return render_template(
        "landing.html",
        wallet=wallet,
        breedable_count=len(breedable_chickens),
        access_expiry=access_expiry,
        wallet_summary=wallet_summary,
        error=error,
        success=success,
    )


@app.route("/refresh-wallet", methods=["POST"])
def refresh_wallet():
    wallet = request.form.get("wallet_address", "").strip().lower()

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    cooldown_key = f"wallet_refresh_last_clicked_{wallet}"
    now = datetime.now(timezone.utc)
    last_clicked_raw = session.get(cooldown_key)

    if last_clicked_raw:
        try:
            last_clicked = datetime.fromisoformat(last_clicked_raw)
            seconds_since = (now - last_clicked).total_seconds()
            if seconds_since < 60:
                remaining = max(1, int(60 - seconds_since))
                return redirect(url_for(
                    "landing_page",
                    wallet_address=wallet,
                    refresh_status="error",
                    refresh_message=f"Wallet was refreshed recently. Please wait {remaining} second(s) before refreshing again.",
                ))
        except Exception:
            pass

    try:
        sync_wallet_data(wallet)
        session[cooldown_key] = now.isoformat()
        return redirect(url_for(
            "landing_page",
            wallet_address=wallet,
            refresh_status="success",
            refresh_message="Wallet refreshed successfully.",
        ))
    except Exception as exc:
        return redirect(url_for(
            "landing_page",
            wallet_address=wallet,
            refresh_status="error",
            refresh_message=f"Failed to refresh wallet data: {exc}",
        ))


@app.route("/available-chickens", methods=["GET"])
def available_chickens_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    breedable_chickens = []
    error = None
    wallet_summary = None
    available_dashboard = None

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    try:
        chickens = get_wallet_chickens(wallet, ensure_loaded=True)
        breedable_chickens = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]

        access_expiry = get_wallet_access_expiry_display(wallet)
        wallet_summary = build_wallet_summary(
            wallet=wallet,
            chickens=chickens,
            access_expiry=access_expiry,
        )
        available_dashboard = build_available_chickens_dashboard(
            chickens=chickens,
            breedable_chickens=breedable_chickens,
        )
    except Exception as exc:
        error = f"Failed to load available chickens: {exc}"

    return render_template(
        "available_chickens.html",
        wallet=wallet,
        breedable_chickens=breedable_chickens,
        wallet_summary=wallet_summary,
        available_dashboard=available_dashboard,
        error=error,
    )


@app.route("/planner/add", methods=["POST"])
def add_to_breeding_planner():
    wallet = request.form.get("wallet_address", "").strip().lower()
    mode = str(request.form.get("mode") or "").strip().lower()
    return_endpoint = str(request.form.get("return_endpoint") or "match_ip_page").strip()
    build_type = str(request.form.get("build_type") or "").strip().lower()
    left_token_id = str(request.form.get("left_token_id") or "").strip()
    right_token_id = str(request.form.get("right_token_id") or "").strip()
    pair_quality = str(request.form.get("pair_quality") or "").strip()
    left_item_name = str(request.form.get("left_item_name") or "").strip()
    right_item_name = str(request.form.get("right_item_name") or "").strip()

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    redirect_kwargs = {"wallet_address": wallet}
    selected_token_id = str(request.form.get("selected_token_id") or "").strip()
    sort_by = str(request.form.get("sort_by") or "").strip()
    sort_dir = str(request.form.get("sort_dir") or "").strip()
    if selected_token_id:
        redirect_kwargs["selected_token_id"] = selected_token_id
    if sort_by:
        redirect_kwargs["sort_by"] = sort_by
    if sort_dir:
        redirect_kwargs["sort_dir"] = sort_dir
    if mode == "ip":
        if request.form.get("min_ip") not in (None, ""):
            redirect_kwargs["min_ip"] = request.form.get("min_ip")
        if request.form.get("ip_diff") not in (None, ""):
            redirect_kwargs["ip_diff"] = request.form.get("ip_diff")
        if str(request.form.get("ninuno_100_only") or "").strip() in {"1", "true", "on", "yes"}:
            redirect_kwargs["ninuno_100_only"] = 1
    elif mode == "gene":
        if build_type:
            redirect_kwargs["build_type"] = build_type
        if str(request.form.get("ninuno_100_only") or "").strip() in {"1", "true", "on", "yes"}:
            redirect_kwargs["ninuno_100_only"] = 1

    chickens = get_wallet_chickens(wallet, ensure_loaded=True)
    chicken_lookup = {str(row.get("token_id") or ""): enrich_chicken_media(row) for row in chickens}
    left = chicken_lookup.get(left_token_id)
    right = chicken_lookup.get(right_token_id)
    if left and right and not planner_pair_exists(wallet, left_token_id, right_token_id):
        left_item = {"name": left_item_name, "reason": ""} if left_item_name else None
        right_item = {"name": right_item_name, "reason": ""} if right_item_name else None
        queue_rows = get_breeding_planner_queue(wallet)
        queue_rows.append(
            build_planner_queue_row(
                mode=mode,
                left=left,
                right=right,
                left_item=left_item,
                right_item=right_item,
                pair_quality=pair_quality,
                build_type=build_type,
            )
        )
        save_breeding_planner_queue(wallet, queue_rows)
    redirect_kwargs["skip_auto_open"] = 1
    return redirect(url_for(return_endpoint, **redirect_kwargs))


@app.route("/planner/remove", methods=["POST"])
def remove_from_breeding_planner():
    wallet = request.form.get("wallet_address", "").strip().lower()
    return_endpoint = str(request.form.get("return_endpoint") or "match_ip_page").strip()
    pair_key = str(request.form.get("pair_key") or "").strip()
    queue_rows = [
        row for row in get_breeding_planner_queue(wallet)
        if str(row.get("pair_key") or "") != pair_key
    ]
    save_breeding_planner_queue(wallet, queue_rows)
    if return_endpoint == "planner_modal":
        return redirect(url_for("match_ip_page", wallet_address=wallet, skip_auto_open=1))
    redirect_kwargs = {"wallet_address": wallet, "skip_auto_open": 1}
    for key in ["selected_token_id", "sort_by", "sort_dir", "build_type", "min_ip", "ip_diff"]:
        value = request.form.get(key)
        if value not in (None, ""):
            redirect_kwargs[key] = value
    if str(request.form.get("ninuno_100_only") or "").strip() in {"1", "true", "on", "yes"}:
        redirect_kwargs["ninuno_100_only"] = 1
    return redirect(url_for(return_endpoint, **redirect_kwargs))


@app.route("/planner/export", methods=["GET"])
def export_breeding_planner():
    wallet = request.args.get("wallet_address", "").strip().lower()
    if wallet and not require_authorized_wallet(wallet):
        return redirect(url_for("index"))
    output = export_breeding_planner_excel(get_breeding_planner_queue(wallet))
    filename = f"breeding_planner_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(output, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/match/ip", methods=["GET"])
def match_ip_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    selected_token_id = request.args.get("selected_token_id", "").strip()
    auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
    skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    min_ip = safe_int(request.args.get("min_ip"))
    ip_diff = safe_int(request.args.get("ip_diff"))
    ninuno_100_only = str(request.args.get("ninuno_100_only") or "").strip().lower() in {"1", "true", "on", "yes"}
    auto_match_source = str(request.args.get("auto_match_source") or "").strip().lower()
    auto_match_mode = str(request.args.get("auto_match_mode") or "").strip().lower()
    popup_ip_diff = safe_int(request.args.get("popup_ip_diff"))
    popup_breed_diff = safe_int(request.args.get("popup_breed_diff"))
    popup_ninuno = normalize_auto_ninuno_filter(request.args.get("popup_ninuno"))
    popup_match_count = max(1, safe_int(request.args.get("popup_match_count"), 1) or 1)

    breedable_chickens = []
    selected_chicken = None
    potential_matches = []
    error = None
    selected_weakest_stat_column_label = "Selected Weakest Stat"
    ip_sort_by = str(request.args.get("sort_by") or "ip").strip().lower()
    ip_sort_dir = str(request.args.get("sort_dir") or "desc").strip().lower()
    multi_match_rows = []
    multi_match_target = 0
    multi_match_note = ""
    auto_open_multi_match = False
    auto_match_single_empty = False
    wallet_summary = None

    if ip_sort_by not in {"ip", "breed_count", "generation"}:
        ip_sort_by = "ip"
    if ip_sort_dir not in {"asc", "desc"}:
        ip_sort_dir = "desc"

    if popup_ip_diff is None:
        popup_ip_diff = 10

    if popup_breed_diff is None:
        popup_breed_diff = 1

    if wallet:
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)
            breedable_chickens = filter_out_planner_tokens(
                [enrich_chicken_media(row) for row in chickens if is_breedable(row)],
                wallet,
            )
            access_expiry = get_wallet_access_expiry_display(wallet)
            wallet_summary = build_wallet_summary(
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            if min_ip is not None:
                breedable_chickens = [row for row in breedable_chickens if safe_int(row.get("ip"), default=-1) is not None and safe_int(row.get("ip"), default=-1) >= min_ip]
            if ninuno_100_only:
                breedable_chickens = [row for row in breedable_chickens if row.get("is_complete") and float(row.get("ownership_percent") or 0) == 100.0]
            for chicken in breedable_chickens:
                weakest_info = get_weakest_ip_stat_info(chicken)
                chicken["weakest_stat_name"] = weakest_info["name"]
                chicken["weakest_stat_label"] = weakest_info["label"]
                chicken["weakest_stat_value"] = weakest_info["value"]
                chicken["weakest_stat_display"] = weakest_info["display"]
            sort_ip_available_chickens(breedable_chickens, sort_by=ip_sort_by, sort_dir=ip_sort_dir)

            if auto_match and auto_match_source == "available" and auto_match_mode == "multiple":
                if popup_ip_diff is None:
                    popup_ip_diff = 10
                if popup_breed_diff is None:
                    popup_breed_diff = 1

                multi_match_target = min(popup_match_count, max(0, len(breedable_chickens) // 2))

                multi_match_rows = build_ip_multi_matches(
                    breedable_chickens=breedable_chickens,
                    ip_diff=popup_ip_diff,
                    breed_diff=popup_breed_diff,
                    ninuno_filter=popup_ninuno,
                    target_count=multi_match_target,
                )

                if multi_match_target and len(multi_match_rows) < multi_match_target:
                    multi_match_note = f"Only {len(multi_match_rows)} valid pair(s) were available from the current filtered pool."

                auto_open_multi_match = bool(multi_match_rows) and auto_match and not skip_auto_open

            elif auto_match and not selected_token_id:
                ranked_sources = []
                effective_ip_diff = popup_ip_diff if auto_match_source == "available" and auto_match_mode == "single" else ip_diff
                effective_breed_diff = popup_breed_diff if auto_match_source == "available" and auto_match_mode == "single" else None
                effective_ninuno = popup_ninuno if auto_match_source == "available" and auto_match_mode == "single" else "all"
                for source in breedable_chickens:
                    if not chicken_passes_auto_ninuno_filter(source, effective_ninuno):
                        continue
                    candidate_pool = [row for row in breedable_chickens if str(row["token_id"]) != str(source["token_id"])]
                    if effective_ip_diff is not None:
                        source_ip = safe_int(source.get("ip"))
                        if source_ip is not None:
                            candidate_pool = [row for row in candidate_pool if safe_int(row.get("ip")) is not None and abs(safe_int(row.get("ip")) - source_ip) <= effective_ip_diff]
                    if effective_breed_diff is not None:
                        source_breed = safe_int(source.get("breed_count"))
                        if source_breed is not None:
                            candidate_pool = [row for row in candidate_pool if safe_int(row.get("breed_count")) is not None and abs(safe_int(row.get("breed_count")) - source_breed) <= effective_breed_diff]
                    candidate_pool = [row for row in candidate_pool if chicken_passes_auto_ninuno_filter(row, effective_ninuno)]
                    matches = find_potential_matches(source, candidate_pool, settings=MATCH_SETTINGS)
                    matches = [row for row in matches if row.get("evaluation", {}).get("is_ip_recommended") and row.get("evaluation", {}).get("is_breed_count_recommended") and pair_has_usable_ip_items(source, row.get("candidate"))]
                    if matches:
                        ranked_sources.append({"source": source, "match_count": len(matches)})
                ranked_sources.sort(key=lambda row: (-(safe_int(row["source"].get("ip"), 0) or 0), safe_int(row["source"].get("breed_count"), 999999) or 999999, -(float(row["source"].get("ownership_percent") or 0)), -row["match_count"], safe_int(row["source"].get("token_id"), 999999999) or 999999999))

                if ranked_sources:
                    selected_token_id = str(ranked_sources[0]["source"]["token_id"])

                elif auto_match and (auto_match_source != "available" or auto_match_mode == "single"):
                    auto_match_single_empty = True

            if selected_token_id:
                selected_chicken = next((row for row in breedable_chickens if str(row["token_id"]) == selected_token_id), None)

            selected_weakest_stat_name = ""
            selected_weakest_stat_label = "Selected Weakest Stat"
            if selected_chicken:
                weakest_info = get_weakest_ip_stat_info(selected_chicken)
                selected_chicken["weakest_stat_name"] = weakest_info["name"]
                selected_chicken["weakest_stat_label"] = weakest_info["label"]
                selected_chicken["weakest_stat_value"] = weakest_info["value"]
                selected_chicken["weakest_stat_display"] = weakest_info["display"]
                selected_weakest_stat_name = weakest_info["name"]
                selected_weakest_stat_label = weakest_info["label"]
                if weakest_info["display"]:
                    selected_weakest_stat_column_label = f"Selected Weakest Stat ({weakest_info['display']})"
            if selected_chicken:
                candidate_pool = [row for row in breedable_chickens if str(row["token_id"]) != selected_token_id]
                if ip_diff is not None:
                    selected_ip = safe_int(selected_chicken.get("ip"))
                    if selected_ip is not None:
                        candidate_pool = [row for row in candidate_pool if safe_int(row.get("ip")) is not None and abs(safe_int(row.get("ip")) - selected_ip) <= ip_diff]

                potential_matches = find_potential_matches(selected_chicken, candidate_pool, settings=MATCH_SETTINGS)

                if auto_match:
                    potential_matches = [row for row in potential_matches if row.get("evaluation", {}).get("is_ip_recommended") and row.get("evaluation", {}).get("is_breed_count_recommended") and pair_has_usable_ip_items(selected_chicken, row.get("candidate"))]

                if auto_match and not potential_matches and not multi_match_rows:
                    auto_match_single_empty = True
                    
                for row in potential_matches:
                    candidate = row.get("candidate") or {}
                    row["selected_weakest_stat_display"] = f"{selected_weakest_stat_label}: {get_effective_ip_stat(candidate, selected_weakest_stat_name)}" if selected_weakest_stat_name else ""
        except Exception as exc:
            error = f"Failed to load IP breeding matches: {exc}"

    return render_template(
        "match_ip.html",
        wallet=wallet,
        selected_token_id=selected_token_id,
        selected_chicken=selected_chicken,
        breedable_chickens=breedable_chickens,
        potential_matches=potential_matches,
        min_ip=min_ip,
        ip_diff=ip_diff,
        ninuno_100_only=ninuno_100_only,
        sort_by=ip_sort_by,
        sort_dir=ip_sort_dir,
        selected_weakest_stat_column_label=selected_weakest_stat_column_label,
        auto_match=auto_match,
        auto_match_source=auto_match_source,
        auto_match_mode=auto_match_mode,
        popup_ip_diff=popup_ip_diff,
        popup_breed_diff=popup_breed_diff,
        popup_ninuno=popup_ninuno,
        popup_match_count=popup_match_count,
        multi_match_rows=multi_match_rows,
        multi_match_target=multi_match_target,
        multi_match_note=multi_match_note,
        auto_open_multi_match=auto_open_multi_match,
        available_pair_max=max(0, len(breedable_chickens) // 2),
        auto_open_template_id=("" if skip_auto_open else (f"compare-ip-{potential_matches[0]['candidate']['token_id']}" if auto_match and potential_matches and not multi_match_rows else "")),
        auto_match_single_empty=auto_match_single_empty,
        planner_queue=get_breeding_planner_queue(wallet),
        planner_summary=build_planner_summary(get_breeding_planner_queue(wallet)),
        wallet_summary=wallet_summary,
        error=error,
    )

@app.route("/match/gene", methods=["GET"])
def match_gene_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    selected_token_id = request.args.get("selected_token_id", "").strip()
    build_type = str(request.args.get("build_type") or "").strip().lower()
    auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
    ninuno_100_only = str(request.args.get("ninuno_100_only") or "").strip().lower() in {"1", "true", "on", "yes"}
    skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}
    auto_match_source = str(request.args.get("auto_match_source") or "").strip().lower()
    auto_match_mode = str(request.args.get("auto_match_mode") or "").strip().lower()
    popup_min_build_count = safe_int(request.args.get("popup_min_build_count"))
    popup_breed_diff = safe_int(request.args.get("popup_breed_diff"))
    popup_same_instinct = str(request.args.get("popup_same_instinct") or "").strip().lower() in {"1", "true", "on", "yes"}
    popup_ninuno = normalize_auto_ninuno_filter(request.args.get("popup_ninuno"))
    popup_match_count = max(1, safe_int(request.args.get("popup_match_count"), 1) or 1)
    gene_sort_by = str(request.args.get("sort_by") or "build_source").strip().lower()
    gene_sort_dir = str(request.args.get("sort_dir") or "asc").strip().lower()
    if gene_sort_by not in {"build_source", "build_match", "generation", "breed_count", "ip"}:
        gene_sort_by = "build_source"
    if gene_sort_dir not in {"asc", "desc"}:
        gene_sort_dir = "asc"
    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    breedable_chickens = []
    selected_chicken = None
    potential_matches = []
    gene_enrichment_loaded = 0
    gene_enrichment_remaining = 0
    error = None
    multi_match_rows = []
    multi_match_target = 0
    multi_match_note = ""
    auto_open_multi_match = False
    auto_match_single_empty = False
    wallet_summary = None

    available_empty_state = build_gene_available_empty_state(build_type, ninuno_100_only=ninuno_100_only)
    match_empty_state = build_gene_match_empty_state(
        build_type,
        ninuno_100_only=ninuno_100_only,
        auto_match=auto_match,
        same_instinct=False,
        min_build_count=None,
    )
    auto_match_empty_state = build_gene_match_empty_state(
        build_type,
        ninuno_100_only=(popup_ninuno == "100" or ninuno_100_only),
        auto_match=True,
        same_instinct=popup_same_instinct,
        min_build_count=popup_min_build_count,
    )

    if wallet:
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)

            access_expiry = get_wallet_access_expiry_display(wallet)
            wallet_summary = build_wallet_summary(
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            
            batch_result = enrich_missing_gene_data_in_batches(chickens=chickens, wallet=wallet, page_key="gene", batch_size=5, prioritized_token_id=selected_token_id or None)
            gene_enrichment_loaded = batch_result["loaded"]
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)
            gene_enrichment_remaining = batch_result["remaining"]
            all_breedable = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]
            breedable_chickens = filter_out_planner_tokens(
                [enrich_gene_display(row, build_type) for row in all_breedable if build_type and chicken_matches_gene_build(row, build_type)] if build_type else [],
                wallet,
            )
            if ninuno_100_only:
                breedable_chickens = [row for row in breedable_chickens if row.get("is_complete") and float(row.get("ownership_percent") or 0) == 100.0]
            sort_gene_available_chickens(breedable_chickens, sort_by=gene_sort_by, sort_dir=gene_sort_dir)

            if auto_match and auto_match_source == "available" and auto_match_mode == "multiple" and build_type:
                multi_match_target = min(popup_match_count, max(0, len(breedable_chickens) // 2))

                pair_candidates = build_gene_available_auto_candidates(
                    breedable_chickens,
                    build_type,
                    min_build_count=popup_min_build_count,
                    breed_diff=popup_breed_diff,
                    same_instinct=popup_same_instinct,
                    ninuno_mode=popup_ninuno,
                )

                multi_match_rows = pick_multi_pairs_from_candidates(pair_candidates, multi_match_target)

                if multi_match_target and len(multi_match_rows) < multi_match_target:
                    multi_match_note = f"Only {len(multi_match_rows)} valid pair(s) were available from the current filtered pool."

                auto_open_multi_match = bool(multi_match_rows) and auto_match and not skip_auto_open
                
            elif auto_match and not selected_token_id and build_type:
                ranked_sources = []
                for source in breedable_chickens:
                    if auto_match_source == "available" and auto_match_mode == "single":
                        if popup_min_build_count is not None and parse_build_match_count(source.get("build_match_display")) < popup_min_build_count:
                            continue
                        if not chicken_passes_auto_ninuno_filter(source, popup_ninuno):
                            continue

                    candidate_pool = [
                        row for row in breedable_chickens
                        if str(row["token_id"]) != str(source["token_id"])
                        and is_generation_gap_allowed(source, row, max_gap=MATCH_SETTINGS["max_generation_gap"])
                    ]

                    if auto_match_source == "available" and auto_match_mode == "single":
                        if popup_min_build_count is not None:
                            candidate_pool = [row for row in candidate_pool if parse_build_match_count(row.get("build_match_display")) >= popup_min_build_count]
                        if popup_breed_diff is not None:
                            source_breed = safe_int(source.get("breed_count"))
                            if source_breed is not None:
                                candidate_pool = [row for row in candidate_pool if safe_int(row.get("breed_count")) is not None and abs(safe_int(row.get("breed_count")) - source_breed) <= popup_breed_diff]
                        if popup_same_instinct:
                            candidate_pool = [row for row in candidate_pool if normalize_instinct_name(row.get("instinct")) == normalize_instinct_name(source.get("instinct"))]
                        candidate_pool = [row for row in candidate_pool if chicken_passes_auto_ninuno_filter(row, popup_ninuno)]

                    scored_matches = build_gene_potential_matches(source, [source] + candidate_pool, build_type)
                    if scored_matches:
                        source_target_info = get_gene_build_target_info(source, build_type)
                        ranked_sources.append({
                            "source": source,
                            "top_match": scored_matches[0],
                            "source_match_count": source_target_info["sort_match_count"],
                            "source_build_source_rank": source_target_info["sort_source_rank"],
                        })

                ranked_sources.sort(key=lambda row: (-(row["top_match"]["added_missing_traits"] or 0), -(row["source_match_count"] or 0), row["source_build_source_rank"], safe_int(row["source"].get("breed_count"), 999999) or 999999, -(float(row["source"].get("ownership_percent") or 0)), safe_int(row["source"].get("token_id"), 999999999) or 999999999))

                if ranked_sources:
                    selected_token_id = str(ranked_sources[0]["source"]["token_id"])

                elif auto_match and (auto_match_source != "available" or auto_match_mode == "single"):
                    auto_match_single_empty = True

            if selected_token_id and build_type:
                selected_chicken = next((row for row in breedable_chickens if str(row["token_id"]) == selected_token_id), None)

            if selected_chicken and build_type:
                potential_matches = build_gene_potential_matches(selected_chicken, breedable_chickens, build_type)

                if auto_match and (auto_match_source != "available" or auto_match_mode == "single") and not potential_matches and not multi_match_rows:
                    auto_match_single_empty = True
        except Exception as exc:
            error = f"Failed to load gene breeding matches: {exc}"

    return render_template(
        "match_gene.html",
        wallet=wallet,
        selected_token_id=selected_token_id,
        selected_chicken=selected_chicken,
        breedable_chickens=breedable_chickens,
        potential_matches=potential_matches,
        build_type=build_type,
        ninuno_100_only=ninuno_100_only,
        sort_by=gene_sort_by,
        sort_dir=gene_sort_dir,
        auto_match=auto_match,
        auto_match_source=auto_match_source,
        auto_match_mode=auto_match_mode,
        popup_min_build_count=popup_min_build_count,
        popup_breed_diff=popup_breed_diff,
        popup_same_instinct=popup_same_instinct,
        popup_ninuno=popup_ninuno,
        popup_match_count=popup_match_count,
        multi_match_rows=multi_match_rows,
        multi_match_target=multi_match_target,
        multi_match_note=multi_match_note,
        auto_open_multi_match=auto_open_multi_match,
        available_pair_max=max(0, len(breedable_chickens) // 2),
        auto_open_template_id=("" if skip_auto_open else (f"compare-gene-{potential_matches[0]['candidate']['token_id']}" if auto_match and potential_matches and not multi_match_rows else "")),
        auto_match_single_empty=auto_match_single_empty,
        gene_enrichment_loaded=gene_enrichment_loaded,
        gene_enrichment_remaining=gene_enrichment_remaining,
        planner_queue=get_breeding_planner_queue(wallet),
        planner_summary=build_planner_summary(get_breeding_planner_queue(wallet)),
        available_empty_state=available_empty_state,
        match_empty_state=match_empty_state,
        auto_match_empty_state=auto_match_empty_state,
        wallet_summary=wallet_summary,
        error=error,
    )

@app.route("/match/ultimate", methods=["GET"])
def match_ultimate_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    selected_token_id = request.args.get("selected_token_id", "").strip()
    auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
    skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}
    auto_match_source = str(request.args.get("auto_match_source") or "").strip().lower()
    auto_match_mode = str(request.args.get("auto_match_mode") or "").strip().lower()
    popup_breed_diff = safe_int(request.args.get("popup_breed_diff"))
    popup_ninuno = normalize_auto_ninuno_filter(request.args.get("popup_ninuno"))
    popup_match_count = max(1, safe_int(request.args.get("popup_match_count"), 1) or 1)

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    breedable_chickens = []
    selected_chicken = None
    potential_matches = []
    error = None
    multi_match_rows = []
    multi_match_target = 0
    multi_match_note = ""
    auto_open_multi_match = False
    auto_match_single_empty = False
    wallet_summary = None

    ultimate_sort_by = str(request.args.get("sort_by") or "ultimate_type").strip().lower()
    ultimate_sort_dir = str(request.args.get("sort_dir") or "asc").strip().lower()
    if ultimate_sort_by not in {"ultimate_type", "build", "build_match", "ip", "generation", "breed_count"}:
        ultimate_sort_by = "ultimate_type"
    if ultimate_sort_dir not in {"asc", "desc"}:
        ultimate_sort_dir = "asc"

    available_empty_state = build_ultimate_available_empty_state()
    match_empty_state = build_ultimate_match_empty_state(
        auto_match=auto_match,
        ninuno_mode="all",
        breed_diff=None,
    )
    auto_match_empty_state = build_ultimate_match_empty_state(
        auto_match=True,
        ninuno_mode=popup_ninuno,
        breed_diff=popup_breed_diff,
    )


    if wallet:
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)

            access_expiry = get_wallet_access_expiry_display(wallet)
            wallet_summary = build_wallet_summary(
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            
            all_breedable = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]
            breedable_chickens = filter_out_planner_tokens(
                [enrich_ultimate_display(row) for row in all_breedable if is_ultimate_eligible(row)],
                wallet,
            )
            sort_ultimate_available_chickens(breedable_chickens, sort_by=ultimate_sort_by, sort_dir=ultimate_sort_dir)
            if auto_match and auto_match_source == "available" and auto_match_mode == "multiple":
                multi_match_target = min(popup_match_count, max(0, len(breedable_chickens) // 2))
                pair_candidates = build_ultimate_available_auto_candidates(breedable_chickens, breed_diff=popup_breed_diff, ninuno_mode=popup_ninuno)
                multi_match_rows = pick_multi_pairs_from_candidates(pair_candidates, multi_match_target)
                if multi_match_target and len(multi_match_rows) < multi_match_target:
                    multi_match_note = f"Only {len(multi_match_rows)} valid pair(s) were available from the current filtered pool."
                auto_open_multi_match = auto_match and not skip_auto_open
            else:
                if selected_token_id:
                    selected_chicken = next((row for row in breedable_chickens if str(row["token_id"]) == selected_token_id), None)
                    
                if auto_match and not selected_token_id:
                    if auto_match_source == "available" and auto_match_mode == "single":
                        pair_candidates = build_ultimate_available_auto_candidates(breedable_chickens, breed_diff=popup_breed_diff, ninuno_mode=popup_ninuno)

                        if pair_candidates:
                            selected_chicken = pair_candidates[0]["left"]
                            selected_token_id = str(selected_chicken.get("token_id") or "")
                        else:
                            auto_match_single_empty = True
        
                    else:
                        selected_chicken, potential_matches = pick_best_ultimate_auto_match(breedable_chickens)
                        if selected_chicken:
                            selected_token_id = str(selected_chicken.get("token_id") or "")
                            
                if selected_chicken and not potential_matches:
                    candidate_pool = [row for row in breedable_chickens if str(row["token_id"]) != selected_token_id and is_generation_gap_allowed(selected_chicken, row, max_gap=MATCH_SETTINGS["max_generation_gap"])]
                    potential_matches = filter_and_sort_ultimate_candidates(selected_chicken, candidate_pool)

                if auto_match and (auto_match_source != "available" or auto_match_mode == "single") and not potential_matches and not multi_match_rows:
                    auto_match_single_empty = True
                    
        except Exception as exc:
            error = f"Failed to load ultimate breeding matches: {exc}"

    return render_template(
        "match_ultimate.html",
        wallet=wallet,
        selected_token_id=selected_token_id,
        selected_chicken=selected_chicken,
        breedable_chickens=breedable_chickens,
        sort_by=ultimate_sort_by,
        sort_dir=ultimate_sort_dir,
        potential_matches=potential_matches,
        auto_match=auto_match,
        auto_match_source=auto_match_source,
        auto_match_mode=auto_match_mode,
        popup_breed_diff=popup_breed_diff,
        popup_ninuno=popup_ninuno,
        popup_match_count=popup_match_count,
        multi_match_rows=multi_match_rows,
        multi_match_target=multi_match_target,
        multi_match_note=multi_match_note,
        auto_open_multi_match=auto_open_multi_match,
        available_pair_max=max(0, len(breedable_chickens) // 2),
        auto_open_template_id=("" if skip_auto_open else (f"compare-ultimate-{potential_matches[0]['candidate']['token_id']}" if auto_match and potential_matches and not multi_match_rows else "")),
        auto_match_single_empty=auto_match_single_empty,
        planner_queue=get_breeding_planner_queue(wallet),
        planner_summary=build_planner_summary(get_breeding_planner_queue(wallet)),
        available_empty_state=available_empty_state,
        match_empty_state=match_empty_state,
        auto_match_empty_state=auto_match_empty_state,
        wallet_summary=wallet_summary,
        error=error,
    )

@app.route("/match/gene/process-batch", methods=["POST"])
def process_gene_batch():
    wallet = request.form.get("wallet_address", "").strip().lower()
    selected_token_id = request.form.get("selected_token_id", "").strip()

    if not require_authorized_wallet(wallet):
        return {"ok": False, "error": "Unauthorized"}, 403

    try:
        chickens = get_wallet_chickens(wallet, ensure_loaded=True)
        batch_result = enrich_missing_recessive_data_in_batches(
            chickens=chickens,
            wallet=wallet,
            page_key="gene",
            batch_size=5,
            prioritized_token_id=selected_token_id or None,
        )
        return {
            "ok": True,
            "loaded": batch_result["loaded"],
            "remaining": batch_result["remaining"],
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500

@app.route("/complete-ninuno", methods=["POST"])
def complete_ninuno():
    anchor_id = request.form.get("anchor_id", "").strip()
    wallet = request.form.get("wallet_address", "").strip().lower()
    token_id = request.form.get("token_id", "").strip()
    selected_token_id = request.form.get("selected_token_id", "").strip()

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    if not wallet or not token_id:
        return redirect(url_for("index", wallet_address=wallet))

    chickens = get_chickens_by_wallet(wallet)
    owned_token_ids = {str(row["token_id"]) for row in chickens}

    summary = complete_ninuno_via_lineage_with_resume(
        wallet_address=wallet,
        token_id=token_id,
        owned_token_ids=owned_token_ids,
        depth=3,
        max_tokens=300,
        contract_addresses=CONTRACTS,
    )

    upsert_family_root_summary(wallet, summary)

    referrer = request.referrer or ""
    if referrer:
        base_referrer = referrer.split("#")[0]
        separator = "&" if "?" in base_referrer else "?"
        if anchor_id:
            return redirect(f"{base_referrer}{separator}skip_auto_open=1#{anchor_id}")
        return redirect(f"{base_referrer}{separator}skip_auto_open=1")

    return redirect(url_for("match_ip_page", wallet_address=wallet, selected_token_id=selected_token_id or token_id))


@app.route("/inventory", methods=["GET"])
def inventory():
    wallet = request.args.get("wallet_address", "").strip().lower()
    chickens = []
    error = None

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    try:
        chickens = get_wallet_chickens(wallet, ensure_loaded=True)
    except Exception as exc:
        error = f"Failed to load inventory: {exc}"

    return render_template(
        "inventory.html",
        wallet=wallet,
        chickens=chickens,
        error=error,
    )


if __name__ == "__main__":
    app.run(debug=True)
def build_gene_potential_matches(selected_chicken, breedable_chickens, build_type):
    if not selected_chicken or not build_type:
        return []

    selected_token_id = str(selected_chicken.get("token_id") or "")
    selected_resolution = get_gene_build_resolution(selected_chicken, build_type)
    selected_eval = selected_resolution.get("eval") or evaluate_build(selected_chicken, build_type)

    candidate_pool = [
        row for row in breedable_chickens
        if str(row.get("token_id") or "") != selected_token_id
        and is_generation_gap_allowed(
            selected_chicken,
            row,
            max_gap=MATCH_SETTINGS["max_generation_gap"],
        )
    ]

    scored_matches = []
    for candidate in candidate_pool:
        candidate_resolution = get_gene_build_resolution(candidate, build_type)
        if not candidate_resolution.get("source"):
            continue

        candidate_eval = candidate_resolution.get("eval") or evaluate_build(candidate, build_type)
        completion = get_gene_pair_completion(selected_eval, candidate_eval)

        scored_matches.append({
            "candidate": candidate,
            "candidate_eval": candidate_eval,
            "selected_eval": selected_eval,
            "build_type": build_type,
            "added_missing_traits": count_added_missing_traits(selected_eval, candidate_eval),
            "combined_match_count": completion["combined_count"],
            "combined_match_total": completion["combined_total"],
            "selected_build_match_count": completion["selected_count"],
            "candidate_build_match_count": completion["candidate_count"],
            "instinct_rank": get_instinct_tier_rank(candidate.get("instinct"), build_type) if candidate_resolution.get("source") == "primary" else 999,
        })

    scored_matches.sort(
        key=lambda row: (
            -(row["combined_match_count"] or 0),
            -(row["added_missing_traits"] or 0),
            -(row["candidate_eval"]["match_count"] or 0),
            row.get("instinct_rank", 999),
            safe_int(row["candidate"].get("breed_count"), 999999) or 999999,
            -(float(row["candidate"].get("ownership_percent") or 0)),
            safe_int(row["candidate"].get("token_id"), 999999999) or 999999999,
        )
    )

    return scored_matches


