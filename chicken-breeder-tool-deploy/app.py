import sqlite3
from pathlib import Path
from services.db.connection import DB_PATH
from flask import Flask, render_template, request, redirect, url_for, session
import os
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
app.secret_key = "replace-this-with-a-real-secret-key"

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

STATIC_EXPORT_DB_PATH = DB_PATH.parent / "chicken_static_export.db"


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

STATIC_CHICKEN_CACHE_FIELDS = [
    "image",
    "generation_text",
    "generation_num",
    "parent_1",
    "parent_2",
    "breed_count",
    "gender",
    "type",
    "instinct",
    "innate_attack",
    "innate_defense",
    "innate_speed",
    "innate_health",
    "innate_ferocity",
    "innate_cockrage",
    "innate_evasion",
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
    "is_dead",
    "is_egg",
    "gene_profile_loaded",
    "recessive_build",
    "recessive_build_match_count",
    "recessive_build_match_total",
    "recessive_build_repeat_bonus",
]

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

    sync_results = sync_static_export_tables_to_main_db()

    return {
        "loaded": True,
        "synced_tables": sync_results,
        "missing_tables": missing_tables,
    }

def merge_static_chicken_cache(record, static_row):
    if not static_row:
        return record

    for field in STATIC_CHICKEN_CACHE_FIELDS:
        if field not in static_row:
            continue

        static_value = static_row.get(field)
        if static_value is None or static_value == "":
            continue

        record[field] = static_value

    return record


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


def sync_wallet_data(wallet):
    ensure_static_cache_tables_loaded()

    raw_items = fetch_all_owned_chickens(wallet, CONTRACTS)
    parsed_records = [parse_chicken_record(wallet, item) for item in raw_items]

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

    return get_chickens_by_wallet(wallet)


def get_wallet_chickens(wallet, ensure_loaded=False):
    if wallet:
        clear_stale_family_root_summaries(wallet, max_age_hours=24)

    chickens = get_chickens_by_wallet(wallet)
    if ensure_loaded and not chickens:
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

        result = [
            {
                "name": item["name"],
                "reason": item["reason"],
            }
            for item in pair_candidates
        ]

        if broad_count >= 3:
            result.append({
                "name": "Soulknot",
                "reason": "Best when this parent is strong across several usable stats.",
            })

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

    return {
        "name": "Gregor's Gift",
        "reason": "Best when this parent is being valued for primary build inheritance.",
    }


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

        candidates.append({
            "name": "Gregor's Gift",
            "reason": "Best when this parent is being valued for primary build inheritance.",
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
    }


def enrich_gene_display(chicken, build_type):
    row = dict(chicken)

    row["build_source_display"] = ""
    row["build_match_display"] = ""

    if not build_type:
        return row

    primary_build = str(row.get("primary_build") or "").strip().lower()
    recessive_build = str(row.get("recessive_build") or "").strip().lower()

    if primary_build == build_type:
        match_count = safe_int(row.get("primary_build_match_count"), 0) or 0
        match_total = safe_int(row.get("primary_build_match_total"), 0) or 0
        row["build_source_display"] = "Primary"
        row["build_match_display"] = f"{match_count}/{match_total}" if match_total else ""
    elif recessive_build == build_type:
        match_count = safe_int(row.get("recessive_build_match_count"), 0) or 0
        match_total = safe_int(row.get("recessive_build_match_total"), 0) or 0
        row["build_source_display"] = "Recessive"
        row["build_match_display"] = f"{match_count}/{match_total}" if match_total else ""

    return enrich_chicken_media(row)


def chicken_matches_gene_build(chicken, build_type):
    if not build_type:
        return False

    primary_build = str(chicken.get("primary_build") or "").strip().lower()
    recessive_build = str(chicken.get("recessive_build") or "").strip().lower()

    return (primary_build == build_type) or (recessive_build == build_type)


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
    selected_eval = evaluate_build(selected_chicken, build_type)

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
        candidate_eval = evaluate_build(candidate, build_type)

        scored_matches.append({
            "candidate": candidate,
            "candidate_eval": candidate_eval,
            "added_missing_traits": count_added_missing_traits(selected_eval, candidate_eval),
            "instinct_rank": get_instinct_tier_rank(candidate.get("instinct"), build_type) if str(candidate.get("build_source_display") or "").strip().lower() == "primary" else 999,
        })

    scored_matches.sort(
        key=lambda row: (
            -(row["added_missing_traits"] or 0),
            -(row["candidate_eval"]["match_count"] or 0),
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

def sort_key_text(value):
    return str(value or "").strip().lower()


def sort_key_int(value, default=0):
    parsed = safe_int(value, default)
    return parsed if parsed is not None else default


def sort_key_build_match(value):
    raw = str(value or "").strip()
    if "/" in raw:
        left, _, right = raw.partition("/")
        return (
            safe_int(left, 0) or 0,
            safe_int(right, 0) or 0,
        )
    return (0, 0)


def get_gene_build_source_rank(value):
    source = str(value or "").strip().lower()
    if source == "primary":
        return 0
    if source == "recessive":
        return 1
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
                    sync_wallet_data(wallet)

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
    access_expiry = None

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    try:
        chickens = get_wallet_chickens(wallet, ensure_loaded=True)
        breedable_chickens = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]
        access_expiry = get_wallet_access_expiry_display(wallet)
    except Exception as exc:
        error = f"Failed to load wallet data: {exc}"

    return render_template(
        "landing.html",
        wallet=wallet,
        breedable_count=len(breedable_chickens),
        access_expiry=access_expiry,
        error=error,
    )


@app.route("/available-chickens", methods=["GET"])
def available_chickens_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    breedable_chickens = []
    error = None

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    try:
        chickens = get_wallet_chickens(wallet, ensure_loaded=True)
        breedable_chickens = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]
    except Exception as exc:
        error = f"Failed to load available chickens: {exc}"

    return render_template(
        "available_chickens.html",
        wallet=wallet,
        breedable_chickens=breedable_chickens,
        error=error,
    )


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

    breedable_chickens = []
    selected_chicken = None
    potential_matches = []
    error = None
    selected_weakest_stat_column_label = "Selected Weakest Stat"
    ip_sort_by = str(request.args.get("sort_by") or "ip").strip().lower()
    ip_sort_dir = str(request.args.get("sort_dir") or "desc").strip().lower()

    if ip_sort_by not in {"ip", "breed_count", "generation"}:
        ip_sort_by = "ip"

    if ip_sort_dir not in {"asc", "desc"}:
        ip_sort_dir = "desc"
    
    if wallet:
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)
            breedable_chickens = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]

            if min_ip is not None:
                breedable_chickens = [
                    row for row in breedable_chickens
                    if safe_int(row.get("ip"), default=-1) is not None
                    and safe_int(row.get("ip"), default=-1) >= min_ip
                ]

            if ninuno_100_only:
                breedable_chickens = [
                    row for row in breedable_chickens
                    if row.get("is_complete")
                    and float(row.get("ownership_percent") or 0) == 100.0
                ]
                
            for chicken in breedable_chickens:
                weakest_info = get_weakest_ip_stat_info(chicken)
                chicken["weakest_stat_name"] = weakest_info["name"]
                chicken["weakest_stat_label"] = weakest_info["label"]
                chicken["weakest_stat_value"] = weakest_info["value"]
                chicken["weakest_stat_display"] = weakest_info["display"]
            
            sort_ip_available_chickens(breedable_chickens, sort_by=ip_sort_by, sort_dir=ip_sort_dir)
            
            if auto_match and not selected_token_id:
                ranked_sources = []

                for source in breedable_chickens:
                    candidate_pool = [
                        row for row in breedable_chickens
                        if str(row["token_id"]) != str(source["token_id"])
                    ]

                    if ip_diff is not None:
                        source_ip = safe_int(source.get("ip"))
                        if source_ip is not None:
                            candidate_pool = [
                                row for row in candidate_pool
                                if safe_int(row.get("ip")) is not None
                                and abs(safe_int(row.get("ip")) - source_ip) <= ip_diff
                            ]

                    matches = find_potential_matches(
                        source,
                        candidate_pool,
                        settings=MATCH_SETTINGS,
                    )

                    matches = [
                        row for row in matches
                        if row.get("evaluation", {}).get("is_ip_recommended")
                        and row.get("evaluation", {}).get("is_breed_count_recommended")
                        and pair_has_usable_ip_items(source, row.get("candidate"))
                    ]
                    
                    if matches:
                        top_match = matches[0]
                        ranked_sources.append({
                            "source": source,
                            "top_match": top_match,
                            "match_count": len(matches),
                        })

                ranked_sources.sort(
                    key=lambda row: (
                        -(safe_int(row["source"].get("ip"), 0) or 0),
                        safe_int(row["source"].get("breed_count"), 999999) or 999999,
                        -(float(row["source"].get("ownership_percent") or 0)),
                        -row["match_count"],
                        safe_int(row["source"].get("token_id"), 999999999) or 999999999,
                    )
                )

                if ranked_sources:
                    selected_token_id = str(ranked_sources[0]["source"]["token_id"])

            if selected_token_id:
                selected_chicken = next(
                    (row for row in breedable_chickens if str(row["token_id"]) == selected_token_id),
                    None,
                )

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
                candidate_pool = [
                    row for row in breedable_chickens
                    if str(row["token_id"]) != selected_token_id
                ]

                if ip_diff is not None:
                    selected_ip = safe_int(selected_chicken.get("ip"))
                    if selected_ip is not None:
                        candidate_pool = [
                            row for row in candidate_pool
                            if safe_int(row.get("ip")) is not None
                            and abs(safe_int(row.get("ip")) - selected_ip) <= ip_diff
                        ]

                potential_matches = find_potential_matches(
                    selected_chicken,
                    candidate_pool,
                    settings=MATCH_SETTINGS,
                )

                if auto_match:
                    potential_matches = [
                        row for row in potential_matches
                        if row.get("evaluation", {}).get("is_ip_recommended")
                        and row.get("evaluation", {}).get("is_breed_count_recommended")
                        and pair_has_usable_ip_items(selected_chicken, row.get("candidate"))
                    ]
                if selected_weakest_stat_name:
                    for row in potential_matches:
                        candidate = row.get("candidate") or {}
                        candidate_stat_value = get_effective_ip_stat(candidate, selected_weakest_stat_name)
                        row["selected_weakest_stat_display"] = f"{selected_weakest_stat_label}: {candidate_stat_value}"
                else:
                    for row in potential_matches:
                        row["selected_weakest_stat_display"] = ""
                        
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
        auto_open_template_id=(
            ""
            if skip_auto_open
            else (f"compare-ip-{potential_matches[0]['candidate']['token_id']}" if auto_match and potential_matches else "")
        ),
        error=error,
        
    )


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

@app.route("/match/gene", methods=["GET"])
def match_gene_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    selected_token_id = request.args.get("selected_token_id", "").strip()
    build_type = str(request.args.get("build_type") or "").strip().lower()
    auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
    ninuno_100_only = str(request.args.get("ninuno_100_only") or "").strip().lower() in {"1", "true", "on", "yes"}
    gene_enrichment_loaded = 0
    gene_enrichment_remaining = 0
    skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}

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

    if wallet:
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)

            batch_result = enrich_missing_gene_data_in_batches(
                chickens=chickens,
                wallet=wallet,
                page_key="gene",
                batch_size=5,
                prioritized_token_id=selected_token_id or None,
            )
            gene_enrichment_loaded = batch_result["loaded"]

            chickens = get_wallet_chickens(wallet, ensure_loaded=True)
            gene_enrichment_remaining = batch_result["remaining"]

            all_breedable = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]

            if build_type:
                breedable_chickens = [
                    enrich_gene_display(row, build_type)
                    for row in all_breedable
                    if chicken_matches_gene_build(row, build_type)
                ]
            else:
                breedable_chickens = []

            if ninuno_100_only:
                breedable_chickens = [
                    row for row in breedable_chickens
                    if row.get("is_complete")
                    and float(row.get("ownership_percent") or 0) == 100.0
                ]
                
            sort_gene_available_chickens(breedable_chickens, sort_by=gene_sort_by, sort_dir=gene_sort_dir)
            
            if auto_match and not selected_token_id and build_type:
                ranked_sources = []

                for source in breedable_chickens:
                    selected_eval = evaluate_build(source, build_type)

                    candidate_pool = [
                        row for row in breedable_chickens
                        if str(row["token_id"]) != str(source["token_id"])
                        and is_generation_gap_allowed(
                            source,
                            row,
                            max_gap=MATCH_SETTINGS["max_generation_gap"],
                        )
                    ]

                    scored_matches = []
                    for candidate in candidate_pool:
                        candidate_eval = evaluate_build(candidate, build_type)

                        scored_matches.append({
                            "candidate": candidate,
                            "candidate_eval": candidate_eval,
                            "added_missing_traits": count_added_missing_traits(selected_eval, candidate_eval),
                            "instinct_rank": get_instinct_tier_rank(candidate.get("instinct"), build_type)
                            if str(candidate.get("build_source_display") or "").strip().lower() == "primary"
                            else 999,
                        })

                    scored_matches.sort(
                        key=lambda row: (
                            -(row["added_missing_traits"] or 0),
                            -(row["candidate_eval"]["match_count"] or 0),
                            row.get("instinct_rank", 999),
                            safe_int(row["candidate"].get("breed_count"), 999999) or 999999,
                            -(float(row["candidate"].get("ownership_percent") or 0)),
                            safe_int(row["candidate"].get("token_id"), 999999999) or 999999999,
                        )
                    )

                    if scored_matches:
                        source_eval = evaluate_build(source, build_type)
                        ranked_sources.append({
                            "source": source,
                            "top_match": scored_matches[0],
                            "source_match_count": source_eval.get("match_count", 0),
                            "source_build_source": str(source.get("build_source_display") or "").strip().lower(),
                            "match_count": len(scored_matches),
                        })

                ranked_sources.sort(
                    key=lambda row: (
                        -(row["top_match"]["added_missing_traits"] or 0),
                        -(row["source_match_count"] or 0),
                        0 if row["source_build_source"] == "primary" else 1,
                        safe_int(row["source"].get("breed_count"), 999999) or 999999,
                        -(float(row["source"].get("ownership_percent") or 0)),
                        safe_int(row["source"].get("token_id"), 999999999) or 999999999,
                    )
                )

                if ranked_sources:
                    selected_token_id = str(ranked_sources[0]["source"]["token_id"])

            if selected_token_id and build_type:
                selected_chicken = next(
                    (row for row in breedable_chickens if str(row["token_id"]) == selected_token_id),
                    None,
                )

            if selected_chicken and build_type:
                selected_eval = evaluate_build(selected_chicken, build_type)

                candidate_pool = [
                    row for row in breedable_chickens
                    if str(row["token_id"]) != selected_token_id
                    and is_generation_gap_allowed(
                        selected_chicken,
                        row,
                        max_gap=MATCH_SETTINGS["max_generation_gap"],
                    )
                ]

                scored_matches = []
                for candidate in candidate_pool:
                    candidate_eval = evaluate_build(candidate, build_type)

                    scored_matches.append({
                        "candidate": candidate,
                        "candidate_eval": candidate_eval,
                        "added_missing_traits": count_added_missing_traits(selected_eval, candidate_eval),
                        "instinct_rank": get_instinct_tier_rank(candidate.get("instinct"), build_type)
                        if str(candidate.get("build_source_display") or "").strip().lower() == "primary"
                        else 999,
                    })

                scored_matches.sort(
                    key=lambda row: (
                        -(row["added_missing_traits"] or 0),
                        -(row["candidate_eval"]["match_count"] or 0),
                        row.get("instinct_rank", 999),
                        safe_int(row["candidate"].get("breed_count"), 999999) or 999999,
                        -(float(row["candidate"].get("ownership_percent") or 0)),
                        safe_int(row["candidate"].get("token_id"), 999999999) or 999999999,
                    )
                )

                potential_matches = scored_matches

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
    auto_open_template_id=(
        ""
        if skip_auto_open
        else (f"compare-gene-{potential_matches[0]['candidate']['token_id']}" if auto_match and potential_matches else "")
    ),
    gene_enrichment_loaded=gene_enrichment_loaded,
    gene_enrichment_remaining=gene_enrichment_remaining,
    error=error,
    )


@app.route("/match/ultimate", methods=["GET"])
def match_ultimate_page():
    wallet = request.args.get("wallet_address", "").strip().lower()
    selected_token_id = request.args.get("selected_token_id", "").strip()
    auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
    skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}

    if not require_authorized_wallet(wallet):
        return redirect(url_for("index"))

    breedable_chickens = []
    selected_chicken = None
    potential_matches = []
    error = None
    
    ultimate_sort_by = str(request.args.get("sort_by") or "ultimate_type").strip().lower()
    ultimate_sort_dir = str(request.args.get("sort_dir") or "asc").strip().lower()

    if ultimate_sort_by not in {"ultimate_type", "build", "build_match", "ip", "generation", "breed_count"}:
        ultimate_sort_by = "ultimate_type"

    if ultimate_sort_dir not in {"asc", "desc"}:
        ultimate_sort_dir = "asc"

    
    if wallet:
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)
            all_breedable = [enrich_chicken_media(row) for row in chickens if is_breedable(row)]

            breedable_chickens = [
                enrich_ultimate_display(row)
                for row in all_breedable
                if is_ultimate_eligible(row)
            ]

            sort_ultimate_available_chickens(
                breedable_chickens,
                sort_by=ultimate_sort_by,
                sort_dir=ultimate_sort_dir,
            )
            
            if selected_token_id:
                selected_chicken = next(
                    (row for row in breedable_chickens if str(row["token_id"]) == selected_token_id),
                    None,
                )

            if auto_match and not selected_token_id:
                selected_chicken, potential_matches = pick_best_ultimate_auto_match(breedable_chickens)
                if selected_chicken:
                    selected_token_id = str(selected_chicken.get("token_id") or "")

            if selected_chicken and not potential_matches:
                candidate_pool = [
                    row for row in breedable_chickens
                    if str(row["token_id"]) != selected_token_id
                    and is_generation_gap_allowed(
                        selected_chicken,
                        row,
                        max_gap=MATCH_SETTINGS["max_generation_gap"],
                    )
                ]

                potential_matches = filter_and_sort_ultimate_candidates(
                    selected_chicken,
                    candidate_pool,
                )

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
        auto_open_template_id=(
            ""
            if skip_auto_open
            else (f"compare-ultimate-{potential_matches[0]['candidate']['token_id']}" if auto_match and potential_matches else "")
        ),
        error=error,
    )


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
