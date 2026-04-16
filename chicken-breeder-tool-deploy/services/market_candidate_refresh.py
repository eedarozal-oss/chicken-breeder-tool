from datetime import datetime, timezone

from services.build_eval import evaluate_all_builds
from services.db.connection import get_connection
from services.market_candidate_cache import (
    delete_market_candidate_cache_row,
    get_market_candidate_cache_row,
    upsert_market_candidate_cache_row,
    MARKET_CANDIDATE_CACHE_VERSION,
)

MAIN_BUILDS = ("killua", "shanks", "levi")


def safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def safe_lower(value):
    return str(value or "").strip().lower()


def build_primary_traits(row):
    row = dict(row or {})
    return {
        "beak": row.get("beak"),
        "comb": row.get("comb"),
        "eyes": row.get("eyes"),
        "feet": row.get("feet"),
        "wings": row.get("wings"),
        "tail": row.get("tail"),
        "body": row.get("body"),
    }


def build_gene_traits(row):
    row = dict(row or {})

    def choose_gene_value(slot_name):
        for key in (f"{slot_name}_h1", f"{slot_name}_h2", f"{slot_name}_h3"):
            value = str(row.get(key) or "").strip()
            if value:
                return value
        return ""

    return {
        "beak": choose_gene_value("beak"),
        "comb": choose_gene_value("comb"),
        "eyes": choose_gene_value("eyes"),
        "feet": choose_gene_value("feet"),
        "wings": choose_gene_value("wings"),
        "tail": choose_gene_value("tail"),
        "body": choose_gene_value("body"),
    }


def compute_total_ip(row):
    row = dict(row or {})
    return sum(
        [
            safe_int(row.get("innate_attack")),
            safe_int(row.get("innate_defense")),
            safe_int(row.get("innate_speed")),
            safe_int(row.get("innate_health")),
            safe_int(row.get("innate_ferocity")),
            safe_int(row.get("innate_cockrage")),
            safe_int(row.get("innate_evasion")),
        ]
    )


def choose_best_main_build(evaluations):
    evaluations = dict(evaluations or {})
    best = None

    for build_name in MAIN_BUILDS:
        result = evaluations.get(build_name) or {}
        match_count = safe_int(result.get("match_count"))
        match_total = safe_int(result.get("match_total"))

        candidate = {
            "build_name": build_name,
            "match_count": match_count,
            "match_total": match_total,
        }

        if best is None:
            best = candidate
            continue

        if candidate["match_count"] > best["match_count"]:
            best = candidate
            continue

        if candidate["match_count"] == best["match_count"]:
            if candidate["match_total"] > best["match_total"]:
                best = candidate
                continue

    if not best:
        return {
            "build_name": "",
            "match_count": 0,
            "match_total": 0,
        }

    return best


def compute_market_candidate_row(static_row):
    static_row = dict(static_row or {})
    token_id = str(static_row.get("token_id") or "").strip()
    source_updated_at = str(static_row.get("updated_at") or "").strip()
    image = str(static_row.get("image") or "").strip()

    total_ip = compute_total_ip(static_row)
    breed_count = safe_int(static_row.get("breed_count"), 0)

    primary_traits = build_primary_traits(static_row)
    gene_traits = build_gene_traits(static_row)

    primary_evaluations = evaluate_all_builds(primary_traits)
    gene_evaluations = evaluate_all_builds(gene_traits)

    best_primary = choose_best_main_build(primary_evaluations)
    best_gene = choose_best_main_build(gene_evaluations)

    if best_primary["match_count"] >= best_gene["match_count"]:
        best_build = best_primary
    else:
        best_build = best_gene

    best_build_name = safe_lower(best_build.get("build_name"))
    best_build_count = safe_int(best_build.get("match_count"))
    best_build_total = safe_int(best_build.get("match_total"))

    qualifies_ip = 1 if total_ip >= 175 else 0
    qualifies_gene = 1 if best_build_count >= 3 else 0
    qualifies_ultimate = 1 if total_ip >= 175 and best_build_count >= 3 else 0
    
    return {
        "token_id": token_id,
        "image": image,
        "breed_count": breed_count,
        "total_ip": total_ip,
        "best_build_name": best_build_name,
        "best_build_count": best_build_count,
        "best_build_total": best_build_total,
        "qualifies_ip": qualifies_ip,
        "qualifies_gene": qualifies_gene,
        "qualifies_ultimate": qualifies_ultimate,
        "source_updated_at": source_updated_at,
        "cache_version": MARKET_CANDIDATE_CACHE_VERSION,
    }


def should_refresh_market_candidate(static_row, cache_row):
    static_row = dict(static_row or {})
    cache_row = dict(cache_row or {})

    if not cache_row:
        return True

    static_updated_at = str(static_row.get("updated_at") or "").strip()
    cached_source_updated_at = str(cache_row.get("source_updated_at") or "").strip()
    cached_version = safe_int(cache_row.get("cache_version"), 0)

    if cached_version != MARKET_CANDIDATE_CACHE_VERSION:
        return True

    return static_updated_at != cached_source_updated_at


def get_changed_market_candidate_source_rows():
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                s.token_id,
                s.image,
                s.generation_text,
                s.generation_num,
                s.parent_1,
                s.parent_2,
                s.breed_count,
                s.gender,
                s.type,
                s.instinct,
                s.innate_attack,
                s.innate_defense,
                s.innate_speed,
                s.innate_health,
                s.innate_ferocity,
                s.innate_cockrage,
                s.innate_evasion,
                s.beak,
                s.comb,
                s.eyes,
                s.feet,
                s.wings,
                s.tail,
                s.body,
                s.beak_h1,
                s.beak_h2,
                s.beak_h3,
                s.comb_h1,
                s.comb_h2,
                s.comb_h3,
                s.eyes_h1,
                s.eyes_h2,
                s.eyes_h3,
                s.feet_h1,
                s.feet_h2,
                s.feet_h3,
                s.wings_h1,
                s.wings_h2,
                s.wings_h3,
                s.tail_h1,
                s.tail_h2,
                s.tail_h3,
                s.body_h1,
                s.body_h2,
                s.body_h3,
                s.is_dead,
                s.gene_profile_loaded,
                s.is_egg,
                s.updated_at
            FROM chicken_static s
            LEFT JOIN chicken_market_candidates c
                ON c.token_id = s.token_id
            WHERE COALESCE(s.is_dead, 0) = 0
              AND COALESCE(s.is_egg, 0) = 0
              AND COALESCE(s.gene_profile_loaded, 0) = 1
              AND (
                    c.token_id IS NULL
                    OR COALESCE(c.source_updated_at, '') != COALESCE(s.updated_at, '')
                    OR COALESCE(c.cache_version, 0) != ?
                  )
            ORDER BY
                CASE
                    WHEN s.updated_at IS NULL OR TRIM(s.updated_at) = '' THEN 1
                    ELSE 0
                END,
                s.updated_at ASC,
                CAST(s.token_id AS INTEGER) ASC
            """,
            (MARKET_CANDIDATE_CACHE_VERSION,),
        ).fetchall()

    return [dict(row) for row in rows]


def refresh_market_candidate_cache():
    changed_rows = get_changed_market_candidate_source_rows()

    processed = 0
    upserted = 0
    deleted = 0

    for static_row in changed_rows:
        processed += 1
        candidate_row = compute_market_candidate_row(static_row)

        qualifies_any = any(
            [
                candidate_row["qualifies_ip"],
                candidate_row["qualifies_gene"],
                candidate_row["qualifies_ultimate"],
            ]
        )

        if qualifies_any:
            upsert_market_candidate_cache_row(candidate_row)
            upserted += 1
        else:
            delete_market_candidate_cache_row(candidate_row["token_id"])
            deleted += 1

    return {
        "processed": processed,
        "upserted": upserted,
        "deleted": deleted,
    }
