from services.build_eval import (
    evaluate_build,
    count_added_missing_traits,
    build_gene_pair_metrics,
)
from services.match_rules import (
    is_generation_gap_allowed,
    is_parent_offspring,
    is_full_siblings,
)

MATCH_SETTINGS = {
    "max_generation_gap": 3,
    "max_ip_diff": 10,
    "max_breed_count_diff": 1,
}

TRAIT_SLOT_ORDER = ["beak", "comb", "eyes", "feet", "wings", "tail", "body"]

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

BUILD_INSTINCT_TIERS = {
    "killua": ["aggressive", "swift", "reckless", "elusive", "relentless", "blazing"],
    "shanks": ["steadfast", "stalwart", "resolute", "tenacious", "bulwark", "enduring"],
    "levi": ["balanced", "unyielding", "vicious", "adaptive", "versatile"],
}

GENE_PRIMARY_MIN_MATCH = 2
GENE_PRIMARY_QUALIFIED_MATCH = 5
GENE_RECESSIVE_MIN_MATCH = 4

GENE_PRIORITY_SLOTS = {
    "killua": ["beak", "tail", "feet", "body"],
    "shanks": ["beak", "wings", "tail", "feet", "body"],
    "levi": ["beak", "tail", "feet", "body"],
    "hybrid 2": ["wings"],
    "hybrid 1": [],
}

def get_gene_build_compatibility(build_type):
    build_key = str(build_type or "").strip().lower()

    compatibility = {
        "killua": {"killua", "hybrid 1", "hybrid 2"},
        "shanks": {"shanks", "hybrid 1"},
        "levi": {"levi", "hybrid 1", "hybrid 2"},
        "hybrid 1": {"killua", "shanks", "levi", "hybrid 1"},
        "hybrid 2": {"killua", "levi", "hybrid 2"},
    }

    return set(compatibility.get(build_key, {build_key} if build_key else set()))


def gene_builds_are_compatible(source_build, candidate_build):
    source_key = str(source_build or "").strip().lower()
    candidate_key = str(candidate_build or "").strip().lower()

    if not source_key or not candidate_key:
        return False

    source_compatible = get_gene_build_compatibility(source_key)
    candidate_compatible = get_gene_build_compatibility(candidate_key)

    return candidate_key in source_compatible and source_key in candidate_compatible

def get_gene_priority_slots(build_type):
    return list(GENE_PRIORITY_SLOTS.get(str(build_type or "").strip().lower(), []))


def build_gene_priority_metrics(selected_eval, candidate_eval, build_type):
    selected_eval = selected_eval or {}
    candidate_eval = candidate_eval or {}
    priority_slots = get_gene_priority_slots(build_type)

    if not priority_slots:
        return {
            "priority_slots": [],
            "selected_missing_priority_slots": [],
            "candidate_missing_priority_slots": [],
            "shared_priority_slots": [],
            "selected_resolved_priority_slots": [],
            "candidate_resolved_priority_slots": [],
            "priority_shared_count": 0,
            "selected_priority_resolved_count": 0,
            "candidate_priority_resolved_count": 0,
            "priority_covered_count": 0,
            "selected_priority_needed": False,
            "candidate_priority_needed": False,
            "selected_priority_satisfied": False,
            "candidate_priority_satisfied": False,
            "priority_any_satisfied": False,
        }

    selected_matched = set(selected_eval.get("matched_slots", []))
    selected_missing = set(selected_eval.get("missing_slots", []))
    candidate_matched = set(candidate_eval.get("matched_slots", []))
    candidate_missing = set(candidate_eval.get("missing_slots", []))
    priority_set = set(priority_slots)

    shared_priority_slots = [slot for slot in priority_slots if slot in selected_matched and slot in candidate_matched]
    selected_resolved_priority_slots = [slot for slot in priority_slots if slot in selected_missing and slot in candidate_matched]
    candidate_resolved_priority_slots = [slot for slot in priority_slots if slot in candidate_missing and slot in selected_matched]
    covered_priority_slots = [slot for slot in priority_slots if slot in (selected_matched | candidate_matched)]
    selected_missing_priority_slots = [slot for slot in priority_slots if slot in selected_missing]
    candidate_missing_priority_slots = [slot for slot in priority_slots if slot in candidate_missing]

    selected_priority_needed = bool(selected_missing_priority_slots)
    candidate_priority_needed = bool(candidate_missing_priority_slots)

    selected_priority_satisfied = (not selected_priority_needed) or bool(selected_resolved_priority_slots) or bool(shared_priority_slots)
    candidate_priority_satisfied = (not candidate_priority_needed) or bool(candidate_resolved_priority_slots) or bool(shared_priority_slots)

    return {
        "priority_slots": priority_slots,
        "selected_missing_priority_slots": selected_missing_priority_slots,
        "candidate_missing_priority_slots": candidate_missing_priority_slots,
        "shared_priority_slots": shared_priority_slots,
        "selected_resolved_priority_slots": selected_resolved_priority_slots,
        "candidate_resolved_priority_slots": candidate_resolved_priority_slots,
        "priority_shared_count": len(shared_priority_slots),
        "selected_priority_resolved_count": len(selected_resolved_priority_slots),
        "candidate_priority_resolved_count": len(candidate_resolved_priority_slots),
        "priority_covered_count": len(covered_priority_slots),
        "selected_priority_needed": selected_priority_needed,
        "candidate_priority_needed": candidate_priority_needed,
        "selected_priority_satisfied": selected_priority_satisfied,
        "candidate_priority_satisfied": candidate_priority_satisfied,
        "priority_any_satisfied": selected_priority_satisfied or candidate_priority_satisfied,
    }

def get_gene_priority_item_slots(parent, other_parent, build_type):
    build_type = str(build_type or "").strip().lower()
    priority_slots = get_gene_priority_slots(build_type)
    if not priority_slots:
        return []

    parent_eval = evaluate_build(parent, build_type)
    other_eval = evaluate_build(other_parent, build_type)

    parent_matched = set(parent_eval.get("matched_slots", []))
    other_missing = set(other_eval.get("missing_slots", []))

    return [slot for slot in priority_slots if slot in parent_matched and slot in other_missing]


def get_gene_non_priority_supply_slots(parent, other_parent, build_type):
    build_type = str(build_type or "").strip().lower()
    supplied_slots = get_build_supply_slots(parent, other_parent, build_type)
    priority_slots = set(get_gene_priority_slots(build_type))
    return [slot for slot in supplied_slots if slot not in priority_slots]

def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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
    build_source = str((chicken or {}).get("build_source_display") or "").strip().lower()
    if build_source != "primary":
        return False

    if str(build_type or "").strip().lower() not in BUILD_INSTINCT_TIERS:
        return False

    return get_instinct_tier_rank((chicken or {}).get("instinct"), build_type) <= len(
        BUILD_INSTINCT_TIERS.get(build_type, [])
    )


def get_gene_gregor_priority(parent, other_parent):
    build_source = str((parent or {}).get("build_source_display") or "").strip().lower()
    if build_source != "primary":
        return ""

    parent_count = safe_int((parent or {}).get("primary_build_match_count"), 0) or 0
    other_count = safe_int((other_parent or {}).get("primary_build_match_count"), 0) or 0
    build_type = str((parent or {}).get("build_type") or "").strip().lower()

    if parent_count >= 7:
        if other_count < 7:
            return "forced"
        return "blocked"

    if build_type:
        supplied_slots = get_build_supply_slots(parent, other_parent, build_type)
        other_eval = evaluate_build(other_parent or {}, build_type)
        other_missing_count = len(other_eval.get("missing_slots", []))

        if (
            parent_count >= 4
            and parent_count > other_count
            and other_missing_count > 0
            and len(supplied_slots) * 2 >= other_missing_count
        ):
            return "forced"

    if parent_count >= 4 and parent_count > other_count:
        return "fallback"

    return "blocked"


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
            return _normalize_gene_eval(
                {
                    "build": build_key,
                    "label": build_key.title(),
                    "match_count": row.get("recessive_build_match_count"),
                    "match_total": row.get("recessive_build_match_total"),
                    "matched_slots": row.get("recessive_build_matched_slots") or [],
                    "missing_slots": row.get("recessive_build_missing_slots") or [],
                }
            )

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


def get_build_supply_slots(parent, other_parent, build_type):
    if not build_type:
        return []

    parent_eval = evaluate_build(parent, build_type)
    other_eval = evaluate_build(other_parent, build_type)

    supplied = set(other_eval.get("missing_slots", [])) & set(parent_eval.get("matched_slots", []))
    return [slot for slot in TRAIT_SLOT_ORDER if slot in supplied]


def recommend_gene_item(parent, other_parent, build_type):
    build_source = str((parent or {}).get("build_source_display") or "").strip().lower()

    if build_source == "recessive":
        return {
            "name": "Mendel's Memento",
            "reason": "Best when this parent is being valued for recessive build inheritance.",
        }

    gregor_priority = get_gene_gregor_priority(parent, other_parent)
    if gregor_priority == "forced":
        return {
            "name": "Gregor's Gift",
            "reason": "Best when this parent is preserving the stronger primary build influence for the pair.",
        }

    priority_slots = get_gene_priority_item_slots(parent, other_parent, build_type)
    if priority_slots:
        item_name, reason = TRAIT_ITEM_RULES[priority_slots[0]]
        return {
            "name": item_name,
            "reason": reason,
        }

    supplied_slots = get_gene_non_priority_supply_slots(parent, other_parent, build_type)
    if supplied_slots:
        item_name, reason = TRAIT_ITEM_RULES[supplied_slots[0]]
        return {
            "name": item_name,
            "reason": reason,
        }

    instinct_tiers = BUILD_INSTINCT_TIERS.get(str(build_type or "").strip().lower(), [])
    instinct_rank = get_instinct_tier_rank((parent or {}).get("instinct"), build_type)
    if instinct_tiers and instinct_rank <= len(instinct_tiers):
        return {
            "name": "St. Elmo's Fire",
            "reason": "Best when no trait edge is available and this parent has a strong instinct fit for the target build.",
        }

    if gregor_priority == "fallback":
        return {
            "name": "Gregor's Gift",
            "reason": "Best when this parent is contributing the stronger primary build in the pair.",
        }

    return None

def get_gene_item_candidates(parent, other_parent, build_type):
    candidates = []

    build_source = str((parent or {}).get("build_source_display") or "").strip().lower()
    if build_source == "recessive":
        candidates.append(
            {
                "name": "Mendel's Memento",
                "reason": "Best when this parent is being valued for recessive build inheritance.",
            }
        )
        return candidates

    gregor_priority = get_gene_gregor_priority(parent, other_parent)
    if gregor_priority == "forced":
        candidates.append(
            {
                "name": "Gregor's Gift",
                "reason": "Best when this parent is preserving the stronger primary build influence for the pair.",
            }
        )
        return candidates

    priority_slots = get_gene_priority_item_slots(parent, other_parent, build_type)
    for slot in priority_slots:
        item_name, reason = TRAIT_ITEM_RULES[slot]
        candidates.append(
            {
                "name": item_name,
                "reason": reason,
            }
        )

    supplied_slots = get_gene_non_priority_supply_slots(parent, other_parent, build_type)
    for slot in supplied_slots:
        item_name, reason = TRAIT_ITEM_RULES[slot]
        candidates.append(
            {
                "name": item_name,
                "reason": reason,
            }
        )

    instinct_tiers = BUILD_INSTINCT_TIERS.get(str(build_type or "").strip().lower(), [])
    instinct_rank = get_instinct_tier_rank((parent or {}).get("instinct"), build_type)
    if instinct_tiers and instinct_rank <= len(instinct_tiers):
        candidates.append(
            {
                "name": "St. Elmo's Fire",
                "reason": "Best when no trait edge is available and this parent has a strong instinct fit for the target build.",
            }
        )

    if gregor_priority == "fallback":
        candidates.append(
            {
                "name": "Gregor's Gift",
                "reason": "Best when this parent is contributing the stronger primary build in the pair.",
            }
        )

    return candidates


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


def get_gene_pair_completion(selected_eval, candidate_eval):
    metrics = build_gene_pair_metrics(selected_eval, candidate_eval)
    return {
        "combined_count": metrics["combined_count"],
        "combined_total": metrics["total"],
        "selected_count": metrics["left_count"],
        "candidate_count": metrics["right_count"],
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
        left_target_info = get_gene_build_target_info(left, build_type)
        right_target_info = get_gene_build_target_info(right, build_type)
        return get_gene_pair_completion(
            left_target_info.get("effective_eval") or evaluate_build(left, build_type),
            right_target_info.get("effective_eval") or evaluate_build(right, build_type),
        )

    return {
        "combined_count": 0,
        "combined_total": 0,
        "selected_count": 0,
        "candidate_count": 0,
    }


def build_gene_pair_quality(row):
    row = row or {}
    selected_eval = row.get("selected_eval") or {}
    candidate_eval = row.get("candidate_eval") or {}
    build_type = str(row.get("build_type") or "").strip().lower()

    if not selected_eval or not candidate_eval:
        metrics = get_gene_pair_completion_from_row(row)
        combined_count = metrics["combined_count"]
        combined_total = metrics["combined_total"]
        selected_count = metrics["selected_count"]
        candidate_count = metrics["candidate_count"]

        pair_metrics = {
            "shared_count": max(0, min(selected_count, candidate_count) - max(0, combined_count - max(selected_count, candidate_count))),
            "combined_count": combined_count,
            "edge_count": max(0, combined_count - max(selected_count, candidate_count)),
            "elite_stabilization": False,
            "anchor_finisher": False,
        }

        priority_metrics = {
            "selected_priority_satisfied": True,
            "selected_priority_resolved_count": 0,
            "priority_shared_count": 0,
        }

        added_missing_traits = safe_int(row.get("added_missing_traits"), 0) or 0
        candidate_target_info = {
            "sort_match_count": candidate_count,
        }
    else:
        pair_metrics = build_gene_pair_metrics(selected_eval, candidate_eval)
        priority_metrics = build_gene_priority_metrics(selected_eval, candidate_eval, build_type)
        combined_count = safe_int(pair_metrics.get("combined_count"), 0) or 0
        combined_total = safe_int(pair_metrics.get("total"), 0) or 0
        added_missing_traits = safe_int(row.get("added_missing_traits"), 0) or 0
        candidate_target_info = row.get("candidate_target_info") or {
            "sort_match_count": safe_int((candidate_eval or {}).get("match_count"), 0) or 0,
        }

    if combined_total <= 0:
        return "Poor"

    selected_count = safe_int((selected_eval or {}).get("match_count"), 0)
    if selected_count is None:
        selected_count = metrics["selected_count"] if not selected_eval or not candidate_eval else 0

    candidate_count = safe_int((candidate_eval or {}).get("match_count"), 0)
    if candidate_count is None:
        candidate_count = metrics["candidate_count"] if not selected_eval or not candidate_eval else 0

    selected_count = selected_count or 0
    candidate_count = candidate_count or 0

    shared_count = safe_int(pair_metrics.get("shared_count"), 0) or 0
    edge_count = safe_int(pair_metrics.get("edge_count"), 0) or 0

    left_finishes = combined_count > selected_count
    right_finishes = combined_count > candidate_count
    both_finish = left_finishes and right_finishes
    both_complete = selected_count >= combined_total and candidate_count >= combined_total
    one_complete_one_near = (
        max(selected_count, candidate_count) >= combined_total
        and min(selected_count, candidate_count) >= max(0, combined_total - 1)
    )
    pure_fill = added_missing_traits >= 1 and shared_count == 0

    if both_complete:
        return "Excellent match"

    if both_finish and combined_count >= combined_total and shared_count >= max(3, combined_total - 2):
        return "Excellent match"

    if one_complete_one_near and combined_count >= combined_total and shared_count >= max(3, combined_total - 2):
        return "Excellent match"

    if (
        combined_count >= max(4, combined_total - 1)
        and shared_count >= max(2, combined_total - 3)
        and (both_finish or added_missing_traits >= 2)
    ):
        return "Strong match"

    if pure_fill:
        return "Situational"

    if shared_count <= 1 and added_missing_traits <= 1:
        return "Poor"

    if (
        (shared_count in {1, 2} and added_missing_traits >= 2)
        or (shared_count >= 3 and added_missing_traits == 0 and not both_complete)
        or (shared_count >= 2 and added_missing_traits >= 1)
    ):
        return "Good match"

    if shared_count == 0 and added_missing_traits >= 1:
        return "Situational"

    return "Poor"


def build_gene_pair_quality_from_score(
    selected_chicken,
    candidate,
    build_type,
    selected_eval=None,
    candidate_eval=None,
    pair_metrics=None,
    candidate_target_info=None,
    priority_metrics=None,
    added_missing_traits=0,
):
    row = {
        "left": selected_chicken or {},
        "candidate": candidate or {},
        "build_type": build_type,
        "selected_eval": selected_eval or {},
        "candidate_eval": candidate_eval or {},
        "candidate_target_info": candidate_target_info or {},
        "added_missing_traits": added_missing_traits,
    }
    return build_gene_pair_quality(row)

def get_gene_item_score_bonus(item):
    item_name = str((item or {}).get("name") or "").strip()

    if not item_name:
        return 0

    if item_name == "Gregor's Gift":
        return 18

    if item_name == "Mendel's Memento":
        return 10

    if item_name == "St. Elmo's Fire":
        return 8

    if item_name in {
        "Chim Lac's Curio",
        "Simurgh's Sovereign",
        "Dragon's Whip",
        "Quentin's Talon",
        "Chibidei's Curse",
    }:
        return 12

    if item_name in {
        "Suave Scissors",
        "All-seeing Seed",
    }:
        return 6

    return 0


def get_gene_overlap_penalty(shared_count):
    shared_count = safe_int(shared_count, 0) or 0

    if shared_count <= 0:
        return 220
    if shared_count == 1:
        return 90
    if shared_count == 2:
        return 25
    return 0


def compute_gene_pair_score(
    selected_chicken,
    candidate,
    build_type,
    pair_metrics=None,
    candidate_target_info=None,
    priority_metrics=None,
    added_missing_traits=0,
):
    selected_chicken = selected_chicken or {}
    candidate = candidate or {}
    pair_metrics = pair_metrics or {}
    candidate_target_info = candidate_target_info or {}
    priority_metrics = priority_metrics or {}
    added_missing_traits = safe_int(added_missing_traits, 0) or 0

    combined_count = safe_int(pair_metrics.get("combined_count"), 0) or 0
    shared_count = safe_int(pair_metrics.get("shared_count"), 0) or 0
    candidate_match_count = safe_int(candidate_target_info.get("sort_match_count"), 0) or 0

    priority_bonus = 0
    if priority_metrics.get("selected_priority_resolved_count", 0):
        priority_bonus += 20
    elif priority_metrics.get("selected_priority_satisfied"):
        priority_bonus += 10

    if priority_metrics.get("priority_shared_count", 0):
        priority_bonus += 6

    left_item_candidates = get_gene_item_candidates(selected_chicken, candidate, build_type)
    right_item_candidates = get_gene_item_candidates(candidate, selected_chicken, build_type)
    left_item, right_item = resolve_pair_item_recommendations(left_item_candidates, right_item_candidates)

    item_bonus = max(
        get_gene_item_score_bonus(left_item),
        get_gene_item_score_bonus(right_item),
    )

    overlap_penalty = get_gene_overlap_penalty(shared_count)

    score = 0
    score += combined_count * 100
    score += shared_count * 20
    score += added_missing_traits * 15
    score += candidate_match_count * 5
    score += priority_bonus
    score += item_bonus
    score -= overlap_penalty

    return score

def rank_gene_pair(
    selected_chicken,
    candidate,
    build_type,
    pair_metrics=None,
    candidate_target_info=None,
    instinct_rank=999,
    priority_metrics=None,
    added_missing_traits=0,
):
    candidate = candidate or {}
    selected_chicken = selected_chicken or {}
    pair_metrics = pair_metrics or {}
    candidate_target_info = candidate_target_info or {}
    priority_metrics = priority_metrics or {}
    added_missing_traits = safe_int(added_missing_traits, 0) or 0

    gene_score = compute_gene_pair_score(
        selected_chicken=selected_chicken,
        candidate=candidate,
        build_type=build_type,
        pair_metrics=pair_metrics,
        candidate_target_info=candidate_target_info,
        priority_metrics=priority_metrics,
        added_missing_traits=added_missing_traits,
    )

    quality_rank = {
        "Excellent match": 0,
        "Strong match": 1,
        "Good match": 2,
        "Situational": 3,
        "Poor": 4,
    }.get(
        build_gene_pair_quality_from_score(
            selected_chicken=selected_chicken,
            candidate=candidate,
            build_type=build_type,
            pair_metrics=pair_metrics,
            candidate_target_info=candidate_target_info,
            priority_metrics=priority_metrics,
            added_missing_traits=added_missing_traits,
        ),
        99,
    )

    return (
        quality_rank,
        -gene_score,
        -(pair_metrics.get("combined_count") or 0),
        -(pair_metrics.get("shared_count") or 0),
        -(added_missing_traits or 0),
        -(candidate_target_info.get("sort_match_count") or 0),
        -int(bool(priority_metrics.get("selected_priority_satisfied"))),
        -(priority_metrics.get("selected_priority_resolved_count") or 0),
        -(priority_metrics.get("priority_shared_count") or 0),
        candidate_target_info.get("sort_source_rank", 9),
        instinct_rank,
        safe_int(candidate.get("breed_count"), 999999) or 999999,
        -(float(candidate.get("ownership_percent") or 0)),
        safe_int(candidate.get("token_id"), 999999999) or 999999999,
        safe_int(selected_chicken.get("breed_count"), 999999) or 999999,
        -(float(selected_chicken.get("ownership_percent") or 0)),
        safe_int(selected_chicken.get("token_id"), 999999999) or 999999999,
    )

def build_gene_potential_matches(selected_chicken, breedable_chickens, build_type):
    if not selected_chicken or not build_type:
        return []

    selected_token_id = str((selected_chicken or {}).get("token_id") or "").strip()
    selected_eval = get_gene_build_target_info(selected_chicken, build_type)["effective_eval"]
    selected_build_type = str((selected_chicken or {}).get("build_type") or "").strip().lower()

    candidate_pool = [
        row
        for row in (breedable_chickens or [])
        if str(row.get("token_id") or "").strip() != selected_token_id
        and gene_builds_are_compatible(
            selected_build_type,
            str(row.get("build_type") or "").strip().lower(),
        )
        and not is_parent_offspring(selected_chicken, row)
        and not is_full_siblings(selected_chicken, row)
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
        completion = get_gene_pair_completion(selected_eval, candidate_eval)
        pair_metrics = build_gene_pair_metrics(selected_eval, candidate_eval)
        priority_metrics = build_gene_priority_metrics(selected_eval, candidate_eval, build_type)
        added_missing_traits = count_added_missing_traits(selected_eval, candidate_eval)

        instinct_rank = (
            get_instinct_tier_rank(candidate.get("instinct"), build_type)
            if candidate_source == "primary"
            else 999
        )

        scored_matches.append(
            {
                "candidate": candidate,
                "candidate_eval": candidate_eval,
                "candidate_target_info": candidate_target_info,
                "selected_eval": selected_eval,
                "build_type": build_type,
                "added_missing_traits": added_missing_traits,
                "combined_match_count": completion["combined_count"],
                "combined_match_total": completion["combined_total"],
                "selected_build_match_count": completion["selected_count"],
                "candidate_build_match_count": completion["candidate_count"],
                "gene_pair_metrics": pair_metrics,
                "gene_priority_metrics": priority_metrics,
                "instinct_rank": instinct_rank,
                "ranking": rank_gene_pair(
                    selected_chicken=selected_chicken,
                    candidate=candidate,
                    build_type=build_type,
                    pair_metrics=pair_metrics,
                    candidate_target_info=candidate_target_info,
                    instinct_rank=instinct_rank,
                    priority_metrics=priority_metrics,
                    added_missing_traits=added_missing_traits,
                ),
            }
        )

    scored_matches.sort(key=lambda row: row["ranking"])
    return scored_matches


def build_gene_potential_matches_strict(selected_chicken, breedable_chickens):
    if not selected_chicken:
        return []

    build_type = str((selected_chicken or {}).get("build_type") or "").strip().lower()
    if not build_type:
        return []

    return build_gene_potential_matches(selected_chicken, breedable_chickens, build_type)


def normalize_auto_ninuno_filter(value):
    value = str(value or "all").strip().lower()
    if value in {"100", "100%", "100_only", "complete"}:
        return "100"
    if value in {"gt0", ">0", "not0", "above0", "positive"}:
        return "gt0"
    return "all"


def chicken_passes_auto_ninuno_filter(chicken, mode):
    mode = normalize_auto_ninuno_filter(mode)
    ownership = float((chicken or {}).get("ownership_percent") or 0)

    if mode == "100":
        return bool((chicken or {}).get("is_complete")) and ownership == 100.0
    if mode == "gt0":
        return ownership > 0
    return True


def build_gene_available_auto_candidates_same_build(
    breedable_chickens,
    min_build_count=None,
    breed_diff=None,
    same_instinct=False,
    ninuno_mode="all",
):
    pair_rows = []

    for index, source in enumerate(breedable_chickens or []):
        source_build = str(source.get("build_type") or "").strip().lower()
        if not source_build:
            continue

        if not chicken_passes_auto_ninuno_filter(source, ninuno_mode):
            continue

        if min_build_count is not None and safe_int(source.get("build_match_count"), 0) < min_build_count:
            continue

        for candidate in (breedable_chickens or [])[index + 1:]:
            candidate_build = str(candidate.get("build_type") or "").strip().lower()
            if not gene_builds_are_compatible(source_build, candidate_build):
                continue

            if not chicken_passes_auto_ninuno_filter(candidate, ninuno_mode):
                continue

            if min_build_count is not None and safe_int(candidate.get("build_match_count"), 0) < min_build_count:
                continue

            if breed_diff is not None:
                source_breed = safe_int(source.get("breed_count"))
                candidate_breed = safe_int(candidate.get("breed_count"))
                if source_breed is None or candidate_breed is None or abs(candidate_breed - source_breed) > breed_diff:
                    continue

            if same_instinct and normalize_instinct_name(source.get("instinct")) != normalize_instinct_name(
                candidate.get("instinct")
            ):
                continue

            if is_parent_offspring(source, candidate):
                continue

            if is_full_siblings(source, candidate):
                continue

            if not is_generation_gap_allowed(source, candidate, max_gap=MATCH_SETTINGS["max_generation_gap"]):
                continue

            forward = build_gene_potential_matches(source, [source, candidate], source_build)
            reverse = build_gene_potential_matches(candidate, [source, candidate], source_build)

            if forward and reverse:
                forward_rank = tuple(forward[0].get("ranking") or ())
                reverse_rank = tuple(reverse[0].get("ranking") or ())

                if forward_rank <= reverse_rank:
                    chosen_left = dict(source)
                    chosen_right = dict(candidate)
                    chosen_match = forward[0]
                    chosen_build = str(chosen_match.get("build_type") or source_build).strip().lower()
                else:
                    chosen_left = dict(candidate)
                    chosen_right = dict(source)
                    chosen_match = reverse[0]
                    chosen_build = str(chosen_match.get("build_type") or candidate_build).strip().lower()
            elif forward:
                chosen_left = dict(source)
                chosen_right = dict(candidate)
                chosen_match = forward[0]
                chosen_build = str(chosen_match.get("build_type") or source_build).strip().lower()
            elif reverse:
                chosen_left = dict(candidate)
                chosen_right = dict(source)
                chosen_match = reverse[0]
                chosen_build = str(chosen_match.get("build_type") or candidate_build).strip().lower()
            else:
                continue

            left_item_candidates = get_gene_item_candidates(chosen_left, chosen_right, chosen_build)
            right_item_candidates = get_gene_item_candidates(chosen_right, chosen_left, chosen_build)
            left_item, right_item = resolve_pair_item_recommendations(
                left_item_candidates,
                right_item_candidates,
            )

            pair_metrics = build_gene_pair_metrics(
                chosen_match.get("selected_eval"),
                chosen_match.get("candidate_eval"),
            )

            priority_metrics = build_gene_priority_metrics(
                chosen_match.get("selected_eval"),
                chosen_match.get("candidate_eval"),
                chosen_build,
            )

            pair_rows.append(
                {
                    "left": chosen_left,
                    "right": chosen_right,
                    "left_item": left_item,
                    "right_item": right_item,
                    "build_type": chosen_build,
                    "selected_eval": chosen_match.get("selected_eval"),
                    "candidate_eval": chosen_match.get("candidate_eval"),
                    "combined_match_count": chosen_match.get("combined_match_count", 0),
                    "combined_match_total": chosen_match.get("combined_match_total", 0),
                    "selected_build_match_count": chosen_match.get("selected_build_match_count", 0),
                    "candidate_build_match_count": chosen_match.get("candidate_build_match_count", 0),
                    "same_instinct": normalize_instinct_name(chosen_left.get("instinct"))
                    == normalize_instinct_name(chosen_right.get("instinct")),
                    "added_missing_traits": chosen_match.get("added_missing_traits") or 0,
                    "gene_pair_metrics": pair_metrics,
                    "gene_priority_metrics": priority_metrics,
                    "ranking": rank_gene_pair(
                        selected_chicken=chosen_left,
                        candidate=chosen_right,
                        build_type=chosen_build,
                        pair_metrics=pair_metrics,
                        candidate_target_info=chosen_match.get("candidate_target_info") or {},
                        instinct_rank=chosen_match.get("instinct_rank", 999),
                        priority_metrics=priority_metrics,
                        added_missing_traits=chosen_match.get("added_missing_traits") or 0,
                    ),
                }
            )

    pair_rows.sort(key=lambda row: row["ranking"])
    return pair_rows


def sort_gene_match_rows(selected_chicken, match_rows):
    rows = list(match_rows or [])

    def sort_key(row):
        candidate = row.get("candidate") or {}
        pair_metrics = row.get("gene_pair_metrics") or {}
        priority_metrics = row.get("gene_priority_metrics") or {}
        candidate_target_info = row.get("candidate_target_info") or {}
        build_type = str(row.get("build_type") or "").strip().lower()

        return rank_gene_pair(
            selected_chicken=selected_chicken,
            candidate=candidate,
            build_type=build_type,
            pair_metrics=pair_metrics,
            candidate_target_info=candidate_target_info,
            instinct_rank=row.get("instinct_rank", 999),
            priority_metrics=priority_metrics,
            added_missing_traits=row.get("added_missing_traits") or 0,
        )

    rows.sort(key=sort_key)
    return rows

def pick_best_gene_auto_match(breedable_chickens, build_type):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in breedable_chickens or []:
        matches = build_gene_potential_matches(selected, breedable_chickens, build_type)
        if not matches:
            continue

        top = matches[0]
        ranking = top.get("ranking") or rank_gene_pair(
            selected_chicken=selected,
            candidate=top.get("candidate") or {},
            build_type=build_type,
            pair_metrics=top.get("gene_pair_metrics") or {},
            priority_metrics=top.get("gene_priority_metrics") or {},
            candidate_target_info=top.get("candidate_target_info") or {},
            instinct_rank=top.get("instinct_rank", 999),
            added_missing_traits=top.get("added_missing_traits") or 0,
        )

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches


def pick_best_gene_auto_match_from_pool(
    breedable_chickens,
    popup_build="all",
    popup_min_build_count=None,
    popup_breed_diff=None,
    popup_ninuno="all",
):
    build_order = ["killua", "shanks", "levi", "hybrid 2", "hybrid 1"]

    pool = list(breedable_chickens or [])

    if popup_min_build_count is not None:
        pool = [
            row for row in pool
            if safe_int(row.get("build_match_count"), 0) >= popup_min_build_count
        ]

    pool = [
        row for row in pool
        if chicken_passes_auto_ninuno_filter(row, popup_ninuno)
    ]

    if popup_build and popup_build != "all":
        build_order = [popup_build]

    best_selected = None
    best_matches = []
    best_top = None

    for build_type in build_order:
        build_pool = [
            row for row in pool
            if gene_builds_are_compatible(
                build_type,
                str(row.get("build_type") or "").strip().lower(),
            )
        ]

        if len(build_pool) < 2:
            continue

        if popup_breed_diff is not None:
            filtered_pool = []
            for source in build_pool:
                source_breed = safe_int(source.get("breed_count"))
                if source_breed is None:
                    continue

                has_valid_partner = any(
                    str(candidate.get("token_id") or "") != str(source.get("token_id") or "")
                    and safe_int(candidate.get("breed_count")) is not None
                    and abs((safe_int(candidate.get("breed_count")) or 0) - source_breed) <= popup_breed_diff
                    for candidate in build_pool
                )

                if has_valid_partner:
                    filtered_pool.append(source)

            build_pool = filtered_pool

        if len(build_pool) < 2:
            continue

        selected, matches = pick_best_gene_auto_match(build_pool, build_type)
        if not selected or not matches:
            continue

        top = matches[0]
        ranking = tuple(top.get("ranking") or ())

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches
