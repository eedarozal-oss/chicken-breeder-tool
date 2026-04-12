from services.primary_build_classifier import safe_int
from services.build_eval import (
    evaluate_build,
    evaluate_all_builds,
    select_qualified_build,
    count_added_missing_traits,
)
from services.item_helper_text import get_item_helper_text, normalize_item_name

ULTIMATE_IP_STRONG_THRESHOLD = 265
ULTIMATE_IP_ENTRY_THRESHOLD = 175
ULTIMATE_BUILD_ENTRY_THRESHOLD = 5
ULTIMATE_BUILD_PARTIAL_THRESHOLD = 3

IP_STAT_PRIORITY = [
    "attack",
    "defense",
    "hp",
    "speed",
    "evasion",
    "ferocity",
    "cockrage",
]

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

ULTIMATE_INNATE_ITEM_BY_STAT = {
    "attack": "Cocktail's Beak",
    "defense": "Pos2 Pellet",
    "hp": "Vananderen's Vitality",
    "speed": "Fetzzz Feet",
    "evasion": "Lockedin State",
    "ferocity": "Ouchie's Ornament",
    "cockrage": "Pinong's Bird",
}

ULTIMATE_TRAIT_ITEM_BY_SLOT = {
    "beak": "Chim Lac's Curio",
    "comb": "Suave Scissors",
    "eyes": "All-seeing Seed",
    "feet": "Quentin's Talon",
    "wings": "Simurgh's Sovereign",
    "tail": "Dragon's Whip",
    "body": "Chibidei's Curse",
}

ULTIMATE_ITEM_PRIORITY_ORDER = [
    ("attack", "beak"),
    ("defense", "wings"),
    ("hp", "body"),
    ("speed", "feet"),
    ("evasion", "tail"),
    ("ferocity", "eyes"),
    ("cockrage", "comb"),
]

ULTIMATE_TYPE_ORDER = ["both", "gene_only", "ip_only"]

def needs_ultimate_primary_build_refresh(chicken, safe_int_fn):
    if not chicken:
        return False

    is_egg = bool(chicken.get("is_egg"))
    state = str(chicken.get("state") or "").strip().lower()

    if is_egg or state != "normal":
        return False

    primary_build = str(chicken.get("primary_build") or "").strip().lower()
    primary_count = safe_int_fn(chicken.get("primary_build_match_count"))
    primary_total = safe_int_fn(chicken.get("primary_build_match_total"))

    if not primary_build:
        return True

    if primary_count is None or primary_total is None:
        return True

    if primary_total <= 0:
        return True

    if primary_count < 3:
        return True

    return False


def refresh_ultimate_primary_builds_if_needed(chickens, upsert_chicken_fn, safe_int_fn):
    updated_any = False

    for chicken in chickens or []:
        if not needs_ultimate_primary_build_refresh(chicken, safe_int_fn):
            continue

        refreshed = dict(chicken)

        evaluations = evaluate_all_builds(refreshed)
        selected = select_qualified_build(evaluations, min_matches=3)

        if selected:
            refreshed["primary_build"] = selected.get("build") or ""
            refreshed["primary_build_match_count"] = selected.get("match_count") or 0
            refreshed["primary_build_match_total"] = selected.get("match_total") or 0
        else:
            refreshed["primary_build"] = ""
            refreshed["primary_build_match_count"] = 0
            refreshed["primary_build_match_total"] = 0

        upsert_chicken_fn(refreshed)
        updated_any = True

    return updated_any

def get_primary_build(chicken):
    return str((chicken or {}).get("primary_build") or "").strip().lower()


def get_primary_build_count(chicken):
    return safe_int((chicken or {}).get("primary_build_match_count"), default=0) or 0


def get_primary_build_total(chicken):
    return safe_int((chicken or {}).get("primary_build_match_total"), default=0) or 0


def get_effective_ip_stat(chicken, stat_name):
    value_field = IP_STAT_VALUE_FIELDS[stat_name]
    raw_value = (chicken or {}).get(value_field)

    if raw_value not in (None, ""):
        parsed = safe_int(raw_value, default=0)
        return parsed or 0

    fallback_field = IP_STAT_FALLBACK_FIELDS.get(stat_name)
    if fallback_field:
        parsed = safe_int((chicken or {}).get(fallback_field), default=0)
        return parsed or 0

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

    weakest_name = ""
    weakest_value = None

    for stat_name in IP_STAT_PRIORITY:
        stat_value = get_effective_ip_stat(chicken, stat_name)
        if weakest_value is None or stat_value < weakest_value:
            weakest_name = stat_name
            weakest_value = stat_value

    if not weakest_name:
        return {
            "name": "",
            "label": "",
            "value": 0,
            "display": "",
        }

    return {
        "name": weakest_name,
        "label": stat_labels[weakest_name],
        "value": weakest_value or 0,
        "display": f"{stat_labels[weakest_name]}: {weakest_value or 0}",
    }


def has_high_ip(chicken):
    return (safe_int((chicken or {}).get("ip"), default=0) or 0) > 264


def has_entry_ip(chicken):
    return (safe_int((chicken or {}).get("ip"), default=0) or 0) > 174


def has_strong_build_count(chicken):
    return get_primary_build_count(chicken) >= ULTIMATE_BUILD_ENTRY_THRESHOLD


def has_partial_build_count(chicken):
    return get_primary_build_count(chicken) > 2


def get_same_stat_supports(source, target):
    supports = []

    for stat_name in IP_STAT_PRIORITY:
        source_value = get_effective_ip_stat(source, stat_name)
        target_value = get_effective_ip_stat(target, stat_name)

        if source_value > 24 and target_value < 25:
            supports.append({
                "stat": stat_name,
                "source_value": source_value,
                "target_value": target_value,
                "gap": source_value - target_value,
            })

    supports.sort(
        key=lambda row: (
            -(row["gap"] or 0),
            -(row["source_value"] or 0),
            IP_STAT_PRIORITY.index(row["stat"]),
        )
    )
    return supports


def count_same_stat_advantages(source, target):
    return len(get_same_stat_supports(source, target))


def improves_other_weakest_stat(source, target):
    weakest = get_weakest_ip_stat_info(target)
    weakest_name = weakest.get("name") or ""
    if not weakest_name:
        return False

    source_value = get_effective_ip_stat(source, weakest_name)
    target_value = get_effective_ip_stat(target, weakest_name)

    return source_value > 24 and target_value < 25


def get_build_eval(chicken, build_name):
    if not build_name:
        return {
            "build": "",
            "label": "",
            "match_count": 0,
            "match_total": 0,
            "matched_slots": [],
            "missing_slots": [],
        }
    return evaluate_build(chicken, build_name)


def count_missing_trait_support(source, target, build_name):
    if not build_name:
        return 0

    source_eval = get_build_eval(source, build_name)
    target_eval = get_build_eval(target, build_name)

    source_matched = set(source_eval.get("matched_slots", []))
    target_missing = set(target_eval.get("missing_slots", []))

    return len(source_matched & target_missing)


def get_missing_trait_support_slots(source, target, build_name):
    if not build_name:
        return []

    source_eval = get_build_eval(source, build_name)
    target_eval = get_build_eval(target, build_name)

    source_matched = set(source_eval.get("matched_slots", []))
    target_missing = set(target_eval.get("missing_slots", []))

    slot_order = ["beak", "comb", "eyes", "feet", "wings", "tail", "body"]
    return [slot for slot in slot_order if slot in source_matched and slot in target_missing]


def has_build_support(source, target, build_name):
    return has_strong_build_count(source) or count_missing_trait_support(source, target, build_name) >= 1


def has_ip_support(source, target):
    return has_high_ip(source) or count_same_stat_advantages(source, target) >= 1


def build_pair_supports(left, right, build_name):
    left_ip_support = has_ip_support(left, right)
    right_ip_support = has_ip_support(right, left)
    left_build_support = has_build_support(left, right, build_name)
    right_build_support = has_build_support(right, left, build_name)

    pair_ip_ok = left_ip_support or right_ip_support
    pair_build_ok = left_build_support or right_build_support

    cross_ok = (
        (left_ip_support and right_build_support)
        or (right_ip_support and left_build_support)
        or (left_ip_support and left_build_support)
        or (right_ip_support and right_build_support)
    )

    return {
        "left_ip_support": left_ip_support,
        "right_ip_support": right_ip_support,
        "left_build_support": left_build_support,
        "right_build_support": right_build_support,
        "pair_ip_ok": pair_ip_ok,
        "pair_build_ok": pair_build_ok,
        "cross_ok": cross_ok,
    }


def get_combined_best_stat_values(left, right):
    return {
        stat_name: max(
            get_effective_ip_stat(left, stat_name),
            get_effective_ip_stat(right, stat_name),
        )
        for stat_name in IP_STAT_PRIORITY
    }


def get_combined_best_stat_total(left, right):
    combined = get_combined_best_stat_values(left, right)
    return sum(combined.values())


def get_combined_build_coverage(left, right, build_name):
    if not build_name:
        return {
            "combined_count": 0,
            "combined_total": 0,
            "matched_slots": [],
        }

    left_eval = get_build_eval(left, build_name)
    right_eval = get_build_eval(right, build_name)

    matched_slots = set(left_eval.get("matched_slots", [])) | set(right_eval.get("matched_slots", []))
    combined_total = safe_int(left_eval.get("match_total"), default=0) or safe_int(right_eval.get("match_total"), default=0) or 0

    return {
        "combined_count": len(matched_slots),
        "combined_total": combined_total,
        "matched_slots": sorted(matched_slots),
    }


def get_ultimate_type(chicken):
    ip_value = safe_int((chicken or {}).get("ip"), default=0) or 0
    build_count = get_primary_build_count(chicken)

    has_ip = ip_value > 264
    has_build = build_count >= 5

    if has_ip and has_build:
        return "both"
    if has_ip:
        return "ip_only"
    if has_build or (ip_value > 174 and build_count > 2):
        return "gene_only"
    return ""


def get_ultimate_type_display(chicken):
    ultimate_type = get_ultimate_type(chicken)

    if ultimate_type == "both":
        return "Both"
    if ultimate_type == "ip_only":
        return "IP Only"
    if ultimate_type == "gene_only":
        return "Gene Only"
    return ""


def get_ultimate_build_display(chicken):
    build_name = get_primary_build(chicken)
    return build_name.title() if build_name else ""


def is_ultimate_eligible(chicken):
    build_count = get_primary_build_count(chicken)
    ip_value = safe_int((chicken or {}).get("ip"), default=0) or 0

    if ip_value > 174 and build_count > 2:
        return True
    if ip_value > 264:
        return True
    if build_count >= 5:
        return True
    return False


def is_valid_ultimate_pair(selected, candidate):
    selected_build = get_primary_build(selected)
    candidate_build = get_primary_build(candidate)

    if not selected_build or not candidate_build:
        return False

    if selected_build != candidate_build:
        return False

    supports = build_pair_supports(selected, candidate, selected_build)
    return supports["pair_ip_ok"] and supports["pair_build_ok"] and supports["cross_ok"]


def get_innate_item_candidate_for_stat(source, target, stat_name):
    source_value = get_effective_ip_stat(source, stat_name)
    target_value = get_effective_ip_stat(target, stat_name)

    if source_value <= 24 or target_value >= 25:
        return None

    item_name = ULTIMATE_INNATE_ITEM_BY_STAT[stat_name]

    return {
        "name": item_name,
        "reason": get_item_helper_text(item_name),
        "category": "innate",
        "stat": stat_name,
        "priority": 0,
    }


def get_trait_item_candidate_for_slot(source, target, build_name, slot_name):
    support_slots = get_missing_trait_support_slots(source, target, build_name)

    if slot_name not in support_slots:
        return None

    item_name = ULTIMATE_TRAIT_ITEM_BY_SLOT[slot_name]

    return {
        "name": item_name,
        "reason": get_item_helper_text(item_name),
        "category": "trait",
        "slot": slot_name,
        "priority": 0,
    }


def get_ultimate_item_candidates(source, target, build_name=None):
    build_name = build_name or get_primary_build(source) or get_primary_build(target)
    candidates = []

    if has_high_ip(source):
        candidates.append({
            "name": "Soulknot",
            "reason": get_item_helper_text("Soulknot"),
            "category": "special_ip",
            "priority": 100,
        })

    if count_same_stat_advantages(source, target) >= 4:
        candidates.append({
            "name": "Soulknot",
            "reason": get_item_helper_text("Soulknot"),
            "category": "special_ip",
            "priority": 95,
        })

    if has_strong_build_count(source):
        candidates.append({
            "name": "Gregor's Gift",
            "reason": get_item_helper_text("Gregor's Gift"),
            "category": "special_build",
            "priority": 90,
        })

    if count_missing_trait_support(source, target, build_name) >= 4:
        candidates.append({
            "name": "Gregor's Gift",
            "reason": get_item_helper_text("Gregor's Gift"),
            "category": "special_build",
            "priority": 85,
        })

    deduped_special = []
    seen_special = set()

    for candidate in sorted(candidates, key=lambda row: (-(row["priority"] or 0), row["name"])):
        key = (candidate["name"], candidate["category"])
        if key in seen_special:
            continue
        seen_special.add(key)
        deduped_special.append(candidate)

    if deduped_special:
        return deduped_special

    ordered_candidates = []

    for index, (stat_name, slot_name) in enumerate(ULTIMATE_ITEM_PRIORITY_ORDER):
        innate_candidate = get_innate_item_candidate_for_stat(source, target, stat_name)
        if innate_candidate:
            innate_candidate["priority"] = 70 - index
            ordered_candidates.append(innate_candidate)
            continue

        trait_candidate = get_trait_item_candidate_for_slot(source, target, build_name, slot_name)
        if trait_candidate:
            trait_candidate["priority"] = 60 - index
            ordered_candidates.append(trait_candidate)

    deduped_ordered = []
    seen_ordered = set()

    for candidate in ordered_candidates:
        key = (candidate["name"], candidate["category"])
        if key in seen_ordered:
            continue
        seen_ordered.add(key)
        deduped_ordered.append(candidate)

    return deduped_ordered


def resolve_ultimate_pair_item_recommendations(left_candidates, right_candidates):
    left_candidates = list(left_candidates or [])
    right_candidates = list(right_candidates or [])

    def is_innate(candidate):
        return str((candidate or {}).get("category") or "") == "innate"

    def is_trait(candidate):
        return str((candidate or {}).get("category") or "") == "trait"

    def is_soulknot(candidate):
        return str((candidate or {}).get("name") or "") == "Soulknot"

    def is_gregor(candidate):
        return str((candidate or {}).get("name") or "") == "Gregor's Gift"

    left_item = left_candidates[0] if left_candidates else None

    filtered_right = list(right_candidates)
    if is_soulknot(left_item):
        filtered_right = [
            row for row in filtered_right
            if not is_soulknot(row) and not is_innate(row)
        ]
    elif is_gregor(left_item):
        filtered_right = [
            row for row in filtered_right
            if not is_gregor(row) and not is_trait(row)
        ]

    right_item = filtered_right[0] if filtered_right else None

    filtered_left = list(left_candidates)
    if is_soulknot(right_item):
        filtered_left = [
            row for row in filtered_left
            if not is_soulknot(row) and not is_innate(row)
        ]
    elif is_gregor(right_item):
        filtered_left = [
            row for row in filtered_left
            if not is_gregor(row) and not is_trait(row)
        ]

    left_item = filtered_left[0] if filtered_left else None

    filtered_right = list(right_candidates)
    if is_soulknot(left_item):
        filtered_right = [
            row for row in filtered_right
            if not is_soulknot(row) and not is_innate(row)
        ]
    elif is_gregor(left_item):
        filtered_right = [
            row for row in filtered_right
            if not is_gregor(row) and not is_trait(row)
        ]

    right_item = filtered_right[0] if filtered_right else None
    return left_item, right_item


def build_ultimate_pair_quality_from_items(left, right, build_name, left_item=None, right_item=None):
    combined_total = get_combined_best_stat_total(left, right)
    combined_values = get_combined_best_stat_values(left, right)
    combined_build = get_combined_build_coverage(left, right, build_name)
    combined_build_count = combined_build["combined_count"]

    left_has_item = bool(left_item and str((left_item or {}).get("name") or "").strip())
    right_has_item = bool(right_item and str((right_item or {}).get("name") or "").strip())

    if not left_has_item or not right_has_item:
        return "Poor match"

    if combined_total > 264 and combined_build_count >= 5:
        return "Excellent match"

    if (
        175 <= combined_total <= 264
        and all(value >= 25 for value in combined_values.values())
        and combined_build_count >= 4
    ):
        return "Strong match"

    left_good = improves_other_weakest_stat(left, right)
    right_good = improves_other_weakest_stat(right, left)
    left_trait = count_missing_trait_support(left, right, build_name) >= 1
    right_trait = count_missing_trait_support(right, left, build_name) >= 1

    if (left_good and right_trait) or (right_good and left_trait):
        return "Good match"

    left_category = str((left_item or {}).get("category") or "")
    right_category = str((right_item or {}).get("category") or "")

    both_innate = left_category == "innate" and right_category == "innate"
    both_trait = left_category == "trait" and right_category == "trait"

    if both_innate or both_trait:
        return "Situational"

    if not left_has_item or not right_has_item:
        return "Poor match"

    return "Situational"


def build_ultimate_candidate_row(selected, candidate):
    build_name = get_primary_build(selected)

    left_candidates = get_ultimate_item_candidates(selected, candidate, build_name)
    right_candidates = get_ultimate_item_candidates(candidate, selected, build_name)
    left_item, right_item = resolve_ultimate_pair_item_recommendations(left_candidates, right_candidates)

    combined_build = get_combined_build_coverage(selected, candidate, build_name)
    supports = build_pair_supports(selected, candidate, build_name)

    return {
        "candidate": candidate,
        "ultimate_type_display": get_ultimate_type_display(candidate),
        "ultimate_build_display": get_ultimate_build_display(candidate),
        "build_complement": count_missing_trait_support(candidate, selected, build_name),
        "selected_ultimate_type": get_ultimate_type(selected),
        "candidate_ultimate_type": get_ultimate_type(candidate),
        "selected_build": build_name,
        "candidate_build": get_primary_build(candidate),
        "selected_build_match_count": get_primary_build_count(selected),
        "selected_build_match_total": get_primary_build_total(selected),
        "candidate_build_match_count": get_primary_build_count(candidate),
        "candidate_build_match_total": get_primary_build_total(candidate),
        "left_item": left_item,
        "right_item": right_item,
        "combined_build_count": combined_build["combined_count"],
        "combined_build_total": combined_build["combined_total"],
        "combined_ip_total": get_combined_best_stat_total(selected, candidate),
        "selected_stat_support_count": count_same_stat_advantages(selected, candidate),
        "candidate_stat_support_count": count_same_stat_advantages(candidate, selected),
        "selected_trait_support_count": count_missing_trait_support(selected, candidate, build_name),
        "candidate_trait_support_count": count_missing_trait_support(candidate, selected, build_name),
        "supports": supports,
        "pair_quality": build_ultimate_pair_quality_from_items(
            selected,
            candidate,
            build_name,
            left_item=left_item,
            right_item=right_item,
        ),
    }


def score_ultimate_candidate(selected, row):
    candidate = row["candidate"]
    return (
        -(row.get("combined_build_count") or 0),
        -(row.get("combined_ip_total") or 0),
        -(max(row.get("selected_trait_support_count") or 0, row.get("candidate_trait_support_count") or 0)),
        -(max(row.get("selected_stat_support_count") or 0, row.get("candidate_stat_support_count") or 0)),
        safe_int(candidate.get("breed_count"), default=999999) or 999999,
        -(float(candidate.get("ownership_percent") or 0)),
        -(safe_int(candidate.get("ip"), default=0) or 0),
        safe_int(candidate.get("token_id"), default=999999999) or 999999999,
    )


def filter_and_sort_ultimate_candidates(selected, chickens, require_items=False):
    rows = []

    for candidate in chickens or []:
        if str(candidate.get("token_id") or "") == str(selected.get("token_id") or ""):
            continue

        if not is_ultimate_eligible(candidate):
            continue

        if not is_valid_ultimate_pair(selected, candidate):
            continue

        row = build_ultimate_candidate_row(selected, candidate)

        if require_items and (not row.get("left_item") or not row.get("right_item")):
            continue

        rows.append(row)

    rows.sort(key=lambda row: score_ultimate_candidate(selected, row))
    return rows
