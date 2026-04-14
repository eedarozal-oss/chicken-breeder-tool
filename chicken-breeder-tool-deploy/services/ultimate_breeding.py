from services.primary_build_classifier import safe_int
from services.build_eval import (
    evaluate_build,
    evaluate_all_builds,
    select_qualified_build,
    build_gene_pair_metrics,
)
from services.item_helper_text import get_item_helper_text, normalize_item_name
from services.match_rules import (
    is_generation_gap_allowed,
    is_parent_offspring,
    is_full_siblings,
)

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

ULTIMATE_BUILD_PRIORITY_SLOTS = {
    "killua": ["beak", "tail", "feet", "body"],
    "shanks": ["beak", "wings", "tail", "feet", "body"],
    "levi": ["beak", "tail", "feet", "body"],
    "hybrid 2": ["wings"],
    "hybrid 1": [],
}

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
        selected = select_qualified_build(evaluations, min_matches=3, traits=refreshed)

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


def build_ultimate_ip_metrics(left, right):
    shared_strong = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(left, stat_name) >= 30
        and get_effective_ip_stat(right, stat_name) >= 30
    ]
    shared_usable = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(left, stat_name) >= 25
        and get_effective_ip_stat(right, stat_name) >= 25
    ]
    left_edge = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(left, stat_name) >= 25
        and get_effective_ip_stat(right, stat_name) < 25
    ]
    right_edge = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(right, stat_name) >= 25
        and get_effective_ip_stat(left, stat_name) < 25
    ]

    left_strong_count = len([stat_name for stat_name in IP_STAT_PRIORITY if get_effective_ip_stat(left, stat_name) >= 30])
    right_strong_count = len([stat_name for stat_name in IP_STAT_PRIORITY if get_effective_ip_stat(right, stat_name) >= 30])

    combined_usable = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if max(get_effective_ip_stat(left, stat_name), get_effective_ip_stat(right, stat_name)) >= 25
    ]

    stronger_count = max(left_strong_count, right_strong_count)
    weaker_count = min(left_strong_count, right_strong_count)

    has_upgrade_path = len(combined_usable) > max(
        len([stat_name for stat_name in IP_STAT_PRIORITY if get_effective_ip_stat(left, stat_name) >= 25]),
        len([stat_name for stat_name in IP_STAT_PRIORITY if get_effective_ip_stat(right, stat_name) >= 25]),
    )

    elite_stabilization = len(shared_strong) >= 5 and weaker_count >= 5
    anchor_finisher = stronger_count >= 5 and weaker_count >= 4 and has_upgrade_path

    return {
        "shared_strong_stats": shared_strong,
        "shared_strong_count": len(shared_strong),
        "shared_usable_stats": shared_usable,
        "shared_usable_count": len(shared_usable),
        "left_edge_stats": left_edge,
        "left_edge_count": len(left_edge),
        "right_edge_stats": right_edge,
        "right_edge_count": len(right_edge),
        "edge_count": len(left_edge) + len(right_edge),
        "combined_usable_count": len(combined_usable),
        "elite_stabilization": elite_stabilization,
        "anchor_finisher": anchor_finisher,
    }


def get_combined_build_coverage(left, right, build_name):
    if not build_name:
        return {
            "combined_count": 0,
            "combined_total": 0,
            "matched_slots": [],
            "build_pair_metrics": {
                "shared_count": 0,
                "combined_count": 0,
                "edge_count": 0,
                "elite_stabilization": False,
                "anchor_finisher": False,
            },
        }

    left_eval = get_build_eval(left, build_name)
    right_eval = get_build_eval(right, build_name)
    metrics = build_gene_pair_metrics(left_eval, right_eval)

    return {
        "combined_count": metrics["combined_count"],
        "combined_total": metrics["total"],
        "matched_slots": metrics["combined_slots"],
        "build_pair_metrics": metrics,
    }

def get_ultimate_build_priority_slots(build_name):
    return list(ULTIMATE_BUILD_PRIORITY_SLOTS.get(str(build_name or "").strip().lower(), []))

def get_ultimate_build_compatibility(build_type):
    build_key = str(build_type or "").strip().lower()

    compatibility = {
        "killua": {"killua", "hybrid 1", "hybrid 2"},
        "shanks": {"shanks", "hybrid 1"},
        "levi": {"levi", "hybrid 1", "hybrid 2"},
        "hybrid 1": {"killua", "shanks", "levi", "hybrid 1"},
        "hybrid 2": {"killua", "levi", "hybrid 2"},
    }

    return set(compatibility.get(build_key, {build_key} if build_key else set()))


def ultimate_builds_are_compatible(source_build, candidate_build):
    source_key = str(source_build or "").strip().lower()
    candidate_key = str(candidate_build or "").strip().lower()

    if not source_key or not candidate_key:
        return False

    source_compatible = get_ultimate_build_compatibility(source_key)
    candidate_compatible = get_ultimate_build_compatibility(candidate_key)

    return candidate_key in source_compatible and source_key in candidate_compatible

def build_ultimate_build_priority_metrics(left, right, build_name):
    build_name = str(build_name or "").strip().lower()
    priority_slots = get_ultimate_build_priority_slots(build_name)

    if not build_name or not priority_slots:
        return {
            "priority_slots": [],
            "left_priority_resolved_count": 0,
            "right_priority_resolved_count": 0,
            "priority_shared_count": 0,
            "priority_covered_count": 0,
            "left_priority_satisfied": True,
            "right_priority_satisfied": True,
            "priority_any_satisfied": True,
        }

    left_eval = get_build_eval(left, build_name)
    right_eval = get_build_eval(right, build_name)

    left_matched = set(left_eval.get("matched_slots", []))
    left_missing = set(left_eval.get("missing_slots", []))
    right_matched = set(right_eval.get("matched_slots", []))
    right_missing = set(right_eval.get("missing_slots", []))

    shared_priority_slots = [slot for slot in priority_slots if slot in left_matched and slot in right_matched]
    left_resolved_priority_slots = [slot for slot in priority_slots if slot in left_missing and slot in right_matched]
    right_resolved_priority_slots = [slot for slot in priority_slots if slot in right_missing and slot in left_matched]
    covered_priority_slots = [slot for slot in priority_slots if slot in (left_matched | right_matched)]

    left_missing_priority_slots = [slot for slot in priority_slots if slot in left_missing]
    right_missing_priority_slots = [slot for slot in priority_slots if slot in right_missing]

    left_priority_satisfied = (not left_missing_priority_slots) or bool(left_resolved_priority_slots) or bool(shared_priority_slots)
    right_priority_satisfied = (not right_missing_priority_slots) or bool(right_resolved_priority_slots) or bool(shared_priority_slots)

    return {
        "priority_slots": priority_slots,
        "left_priority_resolved_count": len(left_resolved_priority_slots),
        "right_priority_resolved_count": len(right_resolved_priority_slots),
        "priority_shared_count": len(shared_priority_slots),
        "priority_covered_count": len(covered_priority_slots),
        "left_priority_satisfied": left_priority_satisfied,
        "right_priority_satisfied": right_priority_satisfied,
        "priority_any_satisfied": left_priority_satisfied or right_priority_satisfied,
    }


def build_ultimate_ip_priority_metrics(left, right):
    left_weakest = get_weakest_ip_stat_info(left)
    right_weakest = get_weakest_ip_stat_info(right)

    left_priority_stat = left_weakest.get("name") or ""
    right_priority_stat = right_weakest.get("name") or ""

    left_priority_value = get_effective_ip_stat(left, left_priority_stat) if left_priority_stat else 0
    right_on_left_priority = get_effective_ip_stat(right, left_priority_stat) if left_priority_stat else 0

    right_priority_value = get_effective_ip_stat(right, right_priority_stat) if right_priority_stat else 0
    left_on_right_priority = get_effective_ip_stat(left, right_priority_stat) if right_priority_stat else 0

    left_priority_resolved = bool(left_priority_stat) and left_priority_value < 25 and right_on_left_priority >= 25
    right_priority_resolved = bool(right_priority_stat) and right_priority_value < 25 and left_on_right_priority >= 25

    shared_unresolved_weakness = (
        bool(left_priority_stat)
        and left_priority_stat == right_priority_stat
        and max(left_priority_value, right_priority_value) < 25
    )

    return {
        "left_priority_resolved": left_priority_resolved,
        "right_priority_resolved": right_priority_resolved,
        "priority_any_resolved": left_priority_resolved or right_priority_resolved,
        "shared_unresolved_weakness": shared_unresolved_weakness,
        "right_on_left_priority": right_on_left_priority,
        "left_on_right_priority": left_on_right_priority,
    }

def get_ultimate_below_threshold_stats(chicken, threshold=25):
    return [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(chicken, stat_name) < threshold
    ]


def count_ultimate_fixed_below_threshold_stats(source, target, threshold=25):
    fixed = []

    for stat_name in IP_STAT_PRIORITY:
        target_value = get_effective_ip_stat(target, stat_name)
        source_value = get_effective_ip_stat(source, stat_name)

        if target_value < threshold and source_value >= threshold:
            fixed.append(stat_name)

    return fixed


def get_ultimate_ip_threshold_metrics(left, right, threshold=25):
    left_below = get_ultimate_below_threshold_stats(left, threshold)
    right_below = get_ultimate_below_threshold_stats(right, threshold)

    left_fixes_right = count_ultimate_fixed_below_threshold_stats(left, right, threshold)
    right_fixes_left = count_ultimate_fixed_below_threshold_stats(right, left, threshold)

    combined_below_remaining = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if max(
            get_effective_ip_stat(left, stat_name),
            get_effective_ip_stat(right, stat_name),
        ) < threshold
    ]

    return {
        "left_below_count": len(left_below),
        "right_below_count": len(right_below),
        "left_below_stats": left_below,
        "right_below_stats": right_below,
        "left_fixes_right_count": len(left_fixes_right),
        "left_fixes_right_stats": left_fixes_right,
        "right_fixes_left_count": len(right_fixes_left),
        "right_fixes_left_stats": right_fixes_left,
        "mutual_fix_count": len(left_fixes_right) + len(right_fixes_left),
        "combined_below_remaining_count": len(combined_below_remaining),
        "combined_below_remaining_stats": combined_below_remaining,
        "all_threshold_gaps_resolved": len(combined_below_remaining) == 0,
    }


def get_ultimate_ip_burden_metrics(left, right, threshold=25):
    left_below = get_ultimate_below_threshold_stats(left, threshold)
    right_below = get_ultimate_below_threshold_stats(right, threshold)

    return {
        "left_below_count": len(left_below),
        "right_below_count": len(right_below),
        "total_below_count": len(left_below) + len(right_below),
    }


def get_ultimate_build_overlap_penalty(shared_count):
    shared_count = safe_int(shared_count, default=0) or 0

    if shared_count <= 0:
        return 220
    if shared_count == 1:
        return 90
    if shared_count == 2:
        return 25
    return 0


def get_ultimate_item_score_bonus(item):
    item_name = str((item or {}).get("name") or "").strip()

    if not item_name:
        return 0

    if item_name == "Gregor's Gift":
        return 18

    if item_name == "Soulknot":
        return 18

    if item_name in {
        "Chim Lac's Curio",
        "Simurgh's Sovereign",
        "Dragon's Whip",
        "Quentin's Talon",
        "Chibidei's Curse",
    }:
        return 12

    if item_name in {
        "Cocktail's Beak",
        "Pos2 Pellet",
        "Vananderen's Vitality",
        "Fetzzz Feet",
        "Lockedin State",
        "Ouchie's Ornament",
        "Pinong's Bird",
    }:
        return 10

    if item_name in {
        "Suave Scissors",
        "All-seeing Seed",
    }:
        return 6

    return 0

def get_ultimate_build_target_cap(build_name, build_metrics=None):
    build_name = str(build_name or "").strip().lower()
    build_metrics = build_metrics or {}

    total = safe_int(build_metrics.get("total"), default=0)
    if total is None or total <= 0:
        total = safe_int(build_metrics.get("combined_total"), default=0) or 0

    if total <= 0:
        return 5

    return min(5, total)

def compute_ultimate_build_score(
    left,
    right,
    build_name,
    build_metrics=None,
    build_priority_metrics=None,
    left_item=None,
    right_item=None,
):
    build_metrics = build_metrics or {}
    build_priority_metrics = build_priority_metrics or {}

    combined_count = safe_int(build_metrics.get("combined_count"), default=0) or 0
    shared_count = safe_int(build_metrics.get("shared_count"), default=0) or 0
    edge_count = safe_int(build_metrics.get("edge_count"), default=0) or 0
    candidate_build_count = get_primary_build_count(right)
    added_missing_traits = count_missing_trait_support(right, left, build_name)

    target_cap = get_ultimate_build_target_cap(build_name, build_metrics)
    capped_combined = min(combined_count, target_cap)
    capped_shared = min(shared_count, target_cap)
    capped_candidate = min(candidate_build_count, target_cap)

    priority_bonus = 0
    if build_priority_metrics.get("left_priority_resolved_count", 0):
        priority_bonus += 20
    elif build_priority_metrics.get("left_priority_satisfied"):
        priority_bonus += 10

    if build_priority_metrics.get("priority_shared_count", 0):
        priority_bonus += 6

    overlap_penalty = get_ultimate_build_overlap_penalty(shared_count)

    item_bonus = max(
        get_ultimate_item_score_bonus(left_item),
        get_ultimate_item_score_bonus(right_item),
    )

    score = 0
    score += capped_combined * 85
    score += capped_shared * 30
    score += added_missing_traits * 18
    score += capped_candidate * 8
    score += edge_count * 4
    score += priority_bonus
    score += item_bonus
    score -= overlap_penalty

    if combined_count >= target_cap:
        score += 25

    return score


def compute_ultimate_ip_score(
    left,
    right,
    ip_metrics=None,
    ip_priority_metrics=None,
    ip_threshold_metrics=None,
    ip_burden_metrics=None,
):
    ip_metrics = ip_metrics or {}
    ip_priority_metrics = ip_priority_metrics or {}
    ip_threshold_metrics = ip_threshold_metrics or {}
    ip_burden_metrics = ip_burden_metrics or {}

    score = 0

    if ip_priority_metrics.get("shared_unresolved_weakness"):
        score -= 180

    if ip_threshold_metrics.get("all_threshold_gaps_resolved"):
        score += 120

    score += (safe_int(ip_threshold_metrics.get("right_fixes_left_count"), default=0) or 0) * 45
    score += (safe_int(ip_threshold_metrics.get("left_fixes_right_count"), default=0) or 0) * 20
    score -= (safe_int(ip_threshold_metrics.get("combined_below_remaining_count"), default=0) or 0) * 60
    score -= (safe_int(ip_burden_metrics.get("right_below_count"), default=0) or 0) * 18
    score -= (safe_int(ip_burden_metrics.get("total_below_count"), default=0) or 0) * 8

    shared_strong = safe_int(ip_metrics.get("shared_strong_count"), default=0) or 0
    shared_usable = safe_int(ip_metrics.get("shared_usable_count"), default=0) or 0
    combined_usable = safe_int(ip_metrics.get("combined_usable_count"), default=0) or 0
    edge_count = safe_int(ip_metrics.get("edge_count"), default=0) or 0

    score += shared_strong * 24
    score += shared_usable * 12
    score += combined_usable * 8
    score += edge_count * 3

    if shared_strong >= 5:
        score += 40
    elif shared_usable >= 5:
        score += 20

    if ip_metrics.get("elite_stabilization"):
        score += 80
    if ip_metrics.get("anchor_finisher"):
        score += 45
    if ip_priority_metrics.get("left_priority_resolved"):
        score += 35

    return score

def compute_ultimate_pair_score(
    left,
    right,
    build_name,
    build_metrics=None,
    ip_metrics=None,
    build_priority_metrics=None,
    ip_priority_metrics=None,
    ip_threshold_metrics=None,
    ip_burden_metrics=None,
    left_item=None,
    right_item=None,
):
    build_score = compute_ultimate_build_score(
        left=left,
        right=right,
        build_name=build_name,
        build_metrics=build_metrics,
        build_priority_metrics=build_priority_metrics,
        left_item=left_item,
        right_item=right_item,
    )

    ip_score = compute_ultimate_ip_score(
        left=left,
        right=right,
        ip_metrics=ip_metrics,
        ip_priority_metrics=ip_priority_metrics,
        ip_threshold_metrics=ip_threshold_metrics,
        ip_burden_metrics=ip_burden_metrics,
    )

    return {
        "build_score": build_score,
        "ip_score": ip_score,
        "total_score": build_score + ip_score,
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

    if not ultimate_builds_are_compatible(selected_build, candidate_build):
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
    combined_build = get_combined_build_coverage(left, right, build_name)
    build_metrics = combined_build["build_pair_metrics"]
    ip_metrics = build_ultimate_ip_metrics(left, right)
    build_priority_metrics = build_ultimate_build_priority_metrics(left, right, build_name)
    ip_priority_metrics = build_ultimate_ip_priority_metrics(left, right)
    ip_threshold_metrics = get_ultimate_ip_threshold_metrics(left, right, threshold=25)
    ip_burden_metrics = get_ultimate_ip_burden_metrics(left, right, threshold=25)

    scores = compute_ultimate_pair_score(
        left=left,
        right=right,
        build_name=build_name,
        build_metrics=build_metrics,
        ip_metrics=ip_metrics,
        build_priority_metrics=build_priority_metrics,
        ip_priority_metrics=ip_priority_metrics,
        ip_threshold_metrics=ip_threshold_metrics,
        ip_burden_metrics=ip_burden_metrics,
        left_item=left_item,
        right_item=right_item,
    )

    total_score = scores["total_score"]
    build_score = scores["build_score"]
    ip_score = scores["ip_score"]

    if build_score < 320 or ip_score < 350:
        if total_score >= 900:
            return "Good match"
        if total_score >= 650:
            return "Situational"
        return "Poor match"

    if total_score >= 1350:
        return "Excellent match"
    if total_score >= 1080:
        return "Strong match"
    if total_score >= 820:
        return "Good match"
    if total_score >= 620:
        return "Situational"

    return "Poor match"

def rank_ultimate_pair(
    selected,
    candidate,
    build_name="",
    build_metrics=None,
    ip_metrics=None,
    build_priority_metrics=None,
    ip_priority_metrics=None,
    ip_threshold_metrics=None,
    ip_burden_metrics=None,
    left_item=None,
    right_item=None,
):
    selected = selected or {}
    candidate = candidate or {}
    build_metrics = build_metrics or {}
    ip_metrics = ip_metrics or {}
    build_priority_metrics = build_priority_metrics or {}
    ip_priority_metrics = ip_priority_metrics or {}
    ip_threshold_metrics = ip_threshold_metrics or {}
    ip_burden_metrics = ip_burden_metrics or {}

    scores = compute_ultimate_pair_score(
        left=selected,
        right=candidate,
        build_name=build_name,
        build_metrics=build_metrics,
        ip_metrics=ip_metrics,
        build_priority_metrics=build_priority_metrics,
        ip_priority_metrics=ip_priority_metrics,
        ip_threshold_metrics=ip_threshold_metrics,
        ip_burden_metrics=ip_burden_metrics,
        left_item=left_item,
        right_item=right_item,
    )

    total_score = scores["total_score"]
    build_score = scores["build_score"]
    ip_score = scores["ip_score"]

    return (
        -total_score,
        -build_score,
        -ip_score,
        -(build_metrics.get("combined_count") or 0),
        -(build_metrics.get("shared_count") or 0),
        -(ip_metrics.get("shared_usable_count") or 0),
        -(ip_threshold_metrics.get("right_fixes_left_count") or 0),
        ip_burden_metrics.get("right_below_count") or 0,
        ip_burden_metrics.get("total_below_count") or 0,
        safe_int(candidate.get("breed_count"), default=999999) or 999999,
        -(float(candidate.get("ownership_percent") or 0)),
        -(safe_int(candidate.get("ip"), default=0) or 0),
        safe_int(candidate.get("token_id"), default=999999999) or 999999999,
        safe_int(selected.get("breed_count"), default=999999) or 999999,
        -(float(selected.get("ownership_percent") or 0)),
        -(safe_int(selected.get("ip"), default=0) or 0),
        safe_int(selected.get("token_id"), default=999999999) or 999999999,
    )

def build_ultimate_candidate_row(selected, candidate):
    build_name = get_primary_build(selected)

    left_candidates = get_ultimate_item_candidates(selected, candidate, build_name)
    right_candidates = get_ultimate_item_candidates(candidate, selected, build_name)
    left_item, right_item = resolve_ultimate_pair_item_recommendations(left_candidates, right_candidates)

    combined_build = get_combined_build_coverage(selected, candidate, build_name)
    supports = build_pair_supports(selected, candidate, build_name)
    ip_metrics = build_ultimate_ip_metrics(selected, candidate)
    build_metrics = combined_build["build_pair_metrics"]

    build_priority_metrics = build_ultimate_build_priority_metrics(selected, candidate, build_name)
    ip_priority_metrics = build_ultimate_ip_priority_metrics(selected, candidate)
    ip_threshold_metrics = get_ultimate_ip_threshold_metrics(selected, candidate, threshold=25)
    ip_burden_metrics = get_ultimate_ip_burden_metrics(selected, candidate, threshold=25)

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
        "ultimate_build_metrics": build_metrics,
        "ultimate_ip_metrics": ip_metrics,
        "ultimate_ip_threshold_metrics": ip_threshold_metrics,
        "ultimate_ip_burden_metrics": ip_burden_metrics,
        "ultimate_build_priority_metrics": build_priority_metrics,
        "ultimate_ip_priority_metrics": ip_priority_metrics,
        "pair_quality": build_ultimate_pair_quality_from_items(
            selected,
            candidate,
            build_name,
            left_item=left_item,
            right_item=right_item,
        ),
        "ranking": rank_ultimate_pair(
            selected=selected,
            candidate=candidate,
            build_name=build_name,
            build_metrics=build_metrics,
            ip_metrics=ip_metrics,
            build_priority_metrics=build_priority_metrics,
            ip_priority_metrics=ip_priority_metrics,
            ip_threshold_metrics=ip_threshold_metrics,
            ip_burden_metrics=ip_burden_metrics,
            left_item=left_item,
            right_item=right_item,
        ),
    }


def score_ultimate_candidate(selected, row):
    return row.get("ranking") or rank_ultimate_pair(
        selected=selected,
        candidate=row.get("candidate") or {},
        build_name=str(row.get("selected_build") or row.get("build_type") or "").strip().lower(),
        build_metrics=row.get("ultimate_build_metrics") or {},
        ip_metrics=row.get("ultimate_ip_metrics") or {},
        build_priority_metrics=row.get("ultimate_build_priority_metrics") or {},
        ip_priority_metrics=row.get("ultimate_ip_priority_metrics") or {},
        ip_threshold_metrics=row.get("ultimate_ip_threshold_metrics") or {},
        ip_burden_metrics=row.get("ultimate_ip_burden_metrics") or {},
        left_item=row.get("left_item"),
        right_item=row.get("right_item"),
    )


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


def pick_best_ultimate_auto_match(breedable_chickens):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in breedable_chickens or []:
        selected_token_id = str(selected.get("token_id") or "")
        candidate_pool = [
            row for row in (breedable_chickens or [])
            if str(row.get("token_id") or "") != selected_token_id
            and not is_parent_offspring(selected, row)
            and not is_full_siblings(selected, row)
            and is_generation_gap_allowed(
                selected,
                row,
                max_gap=3,
            )
        ]

        matches = filter_and_sort_ultimate_candidates(selected, candidate_pool)
        if not matches:
            continue

        top = matches[0]
        ranking = top.get("ranking") or score_ultimate_candidate(selected, top)

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches


def build_ultimate_available_auto_candidates(breedable_chickens, breed_diff=None, ninuno_mode="all"):
    pair_rows = []

    for index, source in enumerate(breedable_chickens or []):
        if not chicken_passes_auto_ninuno_filter(source, ninuno_mode):
            continue

        for candidate in (breedable_chickens or [])[index + 1:]:
            if not chicken_passes_auto_ninuno_filter(candidate, ninuno_mode):
                continue

            if breed_diff is not None:
                source_breed = safe_int(source.get("breed_count"), default=None)
                candidate_breed = safe_int(candidate.get("breed_count"), default=None)
                if source_breed is None or candidate_breed is None or abs(candidate_breed - source_breed) > breed_diff:
                    continue

            if is_parent_offspring(source, candidate):
                continue

            if is_full_siblings(source, candidate):
                continue

            if not is_generation_gap_allowed(source, candidate, max_gap=3):
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

            chosen_build = str(
                chosen_left.get("primary_build")
                or chosen_right.get("primary_build")
                or chosen_match.get("selected_build")
                or ""
            ).strip().lower()

            pair_rows.append(
                {
                    "left": chosen_left,
                    "right": chosen_right,
                    "left_item": chosen_match.get("left_item"),
                    "right_item": chosen_match.get("right_item"),
                    "build_type": chosen_build,
                    "build_complement": chosen_match.get("build_complement"),
                    "left_adds_missing_traits": count_missing_trait_support(chosen_left, chosen_right, chosen_build),
                    "right_adds_missing_traits": count_missing_trait_support(chosen_right, chosen_left, chosen_build),
                    "pair_quality": build_ultimate_pair_quality_from_items(
                        chosen_left,
                        chosen_right,
                        chosen_build,
                        left_item=chosen_match.get("left_item"),
                        right_item=chosen_match.get("right_item"),
                    ),
                    "ranking": chosen_match.get("ranking") or score_ultimate_candidate(chosen_left, chosen_match),
                    "ultimate_build_metrics": chosen_match.get("ultimate_build_metrics") or {},
                    "ultimate_ip_metrics": chosen_match.get("ultimate_ip_metrics") or {},
                    "ultimate_build_priority_metrics": chosen_match.get("ultimate_build_priority_metrics") or {},
                    "ultimate_ip_priority_metrics": chosen_match.get("ultimate_ip_priority_metrics") or {},
                    "ultimate_ip_threshold_metrics": chosen_match.get("ultimate_ip_threshold_metrics") or {},
                    "ultimate_ip_burden_metrics": chosen_match.get("ultimate_ip_burden_metrics") or {},
                }
            )

    pair_rows.sort(key=lambda row: row["ranking"])
    return pair_rows

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
