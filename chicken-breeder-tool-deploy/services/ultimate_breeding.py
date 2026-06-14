from services.builds_config import BUILD_PRIORITY
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
from services.ip_breeding import compute_ip_pair_score as compute_raw_ip_pair_score
from services.gene_breeding import compute_gene_pair_score as compute_raw_gene_pair_score
from services.validation_thresholds import EXCELLENT_CHICKEN_VALIDATION_THRESHOLD

ULTIMATE_IP_STRONG_THRESHOLD = 265
ULTIMATE_IP_ENTRY_THRESHOLD = 175
ULTIMATE_BUILD_ENTRY_THRESHOLD = 5
ULTIMATE_BUILD_PARTIAL_THRESHOLD = 3
ULTIMATE_STRONG_BUILD_SCORE_THRESHOLD = 220
ULTIMATE_STRONG_IP_SCORE_THRESHOLD = 220
ULTIMATE_EXCELLENT_IP_SCORE_THRESHOLD = 300
ULTIMATE_EXCELLENT_BUILD_SCORE_THRESHOLD = 300
ULTIMATE_STRONG_TOTAL_SCORE_THRESHOLD = 440
ULTIMATE_EXCELLENT_TOTAL_SCORE_THRESHOLD = 600
ULTIMATE_ITEM_CONSTRAINED_IP_SLOT_PENALTY = 45
ULTIMATE_ITEM_CONSTRAINED_BUILD_SLOT_PENALTY = 35

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
    raw = str((chicken or {}).get("primary_build") or "").strip().lower()
    return raw if raw in BUILD_PRIORITY else ""


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

    left_priority_resolved = (
        bool(left_priority_stat)
        and left_priority_value < EXCELLENT_CHICKEN_VALIDATION_THRESHOLD
        and right_on_left_priority >= EXCELLENT_CHICKEN_VALIDATION_THRESHOLD
    )
    right_priority_resolved = (
        bool(right_priority_stat)
        and right_priority_value < EXCELLENT_CHICKEN_VALIDATION_THRESHOLD
        and left_on_right_priority >= EXCELLENT_CHICKEN_VALIDATION_THRESHOLD
    )

    shared_unresolved_weakness = (
        bool(left_priority_stat)
        and left_priority_stat == right_priority_stat
        and max(left_priority_value, right_priority_value) < EXCELLENT_CHICKEN_VALIDATION_THRESHOLD
    )

    return {
        "left_priority_resolved": left_priority_resolved,
        "right_priority_resolved": right_priority_resolved,
        "priority_any_resolved": left_priority_resolved or right_priority_resolved,
        "shared_unresolved_weakness": shared_unresolved_weakness,
        "right_on_left_priority": right_on_left_priority,
        "left_on_right_priority": left_on_right_priority,
    }

def get_ultimate_below_threshold_stats(chicken, threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD):
    return [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(chicken, stat_name) < threshold
    ]


def count_ultimate_fixed_below_threshold_stats(source, target, threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD):
    fixed = []

    for stat_name in IP_STAT_PRIORITY:
        target_value = get_effective_ip_stat(target, stat_name)
        source_value = get_effective_ip_stat(source, stat_name)

        if target_value < threshold and source_value >= threshold:
            fixed.append(stat_name)

    return fixed


def get_ultimate_ip_threshold_metrics(left, right, threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD):
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


def get_ultimate_ip_burden_metrics(left, right, threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD):
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


def get_ultimate_item_domain(item):
    item_name = str((item or {}).get("name") or "").strip()
    category = str((item or {}).get("category") or "").strip().lower()

    if category in {"innate", "special_ip"}:
        return "ip"
    if category in {"trait", "special_build"}:
        return "build"

    if item_name in set(ULTIMATE_INNATE_ITEM_BY_STAT.values()) or item_name == "Soulknot":
        return "ip"
    if item_name in set(ULTIMATE_TRAIT_ITEM_BY_SLOT.values()) or item_name == "Gregor's Gift":
        return "build"

    return ""


def count_ultimate_item_domains(left_item=None, right_item=None):
    counts = {"ip": 0, "build": 0}

    for item in (left_item, right_item):
        domain = get_ultimate_item_domain(item)
        if domain in counts:
            counts[domain] += 1

    return counts


def get_ultimate_build_target_cap(build_name, build_metrics=None):
    build_name = str(build_name or "").strip().lower()
    build_metrics = build_metrics or {}

    total = safe_int(build_metrics.get("total"), default=0)
    if total is None or total <= 0:
        total = safe_int(build_metrics.get("combined_total"), default=0) or 0

    if total <= 0:
        return 5

    return min(5, total)


def get_ultimate_build_item_need(left, right, build_name):
    build_name = str(build_name or "").strip().lower()
    if not build_name:
        return 0

    left_supports_right = count_missing_trait_support(left, right, build_name)
    right_supports_left = count_missing_trait_support(right, left, build_name)
    return min(2, left_supports_right + right_supports_left)


def get_ultimate_gene_priority_metrics(build_priority_metrics=None):
    build_priority_metrics = build_priority_metrics or {}
    return {
        "selected_priority_resolved_count": safe_int(
            build_priority_metrics.get("left_priority_resolved_count"),
            default=0,
        ) or 0,
        "candidate_priority_resolved_count": safe_int(
            build_priority_metrics.get("right_priority_resolved_count"),
            default=0,
        ) or 0,
        "priority_shared_count": safe_int(
            build_priority_metrics.get("priority_shared_count"),
            default=0,
        ) or 0,
        "selected_priority_satisfied": bool(build_priority_metrics.get("left_priority_satisfied")),
        "candidate_priority_satisfied": bool(build_priority_metrics.get("right_priority_satisfied")),
    }


def compute_raw_ultimate_build_score(left, right, build_name, build_metrics=None, build_priority_metrics=None):
    build_metrics = build_metrics or {}
    build_priority_metrics = build_priority_metrics or {}
    raw_gene_score = compute_raw_gene_pair_score(
        selected_chicken=left,
        candidate=right,
        build_type=build_name,
        pair_metrics=build_metrics,
        priority_metrics=get_ultimate_gene_priority_metrics(build_priority_metrics),
        added_missing_traits=count_missing_trait_support(right, left, build_name),
    )
    return raw_gene_score


def compute_ultimate_build_score(
    left,
    right,
    build_name,
    build_metrics=None,
    build_priority_metrics=None,
    left_item=None,
    right_item=None,
):
    raw_gene_score = compute_raw_ultimate_build_score(
        left=left,
        right=right,
        build_name=build_name,
        build_metrics=build_metrics,
        build_priority_metrics=build_priority_metrics,
    )
    return raw_gene_score["points"]


def apply_ultimate_build_item_constraint(build_score, left, right, build_name, left_item=None, right_item=None):
    item_domain_counts = count_ultimate_item_domains(left_item=left_item, right_item=right_item)
    build_item_need = get_ultimate_build_item_need(left, right, build_name)
    unmet_build_item_need = max(0, build_item_need - item_domain_counts["build"])
    combined_build = get_combined_build_coverage(left, right, build_name)
    build_metrics = combined_build.get("build_pair_metrics") or {}
    unresolved_build_count = max(
        0,
        (safe_int(build_metrics.get("total"), default=0) or 0)
        - (safe_int(build_metrics.get("combined_count"), default=0) or 0),
    )
    penalty = (
        unmet_build_item_need * ULTIMATE_ITEM_CONSTRAINED_BUILD_SLOT_PENALTY
        + unresolved_build_count * ULTIMATE_ITEM_CONSTRAINED_BUILD_SLOT_PENALTY
    )
    return build_score - penalty


def get_ultimate_item_constraint_details(
    left,
    right,
    build_name,
    ip_threshold_metrics=None,
    build_metrics=None,
    left_item=None,
    right_item=None,
):
    ip_threshold_metrics = ip_threshold_metrics or {}
    build_metrics = build_metrics or (get_combined_build_coverage(left, right, build_name).get("build_pair_metrics") or {})
    item_domain_counts = count_ultimate_item_domains(left_item=left_item, right_item=right_item)
    left_below_count = safe_int(ip_threshold_metrics.get("left_below_count"), default=0) or 0
    right_below_count = safe_int(ip_threshold_metrics.get("right_below_count"), default=0) or 0
    unresolved_count = safe_int(ip_threshold_metrics.get("combined_below_remaining_count"), default=0) or 0
    ip_item_need = min(2, left_below_count + right_below_count)
    build_item_need = get_ultimate_build_item_need(left, right, build_name)
    unmet_ip_item_need = max(0, ip_item_need - item_domain_counts["ip"])
    unmet_build_item_need = max(0, build_item_need - item_domain_counts["build"])
    unresolved_build_count = max(
        0,
        (safe_int(build_metrics.get("total"), default=0) or 0)
        - (safe_int(build_metrics.get("combined_count"), default=0) or 0),
    )

    return {
        "item_domain_counts": item_domain_counts,
        "ip_item_need": ip_item_need,
        "build_item_need": build_item_need,
        "unmet_ip_item_need": unmet_ip_item_need,
        "unmet_build_item_need": unmet_build_item_need,
        "unresolved_ip_count": unresolved_count,
        "unresolved_build_count": unresolved_build_count,
        "ip_item_constraint_penalty": (
            unmet_ip_item_need * ULTIMATE_ITEM_CONSTRAINED_IP_SLOT_PENALTY
            + unresolved_count * ULTIMATE_ITEM_CONSTRAINED_IP_SLOT_PENALTY
        ),
        "build_item_constraint_penalty": (
            unmet_build_item_need * ULTIMATE_ITEM_CONSTRAINED_BUILD_SLOT_PENALTY
            + unresolved_build_count * ULTIMATE_ITEM_CONSTRAINED_BUILD_SLOT_PENALTY
        ),
    }


def compute_ultimate_ip_score(
    left,
    right,
    ip_metrics=None,
    ip_priority_metrics=None,
    ip_threshold_metrics=None,
    ip_burden_metrics=None,
    left_item=None,
    right_item=None,
):
    ip_threshold_metrics = ip_threshold_metrics or {}
    raw_ip_score = compute_raw_ip_pair_score(left, right)
    item_domain_counts = count_ultimate_item_domains(left_item=left_item, right_item=right_item)

    left_below_count = safe_int(ip_threshold_metrics.get("left_below_count"), default=0) or 0
    right_below_count = safe_int(ip_threshold_metrics.get("right_below_count"), default=0) or 0
    unresolved_count = safe_int(ip_threshold_metrics.get("combined_below_remaining_count"), default=0) or 0

    ip_item_need = min(2, left_below_count + right_below_count)
    unmet_ip_item_need = max(0, ip_item_need - item_domain_counts["ip"])
    item_constraint_penalty = unmet_ip_item_need * ULTIMATE_ITEM_CONSTRAINED_IP_SLOT_PENALTY

    if unresolved_count:
        item_constraint_penalty += unresolved_count * ULTIMATE_ITEM_CONSTRAINED_IP_SLOT_PENALTY

    return raw_ip_score["points"] - item_constraint_penalty

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
    if not build_metrics and build_name:
        build_metrics = get_combined_build_coverage(left, right, build_name)["build_pair_metrics"]
    if not ip_metrics:
        ip_metrics = build_ultimate_ip_metrics(left, right)
    if not build_priority_metrics and build_name:
        build_priority_metrics = build_ultimate_build_priority_metrics(left, right, build_name)
    if not ip_priority_metrics:
        ip_priority_metrics = build_ultimate_ip_priority_metrics(left, right)
    if not ip_threshold_metrics:
        ip_threshold_metrics = get_ultimate_ip_threshold_metrics(
            left,
            right,
            threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD,
        )
    if not ip_burden_metrics:
        ip_burden_metrics = get_ultimate_ip_burden_metrics(
            left,
            right,
            threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD,
        )

    raw_ip_score = compute_raw_ip_pair_score(left, right)
    item_constraint_details = get_ultimate_item_constraint_details(
        left,
        right,
        build_name,
        ip_threshold_metrics=ip_threshold_metrics,
        build_metrics=build_metrics,
        left_item=left_item,
        right_item=right_item,
    )
    raw_build_score = compute_raw_ultimate_build_score(
        left=left,
        right=right,
        build_name=build_name,
        build_metrics=build_metrics,
        build_priority_metrics=build_priority_metrics,
    )
    build_score = compute_ultimate_build_score(
        left=left,
        right=right,
        build_name=build_name,
        build_metrics=build_metrics,
        build_priority_metrics=build_priority_metrics,
        left_item=left_item,
        right_item=right_item,
    )
    build_score = apply_ultimate_build_item_constraint(
        build_score,
        left=left,
        right=right,
        build_name=build_name,
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
        left_item=left_item,
        right_item=right_item,
    )

    return {
        "build_score": build_score,
        "ip_score": ip_score,
        "raw_build_score": raw_build_score["points"],
        "raw_build_grade": raw_build_score["grade"],
        "raw_build_notes": raw_build_score.get("notes") or [],
        "raw_build_note_display": raw_build_score.get("note_display") or "",
        "raw_ip_score": raw_ip_score["points"],
        "raw_ip_grade": raw_ip_score["grade"],
        "item_constraint_details": item_constraint_details,
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


def _get_ultimate_build_eval_for(chicken, build_name):
    build_name = str(build_name or "").strip().lower()
    evaluations = (chicken or {}).get("primary_build_evaluations") or {}
    stored = evaluations.get(build_name) if isinstance(evaluations, dict) else None
    if stored:
        return stored
    return get_build_eval(chicken, build_name)


def _get_ultimate_build_count_for(chicken, build_name):
    build_eval = _get_ultimate_build_eval_for(chicken, build_name)
    return safe_int(build_eval.get("match_count"), default=0) or 0


def _ultimate_parent_qualifies_for_gregor(source, target, build_name):
    build_name = str(build_name or "").strip().lower()
    if not build_name:
        return False

    source_count = _get_ultimate_build_count_for(source, build_name)
    target_count = _get_ultimate_build_count_for(target, build_name)
    if source_count < 6 or source_count <= target_count:
        return False

    supplied_slots = get_missing_trait_support_slots(source, target, build_name)
    target_missing_count = len((_get_ultimate_build_eval_for(target, build_name) or {}).get("missing_slots") or [])
    priority_supplied_slots = [
        slot for slot in supplied_slots
        if slot in set(get_ultimate_build_priority_slots(build_name))
    ]

    return (
        target_missing_count > 0
        and (
            len(priority_supplied_slots) >= 2
            or len(supplied_slots) * 2 >= target_missing_count
        )
    )


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

    if _ultimate_parent_qualifies_for_gregor(source, target, build_name):
        candidates.append({
            "name": "Gregor's Gift",
            "reason": get_item_helper_text("Gregor's Gift"),
            "category": "special_build",
            "priority": 90,
        })

    deduped_special = []
    seen_special = set()

    for candidate in sorted(candidates, key=lambda row: (-(row["priority"] or 0), row["name"])):
        key = (candidate["name"], candidate["category"])
        if key in seen_special:
            continue
        seen_special.add(key)
        deduped_special.append(candidate)

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

    return deduped_special + deduped_ordered


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

    def pair_is_allowed(left_item, right_item):
        if not left_item or not right_item:
            return True

        left_name = str((left_item or {}).get("name") or "")
        right_name = str((right_item or {}).get("name") or "")
        if left_name and left_name == right_name and left_name not in {"Gregor's Gift", "Soulknot"}:
            return False

        if is_soulknot(left_item) and (is_soulknot(right_item) or is_innate(right_item)):
            return False
        if is_soulknot(right_item) and (is_soulknot(left_item) or is_innate(left_item)):
            return False
        if is_gregor(left_item) and (is_gregor(right_item) or is_trait(right_item)):
            return False
        if is_gregor(right_item) and (is_gregor(left_item) or is_trait(left_item)):
            return False

        return True

    def item_rank(item):
        if not item:
            return -999
        domain_bonus = {
            "ip": 8,
            "build": 8,
        }.get(get_ultimate_item_domain(item), 0)
        return (safe_int((item or {}).get("priority"), default=0) or 0) + get_ultimate_item_score_bonus(item) + domain_bonus

    possible_left = left_candidates or [None]
    possible_right = right_candidates or [None]
    best_pair = (None, None)
    best_rank = None

    for left_item in possible_left:
        for right_item in possible_right:
            if not pair_is_allowed(left_item, right_item):
                continue
            domains = count_ultimate_item_domains(left_item=left_item, right_item=right_item)
            rank = (
                int(bool(left_item)) + int(bool(right_item)),
                min(domains["ip"], 1) + min(domains["build"], 1),
                item_rank(left_item) + item_rank(right_item),
                item_rank(left_item),
                item_rank(right_item),
            )
            if best_rank is None or rank > best_rank:
                best_rank = rank
                best_pair = (left_item, right_item)

    return best_pair


def count_ultimate_ip_stats_at_max(chicken):
    return sum(
        1
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(chicken, stat_name) >= 40
    )


def _ultimate_build_side_has_high_coverage_exception(side, partner, build_name):
    build_eval = _get_ultimate_build_eval_for(side, build_name)
    partner_eval = _get_ultimate_build_eval_for(partner, build_name)
    total = safe_int(build_eval.get("match_total"), default=0) or 0
    if total <= 0:
        total = safe_int(partner_eval.get("match_total"), default=0) or 0
    if total <= 0:
        return False

    side_count = safe_int(build_eval.get("match_count"), default=0) or 0
    partner_count = safe_int(partner_eval.get("match_count"), default=0) or 0
    return side_count >= max(0, total - 1) and partner_count >= total


def _ultimate_ip_side_has_high_coverage_exception(side, partner):
    return count_ultimate_ip_stats_at_max(side) >= 6 and count_ultimate_ip_stats_at_max(partner) >= 7


def _ultimate_side_missing_item_is_allowed(side, partner, build_name):
    return (
        _ultimate_build_side_has_high_coverage_exception(side, partner, build_name)
        or _ultimate_ip_side_has_high_coverage_exception(side, partner)
    )


def cap_ultimate_pair_quality_by_item_plan(quality, left, right, build_name, left_item=None, right_item=None):
    return quality


def has_low_ultimate_item_pressure(item_constraint_details):
    item_constraint_details = item_constraint_details or {}
    return (
        (safe_int(item_constraint_details.get("unmet_ip_item_need"), default=0) or 0) == 0
        and (safe_int(item_constraint_details.get("unmet_build_item_need"), default=0) or 0) == 0
        and (safe_int(item_constraint_details.get("unresolved_ip_count"), default=0) or 0) == 0
        and (safe_int(item_constraint_details.get("unresolved_build_count"), default=0) or 0) == 0
    )


def build_ultimate_pair_quality_from_items(left, right, build_name, left_item=None, right_item=None):
    combined_build = get_combined_build_coverage(left, right, build_name)
    build_metrics = combined_build["build_pair_metrics"]
    ip_metrics = build_ultimate_ip_metrics(left, right)
    build_priority_metrics = build_ultimate_build_priority_metrics(left, right, build_name)
    ip_priority_metrics = build_ultimate_ip_priority_metrics(left, right)
    ip_threshold_metrics = get_ultimate_ip_threshold_metrics(
        left,
        right,
        threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD,
    )
    ip_burden_metrics = get_ultimate_ip_burden_metrics(
        left,
        right,
        threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD,
    )

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
    raw_build_score = scores.get("raw_build_score", build_score)
    raw_ip_score = scores.get("raw_ip_score", ip_score)
    raw_total_score = raw_build_score + raw_ip_score
    item_constraint_details = scores.get("item_constraint_details") or {}
    low_item_pressure = has_low_ultimate_item_pressure(item_constraint_details)

    if (
        raw_total_score >= ULTIMATE_EXCELLENT_TOTAL_SCORE_THRESHOLD
        and raw_build_score >= ULTIMATE_EXCELLENT_BUILD_SCORE_THRESHOLD
        and raw_ip_score >= ULTIMATE_EXCELLENT_IP_SCORE_THRESHOLD
        and low_item_pressure
    ):
        return cap_ultimate_pair_quality_by_item_plan(
            "Excellent match",
            left,
            right,
            build_name,
            left_item=left_item,
            right_item=right_item,
        )

    if (
        raw_build_score >= ULTIMATE_STRONG_BUILD_SCORE_THRESHOLD
        and raw_ip_score >= ULTIMATE_STRONG_IP_SCORE_THRESHOLD
        and raw_total_score >= ULTIMATE_STRONG_TOTAL_SCORE_THRESHOLD
    ):
        return cap_ultimate_pair_quality_by_item_plan(
            "Strong match",
            left,
            right,
            build_name,
            left_item=left_item,
            right_item=right_item,
        )

    if (
        raw_total_score >= 560
        and (raw_build_score >= ULTIMATE_STRONG_BUILD_SCORE_THRESHOLD or raw_ip_score >= ULTIMATE_STRONG_IP_SCORE_THRESHOLD)
    ):
        return "Good match"

    if raw_total_score >= 420 or total_score >= 420:
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
    quality_rank = {
        "Excellent match": 0,
        "Strong match": 1,
        "Good match": 2,
        "Situational": 3,
        "Poor match": 4,
    }.get(
        build_ultimate_pair_quality_from_items(
            selected,
            candidate,
            build_name,
            left_item=left_item,
            right_item=right_item,
        ),
        99,
    )
    left_item_gap_count = min(2, len(get_ultimate_item_candidates(selected, candidate, build_name)))
    right_item_gap_count = min(2, len(get_ultimate_item_candidates(candidate, selected, build_name)))

    return (
        quality_rank,
        -left_item_gap_count,
        -right_item_gap_count,
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
    ip_threshold_metrics = get_ultimate_ip_threshold_metrics(
        selected,
        candidate,
        threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD,
    )
    ip_burden_metrics = get_ultimate_ip_burden_metrics(
        selected,
        candidate,
        threshold=EXCELLENT_CHICKEN_VALIDATION_THRESHOLD,
    )
    pair_score = compute_ultimate_pair_score(
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
    pair_quality = build_ultimate_pair_quality_from_items(
        selected,
        candidate,
        build_name,
        left_item=left_item,
        right_item=right_item,
    )

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
        "ultimate_pair_score": pair_score,
        "ultimate_pair_points": pair_score["total_score"],
        "ultimate_build_score": pair_score["build_score"],
        "ultimate_ip_score": pair_score["ip_score"],
        "raw_gene_score": pair_score["raw_build_score"],
        "raw_gene_grade": pair_score["raw_build_grade"],
        "raw_gene_note_display": pair_score["raw_build_note_display"],
        "raw_ip_score": pair_score["raw_ip_score"],
        "raw_ip_grade": pair_score["raw_ip_grade"],
        "item_constraint_details": pair_score["item_constraint_details"],
        "pair_quality": pair_quality,
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


def pick_best_ultimate_auto_match(breedable_chickens, include_lower_values=False):
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

        matches = filter_and_sort_ultimate_candidates(
            selected,
            candidate_pool,
            include_lower_values=include_lower_values,
        )
        if not matches:
            continue

        top = matches[0]
        ranking = top.get("ranking") or score_ultimate_candidate(selected, top)

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches


def build_ultimate_available_auto_candidates(
    breedable_chickens,
    ip_diff=None,
    breed_diff=None,
    ninuno_mode="all",
    include_lower_values=False,
    same_build=False,
):
    pair_rows = []

    for index, source in enumerate(breedable_chickens or []):
        if not chicken_passes_auto_ninuno_filter(source, ninuno_mode):
            continue

        source_build = str(source.get("ultimate_build_key") or source.get("build_type") or source.get("primary_build") or "").strip().lower()

        for candidate in (breedable_chickens or [])[index + 1:]:
            if not chicken_passes_auto_ninuno_filter(candidate, ninuno_mode):
                continue

            candidate_build = str(candidate.get("ultimate_build_key") or candidate.get("build_type") or candidate.get("primary_build") or "").strip().lower()
            if same_build and (not source_build or source_build != candidate_build):
                continue

            if ip_diff is not None:
                source_ip = safe_int(source.get("ip"), default=None)
                candidate_ip = safe_int(candidate.get("ip"), default=None)
                if source_ip is None or candidate_ip is None or abs(candidate_ip - source_ip) > ip_diff:
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

            forward = filter_and_sort_ultimate_candidates(
                source,
                [candidate],
                include_lower_values=include_lower_values,
            )
            reverse = filter_and_sort_ultimate_candidates(
                candidate,
                [source],
                include_lower_values=include_lower_values,
            )

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
                    "ultimate_pair_score": chosen_match.get("ultimate_pair_score") or {},
                    "ultimate_pair_points": chosen_match.get("ultimate_pair_points"),
                    "ultimate_build_score": chosen_match.get("ultimate_build_score"),
                    "ultimate_ip_score": chosen_match.get("ultimate_ip_score"),
                    "raw_gene_score": chosen_match.get("raw_gene_score"),
                    "raw_gene_grade": chosen_match.get("raw_gene_grade"),
                    "raw_gene_note_display": chosen_match.get("raw_gene_note_display"),
                    "raw_ip_score": chosen_match.get("raw_ip_score"),
                    "raw_ip_grade": chosen_match.get("raw_ip_grade"),
                    "item_constraint_details": chosen_match.get("item_constraint_details") or {},
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

def filter_and_sort_ultimate_candidates(selected, chickens, require_items=False, include_lower_values=False):
    rows = []

    for candidate in chickens or []:
        if str(candidate.get("token_id") or "") == str(selected.get("token_id") or ""):
            continue

        if is_parent_offspring(selected, candidate):
            continue

        if is_full_siblings(selected, candidate):
            continue

        if not is_generation_gap_allowed(selected, candidate, max_gap=3):
            continue

        if not include_lower_values and not is_ultimate_eligible(candidate):
            continue

        if not is_valid_ultimate_pair(selected, candidate):
            continue

        row = build_ultimate_candidate_row(selected, candidate)

        if require_items and (not row.get("left_item") or not row.get("right_item")):
            continue

        rows.append(row)

    rows.sort(key=lambda row: score_ultimate_candidate(selected, row))
    return rows
