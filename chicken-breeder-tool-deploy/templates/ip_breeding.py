from services.match_rules import find_potential_matches, get_ip_difference

MATCH_SETTINGS = {
    "max_generation_gap": 3,
    "max_ip_diff": 10,
    "max_breed_count_diff": 1,
}

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
    "innate_attack": (
        "Cocktail's Beak",
        "Best when this parent is contributing strong Attack inheritance.",
    ),
    "innate_defense": (
        "Pos2 Pellet",
        "Best when this parent is contributing strong Defense inheritance.",
    ),
    "innate_speed": (
        "Fetzzz Feet",
        "Best when this parent is contributing strong Speed inheritance.",
    ),
    "innate_health": (
        "Vananderen's Vitality",
        "Best when this parent is contributing strong Health inheritance.",
    ),
}

DUPLICATE_ALLOWED_ITEMS = {
    "Gregor's Gift",
    "Mendel's Memento",
    "Soulknot",
}


def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_effective_ip_stat(chicken, stat_name):
    chicken = chicken or {}

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


def get_top_base_stat_field(chicken):
    stat_fields = ["innate_attack", "innate_defense", "innate_speed", "innate_health"]
    ranked = []

    for field in stat_fields:
        ranked.append((field, safe_int((chicken or {}).get(field), 0) or 0))

    ranked.sort(key=lambda x: (-x[1], x[0]))
    return ranked


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

    other_below_threshold = get_below_threshold_stats(other_parent, 25)
    other_weakest = get_weakest_ip_stat_info(other_parent).get("name") or ""

    for priority_index, stat_name in enumerate(IP_STAT_PRIORITY):
        parent_value = get_effective_ip_stat(parent, stat_name)
        other_value = get_effective_ip_stat(other_parent, stat_name)

        if parent_value < 25:
            continue

        if parent_value <= other_value:
            continue

        fixes_threshold = stat_name in other_below_threshold and other_value < 25 and parent_value >= 25
        fixes_weakest = stat_name == other_weakest and other_value < 25 and parent_value >= 25

        threshold_bonus = 10000 if fixes_threshold else 0
        weakest_bonus = 20000 if fixes_weakest else 0

        advantage = parent_value - other_value
        priority_weight = len(IP_STAT_PRIORITY) - priority_index
        weighted_advantage = weakest_bonus + threshold_bonus + (advantage * 100) + priority_weight

        candidates.append(
            {
                "stat": stat_name,
                "name": IP_STAT_ITEM_NAMES[stat_name],
                "reason": get_ip_item_reason(stat_name),
                "parent_value": parent_value,
                "other_value": other_value,
                "advantage": advantage,
                "weighted_advantage": weighted_advantage,
                "fixes_threshold": fixes_threshold,
                "fixes_weakest": fixes_weakest,
                "is_single_target": True,
            }
        )

    candidates.sort(
        key=lambda item: (
            -int(bool(item["fixes_weakest"])),
            -int(bool(item["fixes_threshold"])),
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
            candidates.append(
                {
                    "name": item_name,
                    "reason": reason,
                }
            )

        broad_count = sum(1 for _, value in ranked if value >= 32)
        if broad_count >= 3 and candidates:
            candidates.append(
                {
                    "name": "Soulknot",
                    "reason": "Best when this parent is strong across several innate stats.",
                }
            )

        return candidates

    pair_candidates = get_ip_pair_stat_candidates(parent, other_parent)

    if pair_candidates:
        broad_count = sum(
            1
            for stat_name in IP_STAT_PRIORITY
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

        if broad_count >= 6:
            result.append(soulknot_item)

        return result

    return []


def recommend_ip_item(parent, other_parent=None):
    ranked = get_top_base_stat_field(parent)
    broad_count = sum(1 for _, value in ranked if value >= 32)

    if broad_count >= 6:
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


def pair_has_usable_ip_items(left_chicken, right_chicken):
    left_candidates = get_ip_item_candidates(left_chicken, right_chicken)
    right_candidates = get_ip_item_candidates(right_chicken, left_chicken)

    left_item, right_item = resolve_pair_item_recommendations(left_candidates, right_candidates)

    return bool(left_item) and bool(right_item)


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


def get_shared_ip_stats_at_or_above(left_chicken, right_chicken, threshold):
    return [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(left_chicken, stat_name) >= threshold
        and get_effective_ip_stat(right_chicken, stat_name) >= threshold
    ]


def get_left_only_ip_stats_at_or_above(left_chicken, right_chicken, threshold):
    return [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(left_chicken, stat_name) >= threshold
        and get_effective_ip_stat(right_chicken, stat_name) < threshold
    ]


def get_right_only_ip_stats_at_or_above(left_chicken, right_chicken, threshold):
    return [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(right_chicken, stat_name) >= threshold
        and get_effective_ip_stat(left_chicken, stat_name) < threshold
    ]


def build_ip_pair_metrics(left_chicken, right_chicken):
    left_chicken = left_chicken or {}
    right_chicken = right_chicken or {}

    shared_strong_stats = get_shared_ip_stats_at_or_above(left_chicken, right_chicken, 30)
    shared_usable_stats = get_shared_ip_stats_at_or_above(left_chicken, right_chicken, 25)
    left_edge_stats = get_left_only_ip_stats_at_or_above(left_chicken, right_chicken, 25)
    right_edge_stats = get_right_only_ip_stats_at_or_above(left_chicken, right_chicken, 25)
    combined_usable_stats = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if max(
            get_effective_ip_stat(left_chicken, stat_name),
            get_effective_ip_stat(right_chicken, stat_name),
        ) >= 25
    ]

    left_strong_count = count_ip_stats_at_or_above(left_chicken, 30)
    right_strong_count = count_ip_stats_at_or_above(right_chicken, 30)

    stronger_count = max(left_strong_count, right_strong_count)
    weaker_count = min(left_strong_count, right_strong_count)

    has_upgrade_path = len(combined_usable_stats) > max(
        count_ip_stats_at_or_above(left_chicken, 25),
        count_ip_stats_at_or_above(right_chicken, 25),
    )

    elite_stabilization = (
        len(shared_strong_stats) >= 5
        and stronger_count >= 5
        and weaker_count >= 5
    )

    anchor_finisher = (
        stronger_count >= 5
        and weaker_count >= 4
        and has_upgrade_path
    )

    overlap_score = (
        len(shared_strong_stats) * 100
        + len(shared_usable_stats) * 25
        + len(left_edge_stats) * 10
        + len(right_edge_stats) * 10
    )

    return {
        "shared_strong_stats": shared_strong_stats,
        "shared_strong_count": len(shared_strong_stats),
        "shared_usable_stats": shared_usable_stats,
        "shared_usable_count": len(shared_usable_stats),
        "left_edge_stats": left_edge_stats,
        "left_edge_count": len(left_edge_stats),
        "right_edge_stats": right_edge_stats,
        "right_edge_count": len(right_edge_stats),
        "edge_count": len(left_edge_stats) + len(right_edge_stats),
        "combined_usable_stats": combined_usable_stats,
        "combined_usable_count": len(combined_usable_stats),
        "left_strong_count": left_strong_count,
        "right_strong_count": right_strong_count,
        "stronger_count": stronger_count,
        "weaker_count": weaker_count,
        "has_upgrade_path": has_upgrade_path,
        "elite_stabilization": elite_stabilization,
        "anchor_finisher": anchor_finisher,
        "overlap_score": overlap_score,
    }


def get_ip_priority_metrics(selected_chicken, candidate):
    selected_chicken = selected_chicken or {}
    candidate = candidate or {}

    selected_weakest = get_weakest_ip_stat_info(selected_chicken)
    candidate_weakest = get_weakest_ip_stat_info(candidate)

    selected_priority_stat = selected_weakest.get("name") or ""
    candidate_priority_stat = candidate_weakest.get("name") or ""

    selected_priority_value = (
        get_effective_ip_stat(selected_chicken, selected_priority_stat)
        if selected_priority_stat else 0
    )
    candidate_on_selected_priority = (
        get_effective_ip_stat(candidate, selected_priority_stat)
        if selected_priority_stat else 0
    )

    candidate_priority_value = (
        get_effective_ip_stat(candidate, candidate_priority_stat)
        if candidate_priority_stat else 0
    )
    selected_on_candidate_priority = (
        get_effective_ip_stat(selected_chicken, candidate_priority_stat)
        if candidate_priority_stat else 0
    )

    selected_priority_resolved = (
        bool(selected_priority_stat)
        and selected_priority_value < 25
        and candidate_on_selected_priority >= 25
    )

    candidate_priority_resolved = (
        bool(candidate_priority_stat)
        and candidate_priority_value < 25
        and selected_on_candidate_priority >= 25
    )

    shared_unresolved_weakness = (
        bool(selected_priority_stat)
        and selected_priority_stat == candidate_priority_stat
        and max(selected_priority_value, candidate_priority_value) < 25
    )

    return {
        "selected_priority_stat": selected_priority_stat,
        "candidate_priority_stat": candidate_priority_stat,
        "selected_priority_value": selected_priority_value,
        "candidate_priority_value": candidate_priority_value,
        "candidate_on_selected_priority": candidate_on_selected_priority,
        "selected_on_candidate_priority": selected_on_candidate_priority,
        "selected_priority_resolved": selected_priority_resolved,
        "candidate_priority_resolved": candidate_priority_resolved,
        "mutual_priority_resolved": selected_priority_resolved and candidate_priority_resolved,
        "one_way_priority_resolved": selected_priority_resolved or candidate_priority_resolved,
        "shared_unresolved_weakness": shared_unresolved_weakness,
    }

def get_below_threshold_stats(chicken, threshold=25):
    return [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if get_effective_ip_stat(chicken, stat_name) < threshold
    ]


def count_fixed_below_threshold_stats(source, target, threshold=25):
    fixed = []

    for stat_name in IP_STAT_PRIORITY:
        target_value = get_effective_ip_stat(target, stat_name)
        source_value = get_effective_ip_stat(source, stat_name)

        if target_value < threshold and source_value >= threshold:
            fixed.append(stat_name)

    return fixed


def get_pair_threshold_metrics(left_chicken, right_chicken, threshold=25):
    left_below = get_below_threshold_stats(left_chicken, threshold)
    right_below = get_below_threshold_stats(right_chicken, threshold)

    left_fixes_right = count_fixed_below_threshold_stats(left_chicken, right_chicken, threshold)
    right_fixes_left = count_fixed_below_threshold_stats(right_chicken, left_chicken, threshold)

    combined_below_remaining = [
        stat_name
        for stat_name in IP_STAT_PRIORITY
        if max(
            get_effective_ip_stat(left_chicken, stat_name),
            get_effective_ip_stat(right_chicken, stat_name),
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

def get_ip_pair_burden_metrics(selected_chicken, candidate, threshold=25):
    selected_below = get_below_threshold_stats(selected_chicken, threshold)
    candidate_below = get_below_threshold_stats(candidate, threshold)

    return {
        "selected_below_count": len(selected_below),
        "candidate_below_count": len(candidate_below),
        "total_below_count": len(selected_below) + len(candidate_below),
    }

def rank_ip_pair(selected_chicken, candidate):
    metrics = build_ip_pair_metrics(selected_chicken, candidate)
    priority_metrics = get_ip_priority_metrics(selected_chicken, candidate)
    threshold_metrics = get_pair_threshold_metrics(selected_chicken, candidate, threshold=25)
    burden_metrics = get_ip_pair_burden_metrics(selected_chicken, candidate, threshold=25)

    candidate = candidate or {}
    selected_chicken = selected_chicken or {}

    return (
        int(bool(priority_metrics["shared_unresolved_weakness"])),
        -int(bool(threshold_metrics["all_threshold_gaps_resolved"])),
        -(threshold_metrics["right_fixes_left_count"] or 0),
        burden_metrics["candidate_below_count"] or 0,
        burden_metrics["total_below_count"] or 0,
        -(threshold_metrics["left_fixes_right_count"] or 0),
        threshold_metrics["combined_below_remaining_count"] or 0,
        -int(bool(metrics["elite_stabilization"])) if not priority_metrics["shared_unresolved_weakness"] else 0,
        -int(bool(metrics["anchor_finisher"])) if not priority_metrics["shared_unresolved_weakness"] else 0,
        -int(bool(priority_metrics["selected_priority_resolved"])),
        -(metrics["shared_strong_count"] or 0),
        -(metrics["shared_usable_count"] or 0),
        -(metrics["edge_count"] or 0),
        -(metrics["combined_usable_count"] or 0),
        -(priority_metrics["candidate_on_selected_priority"] or 0),
        safe_int(candidate.get("breed_count"), 999999) or 999999,
        -(float(candidate.get("ownership_percent") or 0)),
        safe_int(candidate.get("token_id"), 999999999) or 999999999,
        safe_int(selected_chicken.get("breed_count"), 999999) or 999999,
        -(float(selected_chicken.get("ownership_percent") or 0)),
        -(safe_int(selected_chicken.get("ip"), 0) or 0),
        safe_int(selected_chicken.get("token_id"), 999999999) or 999999999,
    )


def sort_ip_match_rows(selected_chicken, match_rows):
    rows = list(match_rows or [])

    def sort_key(row):
        evaluation = row.get("evaluation") or {}
        candidate = row.get("candidate") or {}

        is_ip_recommended = bool(evaluation.get("is_ip_recommended"))
        is_breed_count_recommended = bool(evaluation.get("is_breed_count_recommended"))
        is_clean_recommended = is_ip_recommended and is_breed_count_recommended

        return (
            -int(is_clean_recommended),
            -int(is_ip_recommended),
            -int(is_breed_count_recommended),
            *rank_ip_pair(selected_chicken, candidate),
        )

    rows.sort(key=sort_key)
    return rows


def build_ip_pair_quality(selected_chicken, candidate, row=None):
    if not selected_chicken or not candidate:
        return "Poor"

    metrics = build_ip_pair_metrics(selected_chicken, candidate)
    priority_metrics = get_ip_priority_metrics(selected_chicken, candidate)
    threshold_metrics = get_pair_threshold_metrics(selected_chicken, candidate, threshold=25)
    burden_metrics = get_ip_pair_burden_metrics(selected_chicken, candidate, threshold=25)
    ip_difference = get_ip_difference(selected_chicken, candidate)
    evaluation = (row or {}).get("evaluation") or {}

    if evaluation:
        if not evaluation.get("is_ip_recommended") or not evaluation.get("is_breed_count_recommended"):
            if evaluation.get("is_ip_recommended") or evaluation.get("is_breed_count_recommended"):
                return "Situational"
            return "Poor"

    if ip_difference is None:
        return "Poor"

    unresolved_load_too_high = (
        burden_metrics.get("selected_below_count", 0) >= 2
        or burden_metrics.get("candidate_below_count", 0) >= 2
    )

    if priority_metrics.get("shared_unresolved_weakness"):
        if (
            threshold_metrics.get("all_threshold_gaps_resolved")
            and threshold_metrics.get("mutual_fix_count", 0) >= 2
            and not unresolved_load_too_high
        ):
            return "Strong match"
        if threshold_metrics.get("right_fixes_left_count", 0) >= 1:
            return "Good match"
        return "Situational"

    if (
        metrics.get("elite_stabilization")
        and metrics.get("shared_strong_count", 0) >= 5
        and ip_difference < 10
        and threshold_metrics.get("combined_below_remaining_count", 0) == 0
        and not unresolved_load_too_high
    ):
        return "Excellent match"

    if (
        metrics.get("anchor_finisher")
        and threshold_metrics.get("right_fixes_left_count", 0) >= 1
        and threshold_metrics.get("left_fixes_right_count", 0) >= 1
        and threshold_metrics.get("combined_below_remaining_count", 0) == 0
        and ip_difference < 10
        and not unresolved_load_too_high
    ):
        return "Excellent match"

    if (
        threshold_metrics.get("right_fixes_left_count", 0) >= 1
        and threshold_metrics.get("combined_below_remaining_count", 0) <= 1
        and ip_difference <= 10
    ):
        return "Strong match"

    if (
        threshold_metrics.get("right_fixes_left_count", 0) >= 1
        or threshold_metrics.get("left_fixes_right_count", 0) >= 1
    ):
        return "Good match"

    if (
        metrics.get("shared_usable_count", 0) >= 2
        or metrics.get("edge_count", 0) >= 1
    ):
        return "Situational"

    return "Poor"


def pick_best_ip_auto_match(breedable_chickens, enable_ip_diff=False, ip_diff=None):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in breedable_chickens or []:
        selected_token_id = str(selected.get("token_id") or "")
        candidate_pool = [
            row
            for row in (breedable_chickens or [])
            if str(row.get("token_id") or "") != selected_token_id
        ]

        if enable_ip_diff and ip_diff is not None:
            selected_ip = safe_int(selected.get("ip"))
            if selected_ip is not None:
                candidate_pool = [
                    row
                    for row in candidate_pool
                    if safe_int(row.get("ip")) is not None
                    and abs((safe_int(row.get("ip")) or 0) - selected_ip) <= ip_diff
                ]

        matches = find_potential_matches(selected, candidate_pool, settings=MATCH_SETTINGS)
        matches = [
            row
            for row in matches
            if row.get("evaluation", {}).get("is_ip_recommended")
            and row.get("evaluation", {}).get("is_breed_count_recommended")
            and pair_has_usable_ip_items(selected, row.get("candidate"))
        ]
        matches = sort_ip_match_rows(selected, matches)

        if not matches:
            continue

        top = matches[0]
        ranking = rank_ip_pair(selected, top.get("candidate") or {})

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
    ownership = float((chicken or {}).get("ownership_percent") or 0)

    if mode == "100":
        return bool((chicken or {}).get("is_complete")) and ownership == 100.0
    if mode == "gt0":
        return ownership > 0
    return True


def get_chicken_build_key(chicken):
    return str(
        (chicken or {}).get("build_type")
        or (chicken or {}).get("gene_build_key")
        or (chicken or {}).get("primary_build")
        or ""
    ).strip().lower()


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
                row
                for row in find_potential_matches(source, [candidate], settings=MATCH_SETTINGS)
                if row.get("evaluation", {}).get("is_ip_recommended")
                and row.get("evaluation", {}).get("is_breed_count_recommended")
                and pair_has_usable_ip_items(source, row.get("candidate"))
            ]
            reverse = [
                row
                for row in find_potential_matches(candidate, [source], settings=MATCH_SETTINGS)
                if row.get("evaluation", {}).get("is_ip_recommended")
                and row.get("evaluation", {}).get("is_breed_count_recommended")
                and pair_has_usable_ip_items(candidate, row.get("candidate"))
            ]

            if forward:
                forward = sort_ip_match_rows(source, forward)
                chosen_left = dict(source)
                chosen_right = dict(candidate)
                chosen_match = forward[0]
            elif reverse:
                reverse = sort_ip_match_rows(candidate, reverse)
                chosen_left = dict(candidate)
                chosen_right = dict(source)
                chosen_match = reverse[0]
            else:
                continue

            weakest_info = get_weakest_ip_stat_info(chosen_left)
            chosen_left["weakest_stat_display"] = weakest_info["display"]

            selected_item_candidates = get_ip_item_candidates(chosen_left, chosen_right)
            right_item_candidates = get_ip_item_candidates(chosen_right, chosen_left)
            left_item, right_item = resolve_pair_item_recommendations(
                selected_item_candidates,
                right_item_candidates,
            )

            metrics = build_ip_pair_metrics(chosen_left, chosen_right)

            pair_rows.append(
                {
                    "left": chosen_left,
                    "right": chosen_right,
                    "left_item": left_item,
                    "right_item": right_item,
                    "selected_weakest_stat_display": (
                        f"{weakest_info['label']}: {get_effective_ip_stat(chosen_right, weakest_info['name'])}"
                        if weakest_info["name"]
                        else ""
                    ),
                    "ip_difference": chosen_match.get("evaluation", {}).get("ip_difference"),
                    "ip_overlap_metrics": metrics,
                    "ranking": rank_ip_pair(chosen_left, chosen_right),
                    "ip_priority_metrics": get_ip_priority_metrics(chosen_left, chosen_right),
                    "ip_threshold_metrics": get_pair_threshold_metrics(chosen_left, chosen_right, threshold=25),
                }
            )

    pair_rows.sort(key=lambda row: row["ranking"])
    return pair_rows


def pick_best_ip_auto_match_from_pool(pool, ip_diff=10, breed_diff=1):
    best_selected = None
    best_matches = []
    best_top = None

    for selected in pool or []:
        selected_token_id = str(selected.get("token_id") or "")
        candidate_pool = [
            row
            for row in (pool or [])
            if str(row.get("token_id") or "") != selected_token_id
        ]

        if ip_diff is not None:
            selected_ip = safe_int(selected.get("ip"))
            if selected_ip is not None:
                candidate_pool = [
                    row
                    for row in candidate_pool
                    if safe_int(row.get("ip")) is not None
                    and abs((safe_int(row.get("ip")) or 0) - selected_ip) <= ip_diff
                ]

        if breed_diff is not None:
            selected_breed = safe_int(selected.get("breed_count"))
            if selected_breed is not None:
                candidate_pool = [
                    row
                    for row in candidate_pool
                    if safe_int(row.get("breed_count")) is not None
                    and abs((safe_int(row.get("breed_count")) or 0) - selected_breed) <= breed_diff
                ]

        matches = find_potential_matches(selected, candidate_pool, settings=MATCH_SETTINGS)
        matches = [
            row
            for row in matches
            if row.get("evaluation", {}).get("is_ip_recommended")
            and row.get("evaluation", {}).get("is_breed_count_recommended")
            and pair_has_usable_ip_items(selected, row.get("candidate"))
        ]
        matches = sort_ip_match_rows(selected, matches)

        if not matches:
            continue

        top = matches[0]
        ranking = rank_ip_pair(selected, top.get("candidate") or {})

        if best_top is None or ranking < best_top:
            best_top = ranking
            best_selected = selected
            best_matches = matches

    return best_selected, best_matches


def build_ip_multi_matches(breedable_chickens, ip_diff=10, breed_diff=1, ninuno_filter="all", target_count=1, same_build=False):
    pool = list(breedable_chickens or [])
    results = []

    pool = [row for row in pool if chicken_passes_auto_ninuno_filter(row, ninuno_filter)]

    while len(pool) >= 2 and len(results) < target_count:
        ordered_pool = sorted(
            list(pool),
            key=lambda row: (
                -(safe_int(row.get("ip"), 0) or 0),
                safe_int(row.get("breed_count"), 999999) or 999999,
                -(float(row.get("ownership_percent") or 0)),
                safe_int(row.get("token_id"), 999999999) or 999999999,
            ),
        )
        selected = ordered_pool[0] if ordered_pool else None
        matches = []

        if selected:
            selected_token_id = str(selected.get("token_id") or "")
            selected_build = get_chicken_build_key(selected)
            if same_build and not selected_build:
                pool = [
                    row
                    for row in pool
                    if str(row.get("token_id") or "") != selected_token_id
                ]
                continue

            candidate_pool = [
                row
                for row in pool
                if str(row.get("token_id") or "") != selected_token_id
                and (
                    not same_build
                    or get_chicken_build_key(row) == selected_build
                )
            ]

            if ip_diff is not None:
                selected_ip = safe_int(selected.get("ip"))
                if selected_ip is not None:
                    candidate_pool = [
                        row
                        for row in candidate_pool
                        if safe_int(row.get("ip")) is not None
                        and abs((safe_int(row.get("ip")) or 0) - selected_ip) <= ip_diff
                    ]

            matches = find_potential_matches(selected, candidate_pool, settings=MATCH_SETTINGS)
            matches = [
                row
                for row in matches
                if row.get("evaluation", {}).get("is_ip_recommended")
                and row.get("evaluation", {}).get("is_breed_count_recommended")
                and pair_has_usable_ip_items(selected, row.get("candidate"))
            ]
            matches = sort_ip_match_rows(selected, matches)

        if not selected:
            break

        if not matches:
            used_selected_id = str(selected.get("token_id") or "")
            pool = [
                row
                for row in pool
                if str(row.get("token_id") or "") != used_selected_id
            ]
            continue

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

        filtered_matches = sort_ip_match_rows(selected, filtered_matches)

        if not filtered_matches:
            used_selected_id = str(selected.get("token_id") or "")
            pool = [
                row
                for row in pool
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

        results.append(
            {
                "left": selected,
                "right": candidate,
                "left_item": left_item,
                "right_item": right_item,
                "selected_weakest_stat_display": selected_weakest_stat_display,
                "ip_overlap_metrics": build_ip_pair_metrics(selected, candidate),
                "ip_priority_metrics": get_ip_priority_metrics(selected, candidate),
                "ip_threshold_metrics": get_pair_threshold_metrics(selected, candidate, threshold=25),
            }
        )

        used_ids = {
            str(selected.get("token_id") or ""),
            str(candidate.get("token_id") or ""),
        }
        pool = [
            row
            for row in pool
            if str(row.get("token_id") or "") not in used_ids
        ]

    return results
