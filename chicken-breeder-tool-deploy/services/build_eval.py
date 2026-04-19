from services.builds_config import BUILD_PRIORITY, BUILD_INSTINCT_TIERS, TRAIT_SLOTS, BUILD_RULES
from services.build_utils import trait_matches_allowed_value


def get_build_label(build_name):
    config = BUILD_RULES.get(build_name)
    return config["label"] if config else ""


def get_required_slots(build_name):
    config = BUILD_RULES.get(build_name)
    if not config:
        return []

    required_slots = []
    for slot in TRAIT_SLOTS:
        allowed = config["slots"].get(slot, [])
        if allowed:
            required_slots.append(slot)

    return required_slots


def get_required_slot_count(build_name):
    return len(get_required_slots(build_name))

MAIN_BUILD_UNIQUE_SLOTS = {
    "killua": ["beak", "tail", "feet", "body"],
    "shanks": ["beak", "wings", "tail", "feet", "body"],
    "levi": ["beak", "tail", "feet", "body"],
}


def has_unique_build_trait(traits, build_name):
    config = BUILD_RULES.get(build_name) or {}
    slots = (config.get("slots") or {})
    unique_slots = MAIN_BUILD_UNIQUE_SLOTS.get(str(build_name or "").strip().lower(), [])

    for slot in unique_slots:
        allowed = slots.get(slot, [])
        if not allowed:
            continue

        if trait_matches_allowed_value((traits or {}).get(slot), allowed):
            return True

    return False


def get_main_builds_with_unique_traits(traits):
    matched = []

    for build_name in ["killua", "shanks", "levi"]:
        if has_unique_build_trait(traits, build_name):
            matched.append(build_name)

    return matched


def normalize_instinct_name(value):
    return str(value or "").strip().lower()


def build_matches_instinct(instinct_name, build_name):
    build_key = str(build_name or "").strip().lower()
    instinct_key = normalize_instinct_name(instinct_name)
    if not instinct_key:
        return False
    return instinct_key in BUILD_INSTINCT_TIERS.get(build_key, [])


def get_instinct_tie_rank(instinct_name, build_name):
    return 0 if build_matches_instinct(instinct_name, build_name) else 1

def qualifies_for_hybrid_2(traits):
    hybrid_eval = evaluate_build(traits, "hybrid 2")
    return hybrid_eval.get("match_count", 0) == hybrid_eval.get("match_total", 0) and hybrid_eval.get("match_total", 0) > 0


def qualifies_for_hybrid_1(traits):
    hybrid_eval = evaluate_build(traits, "hybrid 1")
    return hybrid_eval.get("match_count", 0) == hybrid_eval.get("match_total", 0) and hybrid_eval.get("match_total", 0) > 0

def evaluate_build(traits, build_name):
    config = BUILD_RULES.get(build_name)
    if not config:
        return {
            "build": build_name,
            "label": "",
            "match_count": 0,
            "match_total": 0,
            "matched_slots": [],
            "missing_slots": [],
        }

    matched_slots = []
    missing_slots = []

    for slot in TRAIT_SLOTS:
        allowed = config["slots"].get(slot, [])
        if not allowed:
            continue

        if trait_matches_allowed_value(traits.get(slot), allowed):
            matched_slots.append(slot)
        else:
            missing_slots.append(slot)

    return {
        "build": build_name,
        "label": config["label"],
        "match_count": len(matched_slots),
        "match_total": len(matched_slots) + len(missing_slots),
        "matched_slots": matched_slots,
        "missing_slots": missing_slots,
    }


def evaluate_all_builds(traits):
    return {build_name: evaluate_build(traits, build_name) for build_name in BUILD_RULES}


def select_qualified_build(evaluations, min_matches, traits=None, instinct=None):
    traits = dict(traits or {})

    main_builds_with_unique = get_main_builds_with_unique_traits(traits)

    if main_builds_with_unique:
        qualified = []

        for build_name in main_builds_with_unique:
            result = evaluations.get(build_name)
            if not result:
                continue

            match_count = result.get("match_count", 0) or 0
            match_total = result.get("match_total", 0) or 0

            if match_count < min_matches or match_total <= 0:
                continue

            completion_ratio = match_count / match_total

            qualified.append({
                "build_name": build_name,
                "result": result,
                "match_count": match_count,
                "completion_ratio": completion_ratio,
                "instinct_tie_rank": get_instinct_tie_rank(instinct, build_name),
                "priority_index": BUILD_PRIORITY.index(build_name),
            })

        if qualified:
            qualified.sort(
                key=lambda item: (
                    -item["match_count"],
                    -item["completion_ratio"],
                    item["instinct_tie_rank"],
                    item["priority_index"],
                )
            )
            return qualified[0]["result"]

    if not main_builds_with_unique:
        if qualifies_for_hybrid_2(traits):
            result = evaluations.get("hybrid 2")
            if result and (result.get("match_count", 0) or 0) >= min_matches:
                return result

        if qualifies_for_hybrid_1(traits):
            result = evaluations.get("hybrid 1")
            if result and (result.get("match_count", 0) or 0) >= min_matches:
                return result

    return None


def count_added_missing_traits(selected_result, candidate_result):
    selected_missing = set((selected_result or {}).get("missing_slots", []))
    candidate_matched = set((candidate_result or {}).get("matched_slots", []))
    return len(selected_missing & candidate_matched)


def get_shared_matched_traits(left_result, right_result):
    left_matched = set((left_result or {}).get("matched_slots", []))
    right_matched = set((right_result or {}).get("matched_slots", []))
    return sorted(left_matched & right_matched)


def get_combined_matched_traits(left_result, right_result):
    left_matched = set((left_result or {}).get("matched_slots", []))
    right_matched = set((right_result or {}).get("matched_slots", []))
    return sorted(left_matched | right_matched)


def get_left_only_matched_traits(left_result, right_result):
    left_matched = set((left_result or {}).get("matched_slots", []))
    right_matched = set((right_result or {}).get("matched_slots", []))
    return sorted(left_matched - right_matched)


def get_right_only_matched_traits(left_result, right_result):
    left_matched = set((left_result or {}).get("matched_slots", []))
    right_matched = set((right_result or {}).get("matched_slots", []))
    return sorted(right_matched - left_matched)


def count_shared_matched_traits(left_result, right_result):
    return len(get_shared_matched_traits(left_result, right_result))


def count_combined_matched_traits(left_result, right_result):
    return len(get_combined_matched_traits(left_result, right_result))


def count_left_only_matched_traits(left_result, right_result):
    return len(get_left_only_matched_traits(left_result, right_result))


def count_right_only_matched_traits(left_result, right_result):
    return len(get_right_only_matched_traits(left_result, right_result))


def build_gene_pair_metrics(left_result, right_result):
    left_result = left_result or {}
    right_result = right_result or {}

    left_count = left_result.get("match_count", 0) or 0
    right_count = right_result.get("match_count", 0) or 0
    total = left_result.get("match_total", 0) or right_result.get("match_total", 0) or 0

    shared_slots = get_shared_matched_traits(left_result, right_result)
    combined_slots = get_combined_matched_traits(left_result, right_result)
    left_only_slots = get_left_only_matched_traits(left_result, right_result)
    right_only_slots = get_right_only_matched_traits(left_result, right_result)

    stronger_count = max(left_count, right_count)
    weaker_count = min(left_count, right_count)

    overlap_ratio = (len(shared_slots) / total) if total else 0.0
    has_upgrade_path = len(combined_slots) > stronger_count

    near_ceiling_threshold = max(0, total - 1)

    elite_stabilization = (
        total > 0
        and left_count >= near_ceiling_threshold
        and right_count >= near_ceiling_threshold
        and len(shared_slots) >= near_ceiling_threshold
    )

    anchor_finisher = (
        total > 0
        and stronger_count >= total
        and weaker_count >= near_ceiling_threshold
        and has_upgrade_path
    )

    return {
        "left_count": left_count,
        "right_count": right_count,
        "total": total,
        "shared_slots": shared_slots,
        "shared_count": len(shared_slots),
        "combined_slots": combined_slots,
        "combined_count": len(combined_slots),
        "left_only_slots": left_only_slots,
        "left_only_count": len(left_only_slots),
        "right_only_slots": right_only_slots,
        "right_only_count": len(right_only_slots),
        "edge_count": len(left_only_slots) + len(right_only_slots),
        "stronger_count": stronger_count,
        "weaker_count": weaker_count,
        "overlap_ratio": overlap_ratio,
        "has_upgrade_path": has_upgrade_path,
        "elite_stabilization": elite_stabilization,
        "anchor_finisher": anchor_finisher,
        "near_ceiling_threshold": near_ceiling_threshold,
    }
