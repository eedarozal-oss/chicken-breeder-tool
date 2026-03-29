from services.builds_config import BUILD_PRIORITY, TRAIT_SLOTS, BUILD_RULES
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


def select_qualified_build(evaluations, min_matches):
    qualified = []

    for build_name in BUILD_PRIORITY:
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
            "priority_index": BUILD_PRIORITY.index(build_name),
        })

    if not qualified:
        return None

    qualified.sort(
        key=lambda item: (
            -item["match_count"],       # higher raw matches first
            -item["completion_ratio"],  # then better fit
            item["priority_index"],     # then configured priority
        )
    )

    return qualified[0]["result"]

def count_added_missing_traits(selected_result, candidate_result):
    selected_missing = set(selected_result.get("missing_slots", []))
    candidate_matched = set(candidate_result.get("matched_slots", []))
    return len(selected_missing & candidate_matched)
