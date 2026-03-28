from services.build_utils import build_trait_map
from services.build_eval import evaluate_all_builds, select_qualified_build


def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def determine_ultimate_type(ip_value, primary_build):
    ip_num = safe_int(ip_value, default=0)
    has_primary_build = bool(primary_build)

    if ip_num >= 260 and has_primary_build:
        return "both"
    if ip_num >= 260 and not has_primary_build:
        return "ip_only"
    if ip_num < 260 and has_primary_build:
        return "gene_only"

    return None


def classify_primary_build(chicken):
    primary_traits = build_trait_map(chicken)
    evaluations = evaluate_all_builds(primary_traits)
    qualified = select_qualified_build(evaluations, min_matches=5)

    if not qualified:
        return {
            "primary_build": None,
            "primary_build_match_count": 0,
            "primary_build_match_total": 0,
            "primary_build_matched_slots": [],
            "primary_build_missing_slots": [],
            "primary_build_evaluations": evaluations,
            "ultimate_type": determine_ultimate_type(chicken.get("ip"), None),
        }

    return {
        "primary_build": qualified["build"],
        "primary_build_match_count": qualified["match_count"],
        "primary_build_match_total": qualified["match_total"],
        "primary_build_matched_slots": qualified["matched_slots"],
        "primary_build_missing_slots": qualified["missing_slots"],
        "primary_build_evaluations": evaluations,
        "ultimate_type": determine_ultimate_type(
            chicken.get("ip"),
            qualified["build"],
        ),
    }
