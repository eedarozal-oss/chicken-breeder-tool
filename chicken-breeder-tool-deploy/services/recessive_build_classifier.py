from services.build_utils import build_trait_map
from services.build_eval import evaluate_all_builds, select_qualified_build
from services.recessive_bonus import calculate_recessive_repeat_bonus


def classify_recessive_build(chicken):
    h1_traits = build_trait_map(chicken, suffix="_h1")
    evaluations = evaluate_all_builds(h1_traits)
    qualified = select_qualified_build(
        evaluations,
        min_matches=4,
        traits=h1_traits,
    )

    if not qualified:
        return {
            "recessive_build": None,
            "recessive_build_match_count": 0,
            "recessive_build_match_total": 0,
            "recessive_build_matched_slots": [],
            "recessive_build_missing_slots": [],
            "recessive_build_repeat_bonus": 0,
            "recessive_build_evaluations": evaluations,
        }

    return {
        "recessive_build": qualified["build"],
        "recessive_build_match_count": qualified["match_count"],
        "recessive_build_match_total": qualified["match_total"],
        "recessive_build_matched_slots": qualified["matched_slots"],
        "recessive_build_missing_slots": qualified["missing_slots"],
        "recessive_build_repeat_bonus": calculate_recessive_repeat_bonus(
            chicken,
            qualified["build"],
        ),
        "recessive_build_evaluations": evaluations,
    }
