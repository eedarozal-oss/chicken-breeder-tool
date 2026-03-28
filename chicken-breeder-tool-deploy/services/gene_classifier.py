from services.primary_build_classifier import classify_primary_build
from services.recessive_build_classifier import classify_recessive_build


def classify_gene_profile(chicken):
    primary_result = classify_primary_build(chicken)

    if primary_result["primary_build"]:
        recessive_result = {
            "recessive_build": None,
            "recessive_build_match_count": 0,
            "recessive_build_match_total": 0,
            "recessive_build_matched_slots": [],
            "recessive_build_missing_slots": [],
            "recessive_build_repeat_bonus": 0,
            "recessive_build_evaluations": {},
        }
    else:
        recessive_result = classify_recessive_build(chicken)

    return {
        **primary_result,
        **recessive_result,
    }
