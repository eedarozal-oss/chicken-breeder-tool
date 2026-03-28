from services.primary_build_classifier import safe_int
from services.build_eval import evaluate_build


ULTIMATE_TYPES = {"ip_only", "gene_only", "both"}


def get_ultimate_type(chicken):
    return str((chicken or {}).get("ultimate_type") or "").strip().lower()


def get_primary_build(chicken):
    return str((chicken or {}).get("primary_build") or "").strip().lower()


def is_ultimate_eligible(chicken):
    return get_ultimate_type(chicken) in ULTIMATE_TYPES


def get_ultimate_type_display(chicken):
    ultimate_type = get_ultimate_type(chicken)

    if ultimate_type == "ip_only":
        return "IP Only"
    if ultimate_type == "gene_only":
        return "Gene Only"
    if ultimate_type == "both":
        return "Both"

    return ""


def get_ultimate_build_display(chicken):
    build_name = get_primary_build(chicken)
    if not build_name:
        return ""
    return build_name.title()


def is_valid_ultimate_pair(selected, candidate):
    selected_type = get_ultimate_type(selected)
    candidate_type = get_ultimate_type(candidate)

    if not selected_type or not candidate_type:
        return False

    if selected_type == "ip_only":
        return candidate_type in {"gene_only", "both"}

    if selected_type == "gene_only":
        if candidate_type == "ip_only":
            return True
        if candidate_type == "both":
            return get_primary_build(selected) == get_primary_build(candidate)
        return False

    if selected_type == "both":
        if candidate_type == "ip_only":
            return True
        if candidate_type in {"gene_only", "both"}:
            return get_primary_build(selected) == get_primary_build(candidate)
        return False

    return False


def get_selected_target_build(selected, candidate):
    selected_build = get_primary_build(selected)
    if selected_build:
        return selected_build

    candidate_build = get_primary_build(candidate)
    if candidate_build:
        return candidate_build

    return ""


def get_build_eval_for_chicken(chicken, build_name):
    if not build_name:
        return {
            "matched_slots": [],
            "missing_slots": [],
        }

    return evaluate_build(chicken, build_name)


def count_build_complement(selected, candidate):
    target_build = get_selected_target_build(selected, candidate)
    if not target_build:
        return 0

    selected_eval = get_build_eval_for_chicken(selected, target_build)
    candidate_eval = get_build_eval_for_chicken(candidate, target_build)

    selected_missing = set(selected_eval.get("missing_slots", []))
    candidate_matched = set(candidate_eval.get("matched_slots", []))

    return len(selected_missing & candidate_matched)


def score_ultimate_candidate(selected, candidate):
    candidate_type = get_ultimate_type(candidate)
    candidate_ip = safe_int(candidate.get("ip"), default=0)
    candidate_breed_count = safe_int(candidate.get("breed_count"), default=999999)
    candidate_ninuno = float(candidate.get("ownership_percent") or 0)

    return (
        -count_build_complement(selected, candidate),
        0 if candidate_type == "both" else 1,
        -candidate_ip,
        candidate_breed_count,
        -candidate_ninuno,
        safe_int(candidate.get("token_id"), default=999999999),
    )


def build_ultimate_candidate_row(selected, candidate):
    return {
        "candidate": candidate,
        "ultimate_type_display": get_ultimate_type_display(candidate),
        "ultimate_build_display": get_ultimate_build_display(candidate),
        "build_complement": count_build_complement(selected, candidate),
    }


def filter_and_sort_ultimate_candidates(selected, chickens):
    rows = []

    for candidate in chickens or []:
        if str(candidate.get("token_id")) == str(selected.get("token_id")):
            continue

        if not is_ultimate_eligible(candidate):
            continue

        if not is_valid_ultimate_pair(selected, candidate):
            continue

        rows.append(build_ultimate_candidate_row(selected, candidate))

    rows.sort(key=lambda row: score_ultimate_candidate(selected, row["candidate"]))
    return rows
