def safe_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_parent_set(chicken):
    parent_1 = str(chicken.get("parent_1") or "").strip()
    parent_2 = str(chicken.get("parent_2") or "").strip()

    parents = set()
    if parent_1:
        parents.add(parent_1)
    if parent_2:
        parents.add(parent_2)

    return parents


def is_breedable_chicken(chicken):
    if not chicken:
        return False

    if chicken.get("is_egg"):
        return False

    if str(chicken.get("state") or "").strip().lower() != "normal":
        return False

    return True


def is_parent_offspring(chicken_a, chicken_b):
    a_id = str(chicken_a.get("token_id") or "").strip()
    b_id = str(chicken_b.get("token_id") or "").strip()

    a_parents = normalize_parent_set(chicken_a)
    b_parents = normalize_parent_set(chicken_b)

    if a_id and a_id in b_parents:
        return True

    if b_id and b_id in a_parents:
        return True

    return False


def is_full_siblings(chicken_a, chicken_b):
    a_parents = normalize_parent_set(chicken_a)
    b_parents = normalize_parent_set(chicken_b)

    if len(a_parents) < 2 or len(b_parents) < 2:
        return False

    return a_parents == b_parents


def get_generation_gap(chicken_a, chicken_b):
    gen_a = safe_int(chicken_a.get("generation_num"))
    gen_b = safe_int(chicken_b.get("generation_num"))

    if gen_a is None or gen_b is None:
        return None

    return abs(gen_a - gen_b)


def is_generation_gap_allowed(chicken_a, chicken_b, max_gap=3):
    gap = get_generation_gap(chicken_a, chicken_b)
    if gap is None:
        return False
    return gap <= max_gap


def get_ip_difference(chicken_a, chicken_b):
    ip_a = safe_int(chicken_a.get("ip"))
    ip_b = safe_int(chicken_b.get("ip"))

    if ip_a is None or ip_b is None:
        return None

    return abs(ip_a - ip_b)


def is_ip_difference_recommended(chicken_a, chicken_b, max_ip_diff=10):
    diff = get_ip_difference(chicken_a, chicken_b)
    if diff is None:
        return False
    return diff < max_ip_diff


def get_breed_count_difference(chicken_a, chicken_b):
    breed_a = safe_int(chicken_a.get("breed_count"))
    breed_b = safe_int(chicken_b.get("breed_count"))

    if breed_a is None or breed_b is None:
        return None

    return abs(breed_a - breed_b)


def is_breed_count_recommended(chicken_a, chicken_b, max_diff=1):
    diff = get_breed_count_difference(chicken_a, chicken_b)
    if diff is None:
        return False
    return diff <= max_diff


def evaluate_match(chicken_a, chicken_b, settings=None):
    """
    Hard block rules:
    - same chicken
    - either chicken not breedable
    - parent / offspring
    - full siblings
    - generation gap must be 3 or less

    Recommendation rules:
    - IP difference < configured threshold
    - breed count difference <= configured threshold
    """
    settings = settings or {}

    max_generation_gap = int(settings.get("max_generation_gap", 3))
    max_ip_diff = int(settings.get("max_ip_diff", 10))
    max_breed_count_diff = int(settings.get("max_breed_count_diff", 1))

    a_id = str(chicken_a.get("token_id") or "").strip()
    b_id = str(chicken_b.get("token_id") or "").strip()

    result = {
        "selected_token_id": a_id,
        "candidate_token_id": b_id,
        "is_allowed": True,
        "block_reason": None,
        "warnings": [],
        "generation_gap": get_generation_gap(chicken_a, chicken_b),
        "ip_difference": get_ip_difference(chicken_a, chicken_b),
        "breed_count_difference": get_breed_count_difference(chicken_a, chicken_b),
        "is_ip_recommended": False,
        "is_breed_count_recommended": False,
        "selected_roots_complete": bool(chicken_a.get("is_complete", 0)),
        "candidate_roots_complete": bool(chicken_b.get("is_complete", 0)),
    }

    if not a_id or not b_id:
        result["is_allowed"] = False
        result["block_reason"] = "Missing token ID"
        return result

    if a_id == b_id:
        result["is_allowed"] = False
        result["block_reason"] = "Same chicken"
        return result

    if not is_breedable_chicken(chicken_a) or not is_breedable_chicken(chicken_b):
        result["is_allowed"] = False
        result["block_reason"] = "One or both chickens are not breedable"
        return result

    if is_parent_offspring(chicken_a, chicken_b):
        result["is_allowed"] = False
        result["block_reason"] = "Parent and offspring pairing is forbidden"
        return result

    if is_full_siblings(chicken_a, chicken_b):
        result["is_allowed"] = False
        result["block_reason"] = "Full siblings pairing is forbidden"
        return result

    if not is_generation_gap_allowed(chicken_a, chicken_b, max_gap=max_generation_gap):
        result["is_allowed"] = False
        result["block_reason"] = f"Generation gap must be {max_generation_gap} or less"
        return result

    result["is_ip_recommended"] = is_ip_difference_recommended(
        chicken_a, chicken_b, max_ip_diff=max_ip_diff
    )
    result["is_breed_count_recommended"] = is_breed_count_recommended(
        chicken_a, chicken_b, max_diff=max_breed_count_diff
    )

    if not result["is_ip_recommended"]:
        result["warnings"].append(f"IP difference is not within preferred range (< {max_ip_diff})")

    if not result["is_breed_count_recommended"]:
        result["warnings"].append(
            f"Breed count difference is not within preferred range (<= {max_breed_count_diff})"
        )

    if not result["selected_roots_complete"]:
        result["warnings"].append("Selected chicken roots are incomplete")

    if not result["candidate_roots_complete"]:
        result["warnings"].append("Candidate chicken roots are incomplete")

    return result


def find_potential_matches(selected_chicken, candidate_chickens, settings=None):
    results = []

    for candidate in candidate_chickens or []:
        evaluation = evaluate_match(selected_chicken, candidate, settings=settings)

        if evaluation["is_allowed"]:
            results.append(
                {
                    "candidate": candidate,
                    "evaluation": evaluation,
                }
            )

    results.sort(
        key=lambda row: (
            len(row["evaluation"]["warnings"]),
            row["evaluation"]["ip_difference"] if row["evaluation"]["ip_difference"] is not None else 999999,
            row["evaluation"]["breed_count_difference"] if row["evaluation"]["breed_count_difference"] is not None else 999999,
            safe_int(row["candidate"].get("token_id")) or 0,
        )
    )

    return results
