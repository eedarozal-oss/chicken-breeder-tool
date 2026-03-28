from services.builds_config import TRAIT_SLOTS


def normalize_text(value):
    return str(value or "").strip().lower()


def trait_matches_allowed_value(trait_value, allowed_values):
    if not allowed_values:
        return False

    normalized_trait = normalize_text(trait_value)
    normalized_allowed = {normalize_text(value) for value in allowed_values}
    return normalized_trait in normalized_allowed


def build_trait_map(source, suffix=""):
    return {slot: source.get(f"{slot}{suffix}") for slot in TRAIT_SLOTS}
