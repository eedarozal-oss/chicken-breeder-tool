from services.build_utils import trait_matches_allowed_value
from services.builds_config import BUILD_RULES, TRAIT_SLOTS


def calculate_recessive_repeat_bonus(chicken, build_name):
    config = BUILD_RULES.get(build_name)
    if not config:
        return 0

    bonus = 0

    for slot in TRAIT_SLOTS:
        allowed_values = config["slots"].get(slot, [])
        if not allowed_values:
            continue

        h1_value = chicken.get(f"{slot}_h1")
        h2_value = chicken.get(f"{slot}_h2")
        h3_value = chicken.get(f"{slot}_h3")

        if not trait_matches_allowed_value(h1_value, allowed_values):
            continue

        h1_text = str(h1_value or "").strip().lower()
        h2_text = str(h2_value or "").strip().lower()
        h3_text = str(h3_value or "").strip().lower()

        if h1_text and h1_text == h2_text == h3_text:
            bonus += 3
        elif h1_text and (h1_text == h2_text or h1_text == h3_text):
            bonus += 2
        else:
            bonus += 1

    return bonus
