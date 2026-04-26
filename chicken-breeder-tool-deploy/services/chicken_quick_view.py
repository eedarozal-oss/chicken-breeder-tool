from services.build_eval import build_matches_instinct, evaluate_build, get_instinct_tie_rank
from services.builds_config import BUILD_PRIORITY
from services.build_utils import build_trait_map


STAT_FIELDS = [
    ("IP", "ip"),
    ("Attack", "innate_attack"),
    ("Defense", "innate_defense"),
    ("Speed", "innate_speed"),
    ("Health", "innate_health"),
    ("Ferocity", "innate_ferocity"),
    ("Cockrage", "innate_cockrage"),
    ("Evasion", "innate_evasion"),
]

TRAIT_LABEL_BY_SLOT = {
    "instinct": "Instinct",
    "beak": "Beak",
    "comb": "Comb",
    "eyes": "Eyes",
    "feet": "Feet",
    "wings": "Wings",
    "tail": "Tail",
    "body": "Body",
}

TRAIT_FIELDS = [
    ("Beak", "beak", "beak_h1"),
    ("Comb", "comb", "comb_h1"),
    ("Eyes", "eyes", "eyes_h1"),
    ("Feet", "feet", "feet_h1"),
    ("Wings", "wings", "wings_h1"),
    ("Tail", "tail", "tail_h1"),
    ("Body", "body", "body_h1"),
]


def _display_value(value):
    if value is None:
        return ""
    return str(value)


def _fallback_stat(chicken, field):
    if field == "innate_ferocity":
        return chicken.get("innate_attack")
    if field == "innate_cockrage":
        return chicken.get("innate_defense")
    if field == "innate_evasion":
        return chicken.get("innate_speed")
    return ""


def _build_stats(row):
    stats = []
    for label, field in STAT_FIELDS:
        value = row.get(field)
        if value in (None, ""):
            value = _fallback_stat(row, field)
        stats.append({
            "label": label,
            "value": _display_value(value),
            "tone": _stat_tone(value),
        })
    return stats


def _stat_tone(value):
    try:
        numeric_value = int(value)
    except (TypeError, ValueError):
        return ""

    if numeric_value < 25:
        return "low"
    if numeric_value == 40:
        return "max"
    return "mid"


def _build_traits(row):
    use_recessive_traits = row.get("build_source_display") == "Recessive"

    traits = [{"label": "Instinct", "value": _display_value(row.get("instinct"))}]
    for label, primary_field, recessive_field in TRAIT_FIELDS:
        field = recessive_field if use_recessive_traits else primary_field
        traits.append({"label": label, "value": _display_value(row.get(field))})
    return traits


def _title_value(value):
    text = _display_value(value).strip()
    return text.title() if text else ""


def _first_value(row, fields):
    for field in fields:
        value = _display_value(row.get(field)).strip()
        if value:
            return value
    return ""


def _raw_build_count_display(row):
    if row.get("build_source_display") == "Recessive":
        count = row.get("recessive_build_match_count")
        total = row.get("recessive_build_match_total")
    else:
        count = row.get("primary_build_match_count")
        total = row.get("primary_build_match_total")
        if total in (None, "", 0):
            count = row.get("build_match_count")
            total = row.get("build_match_total")
        if total in (None, "", 0):
            count = row.get("gene_build_match_count")
            total = row.get("gene_build_match_total")

    if total in (None, "", 0):
        return ""

    return f"{count or 0}/{total or 0}"


def _display_build_name(row):
    explicit = _first_value(row, ["ultimate_build_display", "build_display", "build_label", "gene_build_display"])
    if explicit:
        return explicit

    return _title_value(_first_value(row, ["ultimate_build_key", "build_type", "primary_build", "gene_build_key"]))


def _explicit_matched_slots(row):
    if row.get("build_source_display") == "Recessive" and row.get("recessive_build_matched_slots"):
        return list(row.get("recessive_build_matched_slots") or [])

    for field in [
        "ultimate_build_matched_slots",
        "build_matched_slots",
        "gene_build_matched_slots",
        "primary_build_matched_slots",
    ]:
        if row.get(field):
            return list(row.get(field) or [])

    return []


def _fallback_build_eval(row):
    trait_map = build_trait_map(row)
    best_eval = None
    best_rank = None

    for index, build_name in enumerate(BUILD_PRIORITY):
        result = evaluate_build(trait_map, build_name)
        match_count = result.get("match_count", 0) or 0
        match_total = result.get("match_total", 0) or 0
        if match_count <= 0 or match_total <= 0:
            continue

        rank = (
            -match_count,
            -(match_count / match_total),
            get_instinct_tie_rank(row.get("instinct"), build_name),
            index,
        )
        if best_rank is None or rank < best_rank:
            best_eval = result
            best_rank = rank

    return best_eval or {}


def _quick_build_info(row):
    explicit_count = _first_value(row, ["ultimate_build_match_display", "build_match_display", "gene_build_match_display"])
    raw_count = _raw_build_count_display(row)
    display = _display_build_name(row)
    matched_slots = _explicit_matched_slots(row)

    if display and (explicit_count or raw_count):
        return {
            "build_key": str(_first_value(row, ["ultimate_build_key", "build_type", "primary_build", "gene_build_key"]) or "").strip().lower(),
            "display": display,
            "count_display": explicit_count or raw_count,
            "matched_slots": matched_slots,
        }

    fallback_eval = _fallback_build_eval(row)
    if fallback_eval:
        return {
            "build_key": str(fallback_eval.get("build") or "").strip().lower(),
            "display": fallback_eval.get("label") or _title_value(fallback_eval.get("build")),
            "count_display": f"{fallback_eval.get('match_count', 0) or 0}/{fallback_eval.get('match_total', 0) or 0}",
            "matched_slots": list(fallback_eval.get("matched_slots") or []),
        }

    return {
        "build_key": str(_first_value(row, ["ultimate_build_key", "build_type", "primary_build", "gene_build_key"]) or "").strip().lower(),
        "display": display,
        "count_display": explicit_count or raw_count,
        "matched_slots": matched_slots,
    }


def build_chicken_quick_view(chicken, compare_chicken=None):
    row = dict(chicken or {})
    token_id = _display_value(row.get("token_id"))

    stats = _build_stats(row)
    traits = _build_traits(row)
    build_info = _quick_build_info(row)
    build_key = str(build_info.get("build_key") or "").strip().lower()
    matched_labels = {
        TRAIT_LABEL_BY_SLOT.get(str(slot or "").strip().lower(), "")
        for slot in build_info.get("matched_slots") or []
    }
    matched_labels = {label.strip().lower() for label in matched_labels if label}
    for item in traits:
        label = str(item.get("label") or "").strip().lower()
        item["is_match"] = bool(compare_chicken) and label in matched_labels

    image = row.get("image") or (f"https://chicken-api-ivory.vercel.app/api/image/{token_id}.png" if token_id else "")

    return {
        "token_id": token_id,
        "image": image,
        "instinct": _display_value(row.get("instinct")),
        "instinct_fit": build_matches_instinct(row.get("instinct"), build_key) if build_key else False,
        "build_display": build_info["display"],
        "build_count_display": build_info["count_display"],
        "stats": stats,
        "traits": traits,
    }
