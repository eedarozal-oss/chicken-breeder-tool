from services.gene_breeding import (
    build_gene_available_auto_candidates_same_build,
    build_gene_pair_quality,
)
from services.ip_breeding import (
    build_ip_available_auto_candidates,
    build_ip_pair_quality,
)
from services.ultimate_breeding import build_ultimate_available_auto_candidates


MODE_ORDER = ("ultimate", "gene", "ip")
MODE_LABELS = {
    "ultimate": "Ultimate",
    "gene": "Gene",
    "ip": "IP",
}
QUALITY_ORDER = ("excellent", "strong")
QUALITY_LABELS = {
    "excellent": "Excellent match",
    "strong": "Strong match",
}
MODE_CAP = 3
BONUS_MODE_CAP = 4
TOTAL_CAP = 10


def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_cost_preference(value):
    value = str(value or "prefer_low").strip().lower()
    if value in {"any", "prefer_low", "max_1", "max_2", "max_3"}:
        return value
    return "prefer_low"


def normalize_quality(value):
    value = str(value or "").strip().lower()
    if value.startswith("excellent"):
        return "excellent"
    if value.startswith("strong"):
        return "strong"
    return ""


def quality_label(quality_key):
    return QUALITY_LABELS.get(str(quality_key or "").strip().lower(), "")


def pair_key(left, right):
    left_id = str((left or {}).get("token_id") or "").strip()
    right_id = str((right or {}).get("token_id") or "").strip()
    if not left_id or not right_id:
        return ""
    ordered = sorted([left_id, right_id])
    return f"{ordered[0]}::{ordered[1]}"


def breed_count(row, side):
    return safe_int(((row.get(side) or {}).get("breed_count")), 999999)


def combined_breed_count(row):
    return breed_count(row, "left") + breed_count(row, "right")


def breed_count_difference(row):
    return abs(breed_count(row, "left") - breed_count(row, "right"))


def cost_preference_allows(row, cost_preference):
    cost_preference = normalize_cost_preference(cost_preference)
    if not cost_preference.startswith("max_"):
        return True

    max_breed = safe_int(cost_preference.rsplit("_", 1)[-1], None)
    if max_breed is None:
        return True

    return breed_count(row, "left") <= max_breed and breed_count(row, "right") <= max_breed


def cost_rank(row, cost_preference):
    ranking = tuple(row.get("ranking") or ())
    cost_preference = normalize_cost_preference(cost_preference)
    if cost_preference == "any":
        return (ranking, combined_breed_count(row), breed_count_difference(row))
    return (combined_breed_count(row), breed_count_difference(row), ranking)


def instinct_summary(left, right):
    left_instinct = str((left or {}).get("instinct") or "").strip()
    right_instinct = str((right or {}).get("instinct") or "").strip()
    if not left_instinct and not right_instinct:
        return ""
    if left_instinct and right_instinct and left_instinct.lower() == right_instinct.lower():
        return f"{left_instinct} match"
    return " / ".join([part for part in [left_instinct, right_instinct] if part])


def compact_chicken(chicken):
    row = dict(chicken or {})
    return {
        "token_id": str(row.get("token_id") or "").strip(),
        "image": row.get("image") or "",
        "ip": row.get("ip"),
        "breed_count": row.get("breed_count"),
        "instinct": row.get("instinct") or "",
        "build_display": row.get("build_display") or row.get("build_label") or row.get("primary_build") or "",
        "build_match_display": row.get("build_match_display") or row.get("ultimate_build_match_display") or "",
        "build_source_display": row.get("build_source_display") or "",
        "weakest_stat_display": row.get("weakest_stat_display") or "",
    }


def build_ultimate_summary(row):
    left = row.get("left") or {}
    right = row.get("right") or {}
    build_type = str(row.get("build_type") or "").strip().title()
    build_count = row.get("combined_build_count")
    build_total = row.get("combined_build_total")
    if build_count is None:
        build_count = (row.get("ultimate_build_metrics") or {}).get("combined_count")
    if build_total is None:
        build_total = (row.get("ultimate_build_metrics") or {}).get("total")

    build_display = f"{build_count}/{build_total}" if build_count is not None and build_total else ""
    if build_type and build_display:
        build_display = f"{build_display} {build_type}"
    elif build_type:
        build_display = build_type

    return [
        {"label": "IP", "value": row.get("combined_ip_total") or max(safe_int(left.get("ip")), safe_int(right.get("ip")))},
        {"label": "Build", "value": build_display},
        {"label": "Instinct", "value": instinct_summary(left, right)},
        {"label": "Breed", "value": f"{left.get('breed_count', '')} + {right.get('breed_count', '')}"},
    ]


def build_gene_summary(row):
    left = row.get("left") or {}
    right = row.get("right") or {}
    build_type = str(row.get("build_type") or "").strip().title()
    combined_count = row.get("combined_match_count")
    combined_total = row.get("combined_match_total")
    build_match = f"{combined_count}/{combined_total}" if combined_total else ""
    source = " / ".join(
        part for part in [
            str(left.get("build_source_display") or "").strip(),
            str(right.get("build_source_display") or "").strip(),
        ]
        if part
    )
    return [
        {"label": "Build", "value": build_type},
        {"label": "Match", "value": build_match},
        {"label": "Source", "value": source},
        {"label": "Instinct", "value": instinct_summary(left, right)},
        {"label": "Breed", "value": f"{left.get('breed_count', '')} + {right.get('breed_count', '')}"},
    ]


def build_ip_summary(row):
    left = row.get("left") or {}
    right = row.get("right") or {}
    metrics = row.get("ip_overlap_metrics") or {}
    support = row.get("selected_weakest_stat_display") or ""
    if not support:
        support = f"{metrics.get('shared_strong_count', 0)} shared strong stats"
    return [
        {"label": "IP", "value": f"{left.get('ip', '')} / {right.get('ip', '')}"},
        {"label": "Support", "value": support},
        {"label": "Instinct", "value": instinct_summary(left, right)},
        {"label": "Breed", "value": f"{left.get('breed_count', '')} + {right.get('breed_count', '')}"},
    ]


def decorate_candidate(row, mode, quality):
    row = dict(row or {})
    left = dict(row.get("left") or {})
    right = dict(row.get("right") or {})
    row["left"] = left
    row["right"] = right
    row["mode"] = mode
    row["mode_label"] = MODE_LABELS[mode]
    row["quality_key"] = quality
    row["pair_quality"] = quality_label(quality)
    row["pair_key"] = pair_key(left, right)
    row["result_id"] = f"{mode}-{row['pair_key'].replace('::', '-')}"
    row["combined_breed_count"] = combined_breed_count(row)
    row["breed_count_difference"] = breed_count_difference(row)
    row["left_compact"] = compact_chicken(left)
    row["right_compact"] = compact_chicken(right)

    if mode == "ultimate":
        row["summary"] = build_ultimate_summary(row)
    elif mode == "gene":
        row["summary"] = build_gene_summary(row)
    else:
        row["summary"] = build_ip_summary(row)

    return row


def build_mode_candidates(ultimate_pool, gene_pool, ip_pool, cost_preference):
    raw_by_mode = {
        "ultimate": build_ultimate_available_auto_candidates(
            ultimate_pool,
            ip_diff=None,
            breed_diff=None,
            ninuno_mode="all",
            include_lower_values=False,
            same_build=True,
        ),
        "gene": build_gene_available_auto_candidates_same_build(
            gene_pool,
            min_build_count=None,
            ip_diff=None,
            breed_diff=None,
            same_instinct=False,
            ninuno_mode="all",
            same_build=False,
        ),
        "ip": build_ip_available_auto_candidates(
            ip_pool,
            ip_diff=None,
            breed_diff=None,
            ninuno_mode="all",
            same_build=True,
        ),
    }

    grouped = {
        mode: {quality: [] for quality in QUALITY_ORDER}
        for mode in MODE_ORDER
    }

    for mode, rows in raw_by_mode.items():
        for row in rows or []:
            if not cost_preference_allows(row, cost_preference):
                continue

            if mode == "ultimate":
                quality = normalize_quality(row.get("pair_quality"))
            elif mode == "gene":
                quality = normalize_quality(build_gene_pair_quality(row))
            else:
                quality = normalize_quality(build_ip_pair_quality(row.get("left"), row.get("right"), row))

            if quality not in QUALITY_ORDER:
                continue

            grouped[mode][quality].append(decorate_candidate(row, mode, quality))

    for mode in MODE_ORDER:
        for quality in QUALITY_ORDER:
            grouped[mode][quality].sort(key=lambda row: cost_rank(row, cost_preference))

    return grouped


def _take_next_available_candidate(rows, cursor, used_token_ids):
    while cursor < len(rows):
        row = rows[cursor]
        cursor += 1
        left_id = str((row.get("left") or {}).get("token_id") or "").strip()
        right_id = str((row.get("right") or {}).get("token_id") or "").strip()
        if not left_id or not right_id:
            continue
        if left_id in used_token_ids or right_id in used_token_ids:
            continue
        return row, cursor
    return None, cursor


def _append_selected_candidate(selected, counts, used_token_ids, row):
    selected.append(row)
    mode = str(row.get("mode") or "").strip().lower()
    if mode in counts:
        counts[mode] += 1
    used_token_ids.add(str((row.get("left") or {}).get("token_id") or "").strip())
    used_token_ids.add(str((row.get("right") or {}).get("token_id") or "").strip())


def rotating_select_candidates(grouped_candidates):
    selected = []
    used_token_ids = set()
    counts = {mode: 0 for mode in MODE_ORDER}
    cursors = {
        mode: {quality: 0 for quality in QUALITY_ORDER}
        for mode in MODE_ORDER
    }

    for quality in QUALITY_ORDER:
        while True:
            added_this_round = False

            for mode in MODE_ORDER:
                if counts[mode] >= MODE_CAP:
                    continue
                if len(selected) >= TOTAL_CAP:
                    return selected

                rows = grouped_candidates.get(mode, {}).get(quality, [])
                cursor = cursors[mode][quality]
                chosen, cursor = _take_next_available_candidate(rows, cursor, used_token_ids)
                cursors[mode][quality] = cursor

                if not chosen:
                    continue

                _append_selected_candidate(selected, counts, used_token_ids, chosen)
                added_this_round = True

            if not added_this_round:
                break

    if len(selected) >= TOTAL_CAP:
        return selected

    # Bonus pass keeps the normal 3-per-mode balance first, then checks the
    # remaining Excellent/Strong pool by the same mode rotation up to 10 total.
    for quality in QUALITY_ORDER:
        while len(selected) < TOTAL_CAP:
            added_this_round = False

            for mode in MODE_ORDER:
                if counts[mode] >= BONUS_MODE_CAP:
                    continue
                if len(selected) >= TOTAL_CAP:
                    return selected

                rows = grouped_candidates.get(mode, {}).get(quality, [])
                cursor = cursors[mode][quality]
                chosen, cursor = _take_next_available_candidate(rows, cursor, used_token_ids)
                cursors[mode][quality] = cursor

                if not chosen:
                    continue

                _append_selected_candidate(selected, counts, used_token_ids, chosen)
                added_this_round = True
                break

            if not added_this_round:
                break

    return selected


def build_best_pair_suggestions(ultimate_pool=None, gene_pool=None, ip_pool=None, cost_preference="prefer_low"):
    cost_preference = normalize_cost_preference(cost_preference)
    grouped = build_mode_candidates(
        ultimate_pool or [],
        gene_pool or [],
        ip_pool or [],
        cost_preference,
    )
    selected = rotating_select_candidates(grouped)
    counts = {mode: 0 for mode in MODE_ORDER}
    for row in selected:
        mode = str(row.get("mode") or "").strip().lower()
        if mode in counts:
            counts[mode] += 1
    return {
        "pairs": selected,
        "counts": counts,
        "cost_preference": cost_preference,
    }
