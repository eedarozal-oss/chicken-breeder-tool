from services.build_eval import evaluate_build  # safe to keep if later needed; remove if unused


def parse_csv_query_values(raw_value):
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return []

    values = []
    seen = set()

    for part in raw_text.split(","):
        value = str(part or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)

    return values


def normalize_ip_available_ninuno_filter(value):
    value = str(value or "").strip().lower()
    if value in {"all", "100", "gt0"}:
        return value
    if value in {"1", "true", "on", "yes"}:
        return "100"
    return "all"


def normalize_chicken_type_value(value):
    raw = str(value or "").strip().lower()

    if raw in {"ordinary", "legacy", "genesis"}:
        return raw

    if raw in {"gen 0", "gen0"}:
        return "genesis"

    return "ordinary"


def get_chicken_type_display(value):
    normalized = normalize_chicken_type_value(value)

    if normalized == "genesis":
        return "Genesis"
    if normalized == "legacy":
        return "Legacy"
    return "Ordinary"


def enrich_ip_available_chicken_row(chicken, enrich_chicken_media, get_weakest_ip_stat_info):
    row = enrich_chicken_media(dict(chicken or {}))

    weakest_info = get_weakest_ip_stat_info(row)
    row["weakest_stat_name"] = weakest_info.get("name") or ""
    row["weakest_stat_label"] = weakest_info.get("label") or ""
    row["weakest_stat_value"] = weakest_info.get("value")
    row["weakest_stat_display"] = weakest_info.get("display") or ""

    normalized_type = normalize_chicken_type_value(row.get("type"))
    row["type_normalized"] = normalized_type
    row["type_display"] = get_chicken_type_display(normalized_type)

    return row


def build_ip_available_filter_options(rows, safe_int):
    rows = list(rows or [])

    type_options = []
    for value in ["ordinary", "legacy", "genesis"]:
        if any(str(row.get("type_normalized") or "") == value for row in rows):
            type_options.append({
                "value": value,
                "label": get_chicken_type_display(value),
            })

    generation_values = sorted(
        {
            safe_int(row.get("generation_num"))
            for row in rows
            if safe_int(row.get("generation_num")) is not None
        }
    )

    breed_count_values = sorted(
        {
            safe_int(row.get("breed_count"))
            for row in rows
            if safe_int(row.get("breed_count")) is not None
        }
    )

    return {
        "type_options": type_options,
        "generation_options": [
            {"value": str(value), "label": f"Gen {value}"}
            for value in generation_values
        ],
        "breed_count_options": [
            {"value": str(value), "label": str(value)}
            for value in breed_count_values
        ],
        "ninuno_options": [
            {"value": "all", "label": "All"},
            {"value": "100", "label": "100% only"},
            {"value": "gt0", "label": "Above 0%"},
        ],
    }


def chicken_matches_ip_available_filters(
    chicken,
    safe_int,
    selected_types=None,
    selected_generations=None,
    selected_breed_counts=None,
    ninuno_mode="all",
):
    selected_types = set(selected_types or [])
    selected_generations = set(selected_generations or [])
    selected_breed_counts = set(selected_breed_counts or [])
    ninuno_mode = normalize_ip_available_ninuno_filter(ninuno_mode)

    if selected_types and str(chicken.get("type_normalized") or "") not in selected_types:
        return False

    generation_value = safe_int(chicken.get("generation_num"))
    if selected_generations:
        if generation_value is None or str(generation_value) not in selected_generations:
            return False

    breed_count_value = safe_int(chicken.get("breed_count"))
    if selected_breed_counts:
        if breed_count_value is None or str(breed_count_value) not in selected_breed_counts:
            return False

    ownership = float(chicken.get("ownership_percent") or 0)

    if ninuno_mode == "100":
        return bool(chicken.get("is_complete")) and ownership == 100.0

    if ninuno_mode == "gt0":
        return ownership > 0

    return True


def build_ip_active_filters(
    selected_types=None,
    min_ip=None,
    selected_generations=None,
    selected_breed_counts=None,
    ninuno_mode="all",
):
    filters = []

    if selected_types:
        filters.append({
            "key": "type",
            "label": "Type",
            "value": ", ".join(get_chicken_type_display(value) for value in selected_types),
        })

    if min_ip is not None:
        filters.append({
            "key": "min_ip",
            "label": "Min IP",
            "value": str(min_ip),
        })

    if selected_generations:
        filters.append({
            "key": "generation",
            "label": "Generation",
            "value": ", ".join(f"Gen {value}" for value in selected_generations),
        })

    if selected_breed_counts:
        filters.append({
            "key": "breed_count",
            "label": "Breed Count",
            "value": ", ".join(str(value) for value in selected_breed_counts),
        })

    ninuno_mode = normalize_ip_available_ninuno_filter(ninuno_mode)
    if ninuno_mode == "100":
        ninuno_label = "100% only"
    elif ninuno_mode == "gt0":
        ninuno_label = "Above 0%"
    else:
        ninuno_label = "All"

    filters.append({
        "key": "ninuno",
        "label": "Ninuno",
        "value": ninuno_label,
    })
    return filters


def sort_ip_available_chickens(rows, sort_by="ip", sort_dir="desc", sort_key_int=None, sort_key_text=None):
    reverse = (sort_dir == "desc")
    rows = list(rows or [])

    if sort_by == "token_id":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("token_id"), 999999999),
                -sort_key_int(row.get("ip"), 0),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "weakest_stat":
        rows.sort(
            key=lambda row: (
                sort_key_text(row.get("weakest_stat_name")),
                sort_key_int(row.get("weakest_stat_value"), 999999),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "generation":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("generation_num"), 999999),
                sort_key_int(row.get("breed_count"), 999999),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "breed_count":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("generation_num"), 999999),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "ninuno":
        rows.sort(
            key=lambda row: (
                float(row.get("ownership_percent") or 0),
                int(bool(row.get("is_complete"))),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return rows

    rows.sort(
        key=lambda row: (
            sort_key_int(row.get("ip"), 0),
            -sort_key_int(row.get("breed_count"), 999999),
            -sort_key_int(row.get("generation_num"), 999999),
            -sort_key_int(row.get("token_id"), 999999999),
        ),
        reverse=reverse,
    )
    return rows
