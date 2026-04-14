ULTIMATE_BUILD_ORDER = ["killua", "shanks", "levi", "hybrid 2", "hybrid 1"]
ULTIMATE_TYPE_ORDER = ["both", "gene_only", "ip_only"]


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


def normalize_ultimate_available_ninuno_filter(value):
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

    return ""


def get_chicken_type_display(value):
    normalized = normalize_chicken_type_value(value)

    if normalized == "genesis":
        return "Genesis"
    if normalized == "legacy":
        return "Legacy"
    return "Ordinary"


def normalize_ultimate_type_value(value):
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")

    if raw in ULTIMATE_TYPE_ORDER:
        return raw

    return ""


def get_ultimate_type_display(value):
    normalized = normalize_ultimate_type_value(value)

    if normalized == "both":
        return "Both"
    if normalized == "gene_only":
        return "Gene Only"
    if normalized == "ip_only":
        return "IP Only"

    return ""


def normalize_ultimate_build_value(value):
    raw = str(value or "").strip().lower()
    return raw if raw in ULTIMATE_BUILD_ORDER else ""

def get_ultimate_build_compatibility(build_key):
    key = str(build_key or "").strip().lower()

    compatibility = {
        "killua": {"killua", "hybrid 1", "hybrid 2"},
        "shanks": {"shanks", "hybrid 1"},
        "levi": {"levi", "hybrid 1", "hybrid 2"},
        "hybrid 1": {"killua", "shanks", "levi", "hybrid 1"},
        "hybrid 2": {"killua", "levi", "hybrid 2"},
    }

    return compatibility.get(key, {key} if key else set())


def ultimate_available_builds_are_compatible(selected_build, chicken_build):
    selected_key = str(selected_build or "").strip().lower()
    chicken_key = str(chicken_build or "").strip().lower()

    if not selected_key or not chicken_key:
        return False

    selected_compatible = get_ultimate_build_compatibility(selected_key)
    chicken_compatible = get_ultimate_build_compatibility(chicken_key)

    return chicken_key in selected_compatible and selected_key in chicken_compatible

def get_ultimate_build_display(value):
    normalized = normalize_ultimate_build_value(value)
    return normalized.title() if normalized else ""


def enrich_ultimate_available_chicken_row(
    chicken,
    enrich_chicken_media,
    get_ultimate_type_display_fn,
    get_ultimate_build_display_fn,
    safe_int,
):
    row = enrich_chicken_media(dict(chicken or {}))

    type_normalized = normalize_chicken_type_value(row.get("type"))
    ultimate_type_key = normalize_ultimate_type_value(
        row.get("ultimate_type") or get_ultimate_type_display_fn(row)
    )
    ultimate_build_key = normalize_ultimate_build_value(
        row.get("primary_build") or row.get("ultimate_build_display")
    )

    primary_count = safe_int(row.get("primary_build_match_count"), 0) or 0
    primary_total = safe_int(row.get("primary_build_match_total"), 0) or 0

    row["type_normalized"] = type_normalized
    row["type_display"] = get_chicken_type_display(type_normalized)

    row["ultimate_type_key"] = ultimate_type_key
    row["ultimate_type_display"] = (
        get_ultimate_type_display_fn(row)
        or get_ultimate_type_display(ultimate_type_key)
    )

    row["ultimate_build_key"] = ultimate_build_key
    row["ultimate_build_display"] = (
        get_ultimate_build_display_fn(row)
        or get_ultimate_build_display(ultimate_build_key)
    )

    row["ultimate_build_match_count"] = primary_count
    row["ultimate_build_match_total"] = primary_total
    row["ultimate_build_match_display"] = (
        f"{primary_count}/{primary_total}" if primary_total else ""
    )

    # Compatibility aliases for downstream Ultimate flow
    row["build_type"] = row["ultimate_build_key"]
    row["build_label"] = row["ultimate_build_display"]
    row["build_match_count"] = row["ultimate_build_match_count"]
    row["build_match_total"] = row["ultimate_build_match_total"]

    return row

def build_ultimate_available_filter_options(rows, safe_int):
    rows = list(rows or [])

    type_options = []
    for value in ["ordinary", "legacy", "genesis"]:
        if any(str(row.get("type_normalized") or "") == value for row in rows):
            type_options.append({
                "value": value,
                "label": get_chicken_type_display(value),
            })

    build_options = []
    for value in ULTIMATE_BUILD_ORDER:
        if any(str(row.get("ultimate_build_key") or "") == value for row in rows):
            build_options.append({
                "value": value,
                "label": get_ultimate_build_display(value),
            })

    build_match_values = sorted(
        {
            safe_int(row.get("ultimate_build_match_count"))
            for row in rows
            if safe_int(row.get("ultimate_build_match_count")) is not None
        }
    )

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
        "build_options": build_options,
        "build_match_options": [
            {"value": str(value), "label": str(value)}
            for value in build_match_values
        ],
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


def chicken_matches_ultimate_available_filters(
    chicken,
    safe_int,
    selected_types=None,
    selected_build="all",
    selected_build_matches=None,
    min_ip=None,
    selected_generations=None,
    selected_breed_counts=None,
    ninuno_mode="all",
):
    selected_types = set(selected_types or [])
    selected_build_matches = set(selected_build_matches or [])
    selected_generations = set(selected_generations or [])
    selected_breed_counts = set(selected_breed_counts or [])
    ninuno_mode = normalize_ultimate_available_ninuno_filter(ninuno_mode)
    selected_build = normalize_ultimate_build_value(selected_build) or "all"

    if selected_types and str(chicken.get("type_normalized") or "") not in selected_types:
        return False

    if selected_build != "all":
        chicken_build = str(chicken.get("ultimate_build_key") or "").strip().lower()
        if not ultimate_available_builds_are_compatible(selected_build, chicken_build):
            return False

    build_match_value = safe_int(chicken.get("ultimate_build_match_count"))
    if selected_build_matches:
        if build_match_value is None or str(build_match_value) not in selected_build_matches:
            return False

    ip_value = safe_int(chicken.get("ip"))
    if min_ip is not None:
        if ip_value is None or ip_value < min_ip:
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


def build_ultimate_active_filters(
    selected_types=None,
    selected_build="all",
    selected_build_matches=None,
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

    selected_build = normalize_ultimate_build_value(selected_build)
    if selected_build:
        filters.append({
            "key": "build",
            "label": "Build",
            "value": get_ultimate_build_display(selected_build),
        })

    if selected_build_matches:
        filters.append({
            "key": "build_match",
            "label": "Build Count",
            "value": ", ".join(str(value) for value in selected_build_matches),
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

    ninuno_mode = normalize_ultimate_available_ninuno_filter(ninuno_mode)
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

def get_ultimate_type_rank(value):
    normalized = normalize_ultimate_type_value(value)

    if normalized == "both":
        return 0
    if normalized == "gene_only":
        return 1
    if normalized == "ip_only":
        return 2

    return 9


def get_ultimate_build_rank(value):
    normalized = normalize_ultimate_build_value(value)
    try:
        return ULTIMATE_BUILD_ORDER.index(normalized)
    except ValueError:
        return 999


def sort_key_build_match_value(row, sort_key_int):
    count_value = sort_key_int(row.get("ultimate_build_match_count"), 0)
    total_value = sort_key_int(row.get("ultimate_build_match_total"), 0)
    return count_value, total_value


def sort_ultimate_available_chickens(
    rows,
    sort_by="ultimate_type",
    sort_dir="asc",
    sort_key_int=None,
    sort_key_text=None,
):
    rows = list(rows or [])
    reverse = (sort_dir == "desc")

    if sort_key_int is None:
        def sort_key_int(value, default=0):
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

    if sort_key_text is None:
        def sort_key_text(value):
            return str(value or "").strip().lower()

    if sort_by == "token_id":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("token_id"), 999999999),
                -sort_key_int(row.get("ip"), 0),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "build":
        rows.sort(
            key=lambda row: (
                get_ultimate_build_rank(row.get("ultimate_build_key") or row.get("primary_build")),
                sort_key_build_match_value(row, sort_key_int),
                -sort_key_int(row.get("ip"), 0),
                sort_key_int(row.get("generation_num"), 999999),
                sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "build_match":
        rows.sort(
            key=lambda row: (
                sort_key_build_match_value(row, sort_key_int),
                -sort_key_int(row.get("ip"), 0),
                get_ultimate_build_rank(row.get("ultimate_build_key") or row.get("primary_build")),
                sort_key_int(row.get("generation_num"), 999999),
                sort_key_int(row.get("breed_count"), 999999),
                sort_key_int(row.get("token_id"), 999999999),
            ),
            reverse=reverse,
        )
        return rows

    if sort_by == "ip":
        rows.sort(
            key=lambda row: (
                sort_key_int(row.get("ip"), 0),
                sort_key_build_match_value(row, sort_key_int),
                get_ultimate_type_rank(row.get("ultimate_type_key") or row.get("ultimate_type")),
                get_ultimate_build_rank(row.get("ultimate_build_key") or row.get("primary_build")),
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
                get_ultimate_type_rank(row.get("ultimate_type_key") or row.get("ultimate_type")),
                get_ultimate_build_rank(row.get("ultimate_build_key") or row.get("primary_build")),
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
                get_ultimate_type_rank(row.get("ultimate_type_key") or row.get("ultimate_type")),
                get_ultimate_build_rank(row.get("ultimate_build_key") or row.get("primary_build")),
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
            get_ultimate_type_rank(row.get("ultimate_type_key") or row.get("ultimate_type")),
            get_ultimate_build_rank(row.get("ultimate_build_key") or row.get("primary_build")),
            sort_key_build_match_value(row, sort_key_int),
            -sort_key_int(row.get("ip"), 0),
            sort_key_int(row.get("generation_num"), 999999),
            sort_key_int(row.get("breed_count"), 999999),
            sort_key_int(row.get("token_id"), 999999999),
        ),
        reverse=reverse,
    )
    return rows
