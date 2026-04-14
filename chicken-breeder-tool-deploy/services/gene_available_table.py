GENE_BUILD_ORDER = ["killua", "shanks", "levi", "hybrid 2", "hybrid 1"]
GENE_BUILD_SOURCE_ORDER = ["primary", "recessive", "both"]


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


def normalize_gene_available_build_filter(value, build_order=None):
    allowed_builds = [str(item or "").strip().lower() for item in (build_order or []) if str(item or "").strip()]
    value = str(value or "").strip().lower()
    return value if value in allowed_builds else "all"


def normalize_gene_available_ninuno_filter(value):
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


def get_gene_build_display(value, build_order=None):
    allowed_builds = [str(item or "").strip().lower() for item in (build_order or []) if str(item or "").strip()]
    value = str(value or "").strip().lower()
    return value.title() if value in allowed_builds else ""


def normalize_gene_build_source_value(value):
    raw = str(value or "").strip().lower()
    if raw == "mixed":
        return "both"
    if raw in GENE_BUILD_SOURCE_ORDER:
        return raw
    return ""

def get_gene_build_source_display(value):
    normalized = normalize_gene_build_source_value(value)
    if normalized == "both":
        return "Both"
    return normalized.title() if normalized else ""

def get_gene_build_compatibility(build_key):
    key = str(build_key or "").strip().lower()

    compatibility = {
        "killua": {"killua", "hybrid 1", "hybrid 2"},
        "shanks": {"shanks", "hybrid 1"},
        "levi": {"levi", "hybrid 1", "hybrid 2"},
        "hybrid 1": {"killua", "shanks", "levi", "hybrid 1"},
        "hybrid 2": {"killua", "levi", "hybrid 2"},
    }

    return compatibility.get(key, {key} if key else set())


def gene_available_builds_are_compatible(selected_build, chicken_build):
    selected_key = str(selected_build or "").strip().lower()
    chicken_key = str(chicken_build or "").strip().lower()

    if not selected_key or not chicken_key:
        return False

    selected_compatible = get_gene_build_compatibility(selected_key)
    chicken_compatible = get_gene_build_compatibility(chicken_key)

    return chicken_key in selected_compatible and selected_key in chicken_compatible

def enrich_gene_available_chicken_row(
    chicken,
    enrich_chicken_media,
    get_best_available_gene_build_info,
    build_order=None,
):
    row = enrich_chicken_media(dict(chicken or {}))

    best_info = get_best_available_gene_build_info(row)
    gene_build_key = str(best_info.get("build_key") or "").strip().lower()
    gene_build_source = normalize_gene_build_source_value(best_info.get("source") or best_info.get("display_source"))
    gene_build_match_count = best_info.get("sort_match_count") or 0
    gene_build_match_total = best_info.get("sort_match_total") or 0

    row["type_normalized"] = normalize_chicken_type_value(row.get("type"))
    row["type_display"] = get_chicken_type_display(row.get("type_normalized"))

    row["gene_build_key"] = gene_build_key
    row["gene_build_display"] = get_gene_build_display(gene_build_key, build_order=build_order)
    row["gene_build_source"] = gene_build_source
    row["gene_build_source_display"] = get_gene_build_source_display(gene_build_source)
    row["gene_build_match_count"] = gene_build_match_count
    row["gene_build_match_total"] = gene_build_match_total
    row["gene_build_match_display"] = (
        f"{gene_build_match_count}/{gene_build_match_total}"
        if gene_build_match_total
        else ""
    )

    row["build_display"] = row["gene_build_display"]
    row["build_source_display"] = row["gene_build_source_display"]
    row["build_match_display"] = row["gene_build_match_display"]


    row["build_type"] = row["gene_build_key"]
    row["build_label"] = row["gene_build_display"]
    row["build_match_count"] = row["gene_build_match_count"]
    row["build_match_total"] = row["gene_build_match_total"]

    if row["gene_build_source"] == "primary":
        row["gene_sort_source_rank"] = 0
    elif row["gene_build_source"] == "both":
        row["gene_sort_source_rank"] = 1
    elif row["gene_build_source"] == "recessive":
        row["gene_sort_source_rank"] = 2
    else:
        row["gene_sort_source_rank"] = 99
    return row


def build_gene_available_filter_options(rows, safe_int, build_order=None):
    rows = list(rows or [])

    type_options = []
    for value in ["ordinary", "legacy", "genesis"]:
        if any(str(row.get("type_normalized") or "") == value for row in rows):
            type_options.append({"value": value, "label": get_chicken_type_display(value)})

    resolved_build_order = [
        str(item or "").strip().lower()
        for item in (build_order or [])
        if str(item or "").strip()
    ]

    available_builds = {
        str(row.get("gene_build_key") or "").strip().lower()
        for row in rows
        if str(row.get("gene_build_key") or "").strip()
    }

    build_options = [
        {"value": value, "label": get_gene_build_display(value, build_order=resolved_build_order)}
        for value in resolved_build_order
        if value in available_builds
    ]

    build_match_values = sorted({
        safe_int(row.get("gene_build_match_count"))
        for row in rows
        if safe_int(row.get("gene_build_match_count")) is not None and 2 <= safe_int(row.get("gene_build_match_count")) <= 7
    })

    build_source_options = []
    for value in GENE_BUILD_SOURCE_ORDER:
        if any(str(row.get("gene_build_source") or "") == value for row in rows):
            build_source_options.append({"value": value, "label": get_gene_build_source_display(value)})

    instinct_values = sorted({
        str(row.get("instinct") or "").strip()
        for row in rows
        if str(row.get("instinct") or "").strip()
    }, key=lambda item: item.lower())

    generation_values = sorted({
        safe_int(row.get("generation_num"))
        for row in rows
        if safe_int(row.get("generation_num")) is not None
    })

    breed_count_values = sorted({
        safe_int(row.get("breed_count"))
        for row in rows
        if safe_int(row.get("breed_count")) is not None
    })

    return {
        "type_options": type_options,
        "build_options": build_options,
        "build_match_options": [{"value": str(v), "label": str(v)} for v in build_match_values],
        "build_source_options": build_source_options,
        "instinct_options": [{"value": value, "label": value} for value in instinct_values],
        "generation_options": [{"value": str(v), "label": f"Gen {v}"} for v in generation_values],
        "breed_count_options": [{"value": str(v), "label": str(v)} for v in breed_count_values],
        "ninuno_options": [
            {"value": "all", "label": "All"},
            {"value": "100", "label": "100% only"},
            {"value": "gt0", "label": "Above 0%"},
        ],
    }


def chicken_matches_gene_available_filters(
    chicken,
    safe_int,
    selected_types=None,
    selected_build="all",
    selected_build_matches=None,
    selected_build_sources=None,
    selected_instincts=None,
    min_ip=None,
    selected_generations=None,
    selected_breed_counts=None,
    ninuno_mode="all",
    build_order=None,
):
    selected_types = set(selected_types or [])
    selected_build_matches = set(selected_build_matches or [])
    selected_build_sources = set(selected_build_sources or [])
    selected_instincts = {str(value or "").strip().lower() for value in (selected_instincts or []) if str(value or "").strip()}
    selected_generations = set(selected_generations or [])
    selected_breed_counts = set(selected_breed_counts or [])
    selected_build = normalize_gene_available_build_filter(selected_build, build_order=build_order)
    ninuno_mode = normalize_gene_available_ninuno_filter(ninuno_mode)

    if selected_types and str(chicken.get("type_normalized") or "") not in selected_types:
        return False

    chicken_build = str(chicken.get("gene_build_key") or "").strip().lower()
    if selected_build != "all" and not gene_available_builds_are_compatible(selected_build, chicken_build):
        return False

    build_match_value = safe_int(chicken.get("gene_build_match_count"))
    if selected_build_matches:
        if build_match_value is None or str(build_match_value) not in selected_build_matches:
            return False

    if selected_build_sources and str(chicken.get("gene_build_source") or "") not in selected_build_sources:
        return False

    instinct_value = str(chicken.get("instinct") or "").strip().lower()
    if selected_instincts and instinct_value not in selected_instincts:
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


def build_gene_active_filters(
    selected_types=None,
    selected_build="all",
    selected_build_matches=None,
    selected_build_sources=None,
    selected_instincts=None,
    min_ip=None,
    selected_generations=None,
    selected_breed_counts=None,
    ninuno_mode="all",
    build_order=None,
):
    filters = []

    if selected_types:
        filters.append({"key": "type", "label": "Type", "value": ", ".join(get_chicken_type_display(v) for v in selected_types)})

    selected_build = normalize_gene_available_build_filter(selected_build, build_order=build_order)
    if selected_build != "all":
        filters.append({
            "key": "build",
            "label": "Build",
            "value": get_gene_build_display(selected_build, build_order=build_order),
        })

    if selected_build_matches:
        filters.append({"key": "build_match", "label": "Build Match", "value": ", ".join(str(v) for v in selected_build_matches)})

    if selected_build_sources:
        filters.append({"key": "build_source", "label": "Build Source", "value": ", ".join(get_gene_build_source_display(v) for v in selected_build_sources)})

    if selected_instincts:
        filters.append({"key": "instinct", "label": "Instinct", "value": ", ".join(selected_instincts)})

    if min_ip is not None:
        filters.append({"key": "min_ip", "label": "Min IP", "value": str(min_ip)})

    if selected_generations:
        filters.append({"key": "generation", "label": "Generation", "value": ", ".join(f"Gen {v}" for v in selected_generations)})

    if selected_breed_counts:
        filters.append({"key": "breed_count", "label": "Breed Count", "value": ", ".join(str(v) for v in selected_breed_counts)})

    ninuno_mode = normalize_gene_available_ninuno_filter(ninuno_mode)
    ninuno_label = "All"
    if ninuno_mode == "100":
        ninuno_label = "100% only"
    elif ninuno_mode == "gt0":
        ninuno_label = "Above 0%"
    filters.append({"key": "ninuno", "label": "Ninuno", "value": ninuno_label})
    return filters


def sort_gene_available_chickens(
    rows,
    sort_by="build",
    sort_dir="asc",
    sort_key_int=None,
    sort_key_text=None,
    build_order=None,
):
    rows = list(rows or [])
    reverse = sort_dir == "desc"

    if sort_key_int is None:
        def sort_key_int(value, default=0):
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

    if sort_key_text is None:
        def sort_key_text(value):
            return str(value or "").strip().lower()

    resolved_build_order = [
        str(item or "").strip().lower()
        for item in (build_order or [])
        if str(item or "").strip()
    ]
    build_rank = {value: index for index, value in enumerate(resolved_build_order)}
    
    source_rank = {value: index for index, value in enumerate(GENE_BUILD_SOURCE_ORDER)}

    if sort_by == "token_id":
        rows.sort(key=lambda row: (
            sort_key_int(row.get("token_id"), 999999999),
            -sort_key_int(row.get("gene_build_match_count"), 0),
        ), reverse=reverse)
        return rows

    if sort_by == "build":
        rows.sort(key=lambda row: (
            build_rank.get(str(row.get("gene_build_key") or ""), 999),
            -sort_key_int(row.get("gene_build_match_count"), 0),
            source_rank.get(str(row.get("gene_build_source") or ""), 999),
            sort_key_int(row.get("breed_count"), 999999),
            -sort_key_int(row.get("ip"), 0),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    if sort_by == "build_match":
        rows.sort(key=lambda row: (
            sort_key_int(row.get("gene_build_match_count"), 0),
            build_rank.get(str(row.get("gene_build_key") or ""), 999),
            source_rank.get(str(row.get("gene_build_source") or ""), 999),
            sort_key_int(row.get("breed_count"), 999999),
            -sort_key_int(row.get("ip"), 0),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    if sort_by == "build_source":
        rows.sort(key=lambda row: (
            source_rank.get(str(row.get("gene_build_source") or ""), 999),
            -sort_key_int(row.get("gene_build_match_count"), 0),
            build_rank.get(str(row.get("gene_build_key") or ""), 999),
            sort_key_int(row.get("breed_count"), 999999),
            -sort_key_int(row.get("ip"), 0),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    if sort_by == "instinct":
        rows.sort(key=lambda row: (
            sort_key_text(row.get("instinct")),
            -sort_key_int(row.get("gene_build_match_count"), 0),
            build_rank.get(str(row.get("gene_build_key") or ""), 999),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    if sort_by == "generation":
        rows.sort(key=lambda row: (
            sort_key_int(row.get("generation_num"), 999999),
            sort_key_int(row.get("breed_count"), 999999),
            -sort_key_int(row.get("ip"), 0),
            build_rank.get(str(row.get("gene_build_key") or ""), 999),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    if sort_by == "breed_count":
        rows.sort(key=lambda row: (
            sort_key_int(row.get("breed_count"), 999999),
            sort_key_int(row.get("generation_num"), 999999),
            -sort_key_int(row.get("ip"), 0),
            build_rank.get(str(row.get("gene_build_key") or ""), 999),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    if sort_by == "ninuno":
        rows.sort(key=lambda row: (
            float(row.get("ownership_percent") or 0),
            int(bool(row.get("is_complete"))),
            -sort_key_int(row.get("gene_build_match_count"), 0),
            sort_key_int(row.get("token_id"), 999999999),
        ), reverse=reverse)
        return rows

    rows.sort(key=lambda row: (
        sort_key_int(row.get("ip"), 0),
        -sort_key_int(row.get("gene_build_match_count"), 0),
        build_rank.get(str(row.get("gene_build_key") or ""), 999),
        sort_key_int(row.get("token_id"), 999999999),
    ), reverse=reverse)
    return rows


def normalize_gene_available_source_values(values):
    normalized = []
    seen = set()

    for value in values or []:
        item = normalize_gene_build_source_value(value)
        if not item or item in seen:
            continue
        seen.add(item)
        normalized.append(item)

    return normalized
