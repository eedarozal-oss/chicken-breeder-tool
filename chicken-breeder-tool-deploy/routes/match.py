from flask import redirect, render_template, request, session, url_for


def register_match_routes(app, deps):
    build_gene_active_filters = deps["build_gene_active_filters"]
    build_gene_available_auto_candidates_same_build = deps["build_gene_available_auto_candidates_same_build"]
    build_gene_available_filter_options = deps["build_gene_available_filter_options"]
    build_gene_match_empty_state = deps["build_gene_match_empty_state"]
    build_gene_potential_matches_strict = deps["build_gene_potential_matches_strict"]
    build_ip_active_filters = deps["build_ip_active_filters"]
    build_ip_available_filter_options = deps["build_ip_available_filter_options"]
    build_ip_multi_matches = deps["build_ip_multi_matches"]
    build_planner_summary = deps["build_planner_summary"]
    build_ultimate_active_filters = deps["build_ultimate_active_filters"]
    build_ultimate_available_auto_candidates = deps["build_ultimate_available_auto_candidates"]
    build_ultimate_available_empty_state = deps["build_ultimate_available_empty_state"]
    build_ultimate_available_filter_options = deps["build_ultimate_available_filter_options"]
    build_ultimate_match_empty_state = deps["build_ultimate_match_empty_state"]
    build_wallet_summary = deps["build_wallet_summary"]
    chicken_matches_gene_available_filters = deps["chicken_matches_gene_available_filters"]
    chicken_matches_ip_available_filters = deps["chicken_matches_ip_available_filters"]
    chicken_matches_ultimate_available_filters = deps["chicken_matches_ultimate_available_filters"]
    chicken_passes_auto_ninuno_filter = deps["chicken_passes_auto_ninuno_filter"]
    complete_ninuno_via_lineage_with_resume = deps["complete_ninuno_via_lineage_with_resume"]
    enrich_chicken_media = deps["enrich_chicken_media"]
    enrich_gene_available_chicken_row = deps["enrich_gene_available_chicken_row"]
    enrich_missing_gene_data_in_batches = deps["enrich_missing_gene_data_in_batches"]
    enrich_missing_recessive_data_in_batches = deps["enrich_missing_recessive_data_in_batches"]
    enrich_ip_available_chicken_row = deps["enrich_ip_available_chicken_row"]
    enrich_ultimate_available_chicken_row = deps["enrich_ultimate_available_chicken_row"]
    enrich_ultimate_display = deps["enrich_ultimate_display"]
    filter_and_sort_ultimate_candidates = deps["filter_and_sort_ultimate_candidates"]
    filter_out_planner_tokens = deps["filter_out_planner_tokens"]
    find_potential_matches = deps["find_potential_matches"]
    get_best_available_gene_build_info = deps["get_best_available_gene_build_info"]
    get_chickens_by_wallet = deps["get_chickens_by_wallet"]
    get_effective_ip_stat = deps["get_effective_ip_stat"]
    get_featured_market_feed = deps["get_featured_market_feed"]
    get_ultimate_build_display = deps["get_ultimate_build_display"]
    get_ultimate_type_display = deps["get_ultimate_type_display"]
    get_wallet_access_expiry_display = deps["get_wallet_access_expiry_display"]
    get_wallet_chickens = deps["get_wallet_chickens"]
    get_weakest_ip_stat_info = deps["get_weakest_ip_stat_info"]
    has_active_payment_access_in_db = deps["has_active_payment_access_in_db"]
    is_breedable = deps["is_breedable"]
    is_full_siblings = deps["is_full_siblings"]
    is_generation_gap_allowed = deps["is_generation_gap_allowed"]
    is_parent_offspring = deps["is_parent_offspring"]
    is_ultimate_eligible = deps["is_ultimate_eligible"]
    make_empty_state = deps["make_empty_state"]
    match_settings = deps["match_settings"]
    normalize_auto_ninuno_filter = deps["normalize_auto_ninuno_filter"]
    normalize_gene_available_build_filter = deps["normalize_gene_available_build_filter"]
    normalize_gene_available_ninuno_filter = deps["normalize_gene_available_ninuno_filter"]
    normalize_gene_available_source_values = deps["normalize_gene_available_source_values"]
    normalize_ip_available_ninuno_filter = deps["normalize_ip_available_ninuno_filter"]
    normalize_ultimate_available_ninuno_filter = deps["normalize_ultimate_available_ninuno_filter"]
    parse_gene_csv_query_values = deps["parse_gene_csv_query_values"]
    parse_ip_csv_query_values = deps["parse_ip_csv_query_values"]
    parse_ultimate_csv_query_values = deps["parse_ultimate_csv_query_values"]
    pair_has_usable_ip_items = deps["pair_has_usable_ip_items"]
    pick_best_gene_auto_match_from_pool = deps["pick_best_gene_auto_match_from_pool"]
    pick_best_ultimate_auto_match = deps["pick_best_ultimate_auto_match"]
    pick_multi_pairs_from_candidates = deps["pick_multi_pairs_from_candidates"]
    refresh_ultimate_primary_builds_if_needed = deps["refresh_ultimate_primary_builds_if_needed"]
    require_authorized_wallet = deps["require_authorized_wallet"]
    safe_int = deps["safe_int"]
    sort_gene_available_chickens = deps["sort_gene_available_chickens"]
    sort_ip_available_chickens = deps["sort_ip_available_chickens"]
    sort_ip_match_rows = deps["sort_ip_match_rows"]
    sort_key_int = deps["sort_key_int"]
    sort_key_text = deps["sort_key_text"]
    sort_ultimate_available_table_chickens = deps["sort_ultimate_available_table_chickens"]
    upsert_chicken = deps["upsert_chicken"]
    upsert_family_root_summary = deps["upsert_family_root_summary"]
    GENE_BUILD_ORDER = deps["GENE_BUILD_ORDER"]
    CONTRACTS = deps["CONTRACTS"]

    def build_market_context(wallet):
        market_open = str(request.args.get("market_open") or "").strip().lower() in {"1", "true", "on", "yes"}
        show_featured_market_bar = has_active_payment_access_in_db(wallet)
        return {
            "market_open": market_open,
            "show_featured_market_bar": show_featured_market_bar,
            "featured_feed": None,
        }

    def maybe_load_featured_feed(mode, market_context):
        if market_context["show_featured_market_bar"] and market_context["market_open"]:
            market_context["featured_feed"] = get_featured_market_feed(
                mode=mode,
                target_count=8,
                batch_size=20,
            )

    def build_planner_context(wallet):
        planner_queue = deps["get_breeding_planner_queue"](wallet)
        return {
            "planner_queue": planner_queue,
            "planner_summary": build_planner_summary(planner_queue),
        }

    def build_auto_open_template_id(prefix, skip_auto_open, auto_match, potential_matches, multi_match_rows):
        if skip_auto_open or not auto_match or not potential_matches or multi_match_rows:
            return ""
        return f"{prefix}-{potential_matches[0]['candidate']['token_id']}"

    def get_ultimate_relaxed_session_key(wallet):
        wallet_key = str(wallet or "").strip().lower()
        return f"ultimate_include_lower::{wallet_key}"

    def build_available_pair_max(rows):
        return max(0, len(rows or []) // 2)

    def parse_ip_popup_params():
        popup_ip_diff = safe_int(request.args.get("popup_ip_diff"))
        popup_breed_diff = safe_int(request.args.get("popup_breed_diff"))
        popup_same_build = str(request.args.get("popup_same_build") or "").strip().lower() in {"1", "true", "on", "yes"}
        if popup_ip_diff is None:
            popup_ip_diff = 10
        if popup_breed_diff is None:
            popup_breed_diff = 1
        return {
            "popup_ip_diff": popup_ip_diff,
            "popup_breed_diff": popup_breed_diff,
            "popup_ninuno": "all",
            "popup_same_build": popup_same_build,
            "popup_match_count": max(1, safe_int(request.args.get("popup_match_count"), 1) or 1),
        }

    def parse_build_popup_params():
        popup_ip_diff = safe_int(request.args.get("popup_ip_diff"))
        popup_breed_diff = safe_int(request.args.get("popup_breed_diff"))
        if popup_ip_diff is None:
            popup_ip_diff = 10
        if popup_breed_diff is None:
            popup_breed_diff = 1
        popup_same_build = str(request.args.get("popup_same_build") or "").strip().lower() in {"1", "true", "on", "yes"}
        popup_same_instinct = str(request.args.get("popup_same_instinct") or "").strip().lower() in {"1", "true", "on", "yes"}
        return {
            "popup_build": "all",
            "popup_min_build_count": None,
            "popup_ip_diff": popup_ip_diff,
            "popup_breed_diff": popup_breed_diff,
            "popup_ninuno": "all",
            "popup_same_build": popup_same_build,
            "popup_same_instinct": popup_same_instinct,
            "popup_match_count": max(1, safe_int(request.args.get("popup_match_count"), 1) or 1),
        }

    def resolve_auto_match_mode(auto_match, popup_match_count, requested_mode=""):
        if not auto_match:
            return str(requested_mode or "").strip().lower()
        return "multiple" if (safe_int(popup_match_count, 1) or 1) > 1 else "single"

    def resolve_selected_chicken(rows, selected_token_id):
        if not selected_token_id:
            return None
        return next((row for row in rows if str(row["token_id"]) == str(selected_token_id)), None)

    def resolve_selected_token_id(selected_chicken, selected_token_id=""):
        if selected_chicken:
            return str(selected_chicken.get("token_id") or "")
        return str(selected_token_id or "")

    def select_left_pair_candidate(pair_candidates):
        if not pair_candidates:
            return None, ""
        selected_chicken = pair_candidates[0]["left"]
        return selected_chicken, resolve_selected_token_id(selected_chicken)

    def build_multi_match_feedback(multi_match_rows, multi_match_target, *, auto_match, skip_auto_open):
        multi_match_rows = list(multi_match_rows or [])
        multi_match_note = ""
        if multi_match_target and len(multi_match_rows) < multi_match_target:
            multi_match_note = f"Only {len(multi_match_rows)} valid pair(s) were available from the current filtered pool."
        auto_open_multi_match = bool(multi_match_rows) and auto_match and not skip_auto_open
        return {
            "multi_match_rows": multi_match_rows,
            "multi_match_target": multi_match_target,
            "multi_match_note": multi_match_note,
            "auto_open_multi_match": auto_open_multi_match,
        }

    def should_mark_auto_match_empty(auto_match, auto_match_source, auto_match_mode, potential_matches, multi_match_rows):
        if not auto_match:
            return False
        if auto_match_source == "available" and auto_match_mode == "multiple":
            return False
        return not potential_matches and not multi_match_rows

    def match_ip_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        selected_token_id = request.args.get("selected_token_id", "").strip()
        auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
        skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}

        if not require_authorized_wallet(wallet):
            return redirect(url_for("index"))

        min_ip = safe_int(request.args.get("min_ip"))
        ip_diff = safe_int(request.args.get("ip_diff"))
        ninuno_100_only = str(request.args.get("ninuno_100_only") or "").strip().lower() in {"1", "true", "on", "yes"}
        auto_match_source = str(request.args.get("auto_match_source") or "").strip().lower()
        auto_match_mode = str(request.args.get("auto_match_mode") or "").strip().lower()
        popup_params = parse_ip_popup_params()
        popup_ip_diff = popup_params["popup_ip_diff"]
        popup_breed_diff = popup_params["popup_breed_diff"]
        popup_ninuno = popup_params["popup_ninuno"]
        popup_same_build = popup_params["popup_same_build"]
        popup_match_count = popup_params["popup_match_count"]
        auto_match_mode = resolve_auto_match_mode(auto_match, popup_match_count, auto_match_mode)

        market_context = build_market_context(wallet)

        breedable_chickens = []
        selected_chicken = None
        potential_matches = []
        error = None

        ip_original_available_pool = []
        ip_available_filter_options = {
            "type_options": [],
            "generation_options": [],
            "breed_count_options": [],
            "ninuno_options": [
                {"value": "all", "label": "All"},
                {"value": "100", "label": "100% only"},
                {"value": "gt0", "label": "Above 0%"},
            ],
        }
        ip_active_filters = []
        selected_weakest_stat_column_label = "Selected Weakest Stat"
        ip_sort_by = str(request.args.get("sort_by") or "ip").strip().lower()
        ip_sort_dir = str(request.args.get("sort_dir") or "desc").strip().lower()
        multi_match_rows = []
        multi_match_target = 0
        multi_match_note = ""
        auto_open_multi_match = False
        auto_match_single_empty = False
        wallet_summary = None

        ip_filter_type_values = parse_ip_csv_query_values(request.args.get("ip_filter_type"))
        ip_filter_generation_values = parse_ip_csv_query_values(request.args.get("ip_filter_generation"))
        ip_filter_breed_count_values = parse_ip_csv_query_values(request.args.get("ip_filter_breed_count"))
        ip_filter_ninuno = normalize_ip_available_ninuno_filter(request.args.get("ip_filter_ninuno"))

        if ip_sort_by not in {"token_id", "ip", "weakest_stat", "generation", "breed_count", "ninuno"}:
            ip_sort_by = "ip"
        if ip_sort_dir not in {"asc", "desc"}:
            ip_sort_dir = "desc"
        if not request.args.get("ip_filter_ninuno"):
            ip_filter_ninuno = "100" if ninuno_100_only else "all"

        if wallet:
            try:
                chickens = get_wallet_chickens(wallet, ensure_loaded=True)
                ip_original_available_pool = filter_out_planner_tokens(
                    [
                        enrich_ip_available_chicken_row(
                            row,
                            enrich_chicken_media=enrich_chicken_media,
                            get_weakest_ip_stat_info=get_weakest_ip_stat_info,
                        )
                        for row in chickens
                        if is_breedable(row)
                    ],
                    wallet,
                )
                ip_available_filter_options = build_ip_available_filter_options(ip_original_available_pool, safe_int=safe_int)
                breedable_chickens = list(ip_original_available_pool)
                access_expiry = get_wallet_access_expiry_display(wallet)
                wallet_summary = build_wallet_summary(wallet=wallet, chickens=chickens, access_expiry=access_expiry)

                if min_ip is not None:
                    breedable_chickens = [
                        row for row in breedable_chickens
                        if safe_int(row.get("ip"), default=-1) is not None
                        and safe_int(row.get("ip"), default=-1) >= min_ip
                    ]

                breedable_chickens = [
                    row for row in breedable_chickens
                    if chicken_matches_ip_available_filters(
                        row,
                        safe_int=safe_int,
                        selected_types=ip_filter_type_values,
                        selected_generations=ip_filter_generation_values,
                        selected_breed_counts=ip_filter_breed_count_values,
                        ninuno_mode=ip_filter_ninuno,
                    )
                ]

                ip_active_filters = build_ip_active_filters(
                    selected_types=ip_filter_type_values,
                    min_ip=min_ip,
                    selected_generations=ip_filter_generation_values,
                    selected_breed_counts=ip_filter_breed_count_values,
                    ninuno_mode=ip_filter_ninuno,
                )
                breedable_chickens = sort_ip_available_chickens(
                    breedable_chickens,
                    sort_by=ip_sort_by,
                    sort_dir=ip_sort_dir,
                    sort_key_int=sort_key_int,
                    sort_key_text=sort_key_text,
                )

                if auto_match and auto_match_source == "available" and auto_match_mode == "multiple":
                    multi_match_target = min(popup_match_count, max(0, len(breedable_chickens) // 2))
                    multi_match_feedback = build_multi_match_feedback(
                        build_ip_multi_matches(
                        breedable_chickens=breedable_chickens,
                        ip_diff=popup_ip_diff,
                        breed_diff=popup_breed_diff,
                        ninuno_filter="all",
                        same_build=popup_same_build,
                        target_count=multi_match_target,
                        ),
                        multi_match_target,
                        auto_match=auto_match,
                        skip_auto_open=skip_auto_open,
                    )
                    multi_match_rows = multi_match_feedback["multi_match_rows"]
                    multi_match_note = multi_match_feedback["multi_match_note"]
                    auto_open_multi_match = multi_match_feedback["auto_open_multi_match"]
                elif auto_match and not selected_token_id:
                    ranked_sources = []
                    effective_ip_diff = popup_ip_diff if auto_match_source == "available" and auto_match_mode == "single" else ip_diff
                    effective_breed_diff = popup_breed_diff if auto_match_source == "available" and auto_match_mode == "single" else None
                    for source in breedable_chickens:
                        candidate_pool = [row for row in breedable_chickens if str(row["token_id"]) != str(source["token_id"])]
                        if auto_match_source == "available" and auto_match_mode == "single" and popup_same_build:
                            source_build = str(source.get("build_type") or source.get("gene_build_key") or source.get("primary_build") or "").strip().lower()
                            candidate_pool = [
                                row for row in candidate_pool
                                if source_build and str(row.get("build_type") or row.get("gene_build_key") or row.get("primary_build") or "").strip().lower() == source_build
                            ]
                        if effective_ip_diff is not None:
                            source_ip = safe_int(source.get("ip"))
                            if source_ip is not None:
                                candidate_pool = [row for row in candidate_pool if safe_int(row.get("ip")) is not None and abs(safe_int(row.get("ip")) - source_ip) <= effective_ip_diff]
                        if effective_breed_diff is not None:
                            source_breed = safe_int(source.get("breed_count"))
                            if source_breed is not None:
                                candidate_pool = [row for row in candidate_pool if safe_int(row.get("breed_count")) is not None and abs(safe_int(row.get("breed_count")) - source_breed) <= effective_breed_diff]
                        matches = find_potential_matches(source, candidate_pool, settings=match_settings)
                        matches = [row for row in matches if row.get("evaluation", {}).get("is_ip_recommended") and row.get("evaluation", {}).get("is_breed_count_recommended") and pair_has_usable_ip_items(source, row.get("candidate"))]
                        if matches:
                            ranked_sources.append({"source": source, "match_count": len(matches)})
                    ranked_sources.sort(key=lambda row: (-(safe_int(row["source"].get("ip"), 0) or 0), safe_int(row["source"].get("breed_count"), 999999) or 999999, -(float(row["source"].get("ownership_percent") or 0)), -row["match_count"], safe_int(row["source"].get("token_id"), 999999999) or 999999999))
                    if ranked_sources:
                        selected_token_id = str(ranked_sources[0]["source"]["token_id"])
                    elif should_mark_auto_match_empty(
                        auto_match,
                        auto_match_source,
                        auto_match_mode,
                        potential_matches,
                        multi_match_rows,
                    ):
                        auto_match_single_empty = True

                selected_chicken = resolve_selected_chicken(breedable_chickens, selected_token_id)

                selected_weakest_stat_name = ""
                selected_weakest_stat_label = "Selected Weakest Stat"
                if selected_chicken:
                    selected_weakest_info = get_weakest_ip_stat_info(selected_chicken)
                    selected_weakest_stat_name = selected_weakest_info.get("name") or ""
                    selected_weakest_stat_label = selected_weakest_info.get("label") or selected_weakest_stat_label
                    selected_weakest_stat_column_label = (
                        f"Matched {selected_weakest_stat_label}"
                        if selected_weakest_stat_label != "Selected Weakest Stat"
                        else selected_weakest_stat_column_label
                    )
                    candidate_pool = [row for row in breedable_chickens if str(row["token_id"]) != selected_token_id]
                    is_available_single_auto = auto_match and auto_match_source == "available" and auto_match_mode == "single"
                    if is_available_single_auto and popup_same_build:
                        selected_build = str(selected_chicken.get("build_type") or selected_chicken.get("gene_build_key") or selected_chicken.get("primary_build") or "").strip().lower()
                        candidate_pool = [
                            row for row in candidate_pool
                            if selected_build and str(row.get("build_type") or row.get("gene_build_key") or row.get("primary_build") or "").strip().lower() == selected_build
                        ]
                    preview_ip_diff = popup_ip_diff if is_available_single_auto else ip_diff
                    if preview_ip_diff is not None:
                        selected_ip = safe_int(selected_chicken.get("ip"))
                        if selected_ip is not None:
                            candidate_pool = [row for row in candidate_pool if safe_int(row.get("ip")) is not None and abs(safe_int(row.get("ip")) - selected_ip) <= preview_ip_diff]
                    if is_available_single_auto and popup_breed_diff is not None:
                        selected_breed = safe_int(selected_chicken.get("breed_count"))
                        if selected_breed is not None:
                            candidate_pool = [row for row in candidate_pool if safe_int(row.get("breed_count")) is not None and abs(safe_int(row.get("breed_count")) - selected_breed) <= popup_breed_diff]
                    potential_matches = find_potential_matches(selected_chicken, candidate_pool, settings=match_settings)
                    if auto_match:
                        potential_matches = [
                            row for row in potential_matches
                            if row.get("evaluation", {}).get("is_ip_recommended")
                            and row.get("evaluation", {}).get("is_breed_count_recommended")
                            and pair_has_usable_ip_items(selected_chicken, row.get("candidate"))
                        ]
                    potential_matches = sort_ip_match_rows(selected_chicken, potential_matches)
                    if should_mark_auto_match_empty(
                        auto_match,
                        auto_match_source,
                        auto_match_mode,
                        potential_matches,
                        multi_match_rows,
                    ):
                        auto_match_single_empty = True
                    for row in potential_matches:
                        candidate = row.get("candidate") or {}
                        row["selected_weakest_stat_display"] = (
                            f"{selected_weakest_stat_label}: {get_effective_ip_stat(candidate, selected_weakest_stat_name)}"
                            if selected_weakest_stat_name else ""
                        )

                maybe_load_featured_feed("ip", market_context)
            except Exception as exc:
                error = f"Failed to load IP breeding matches: {exc}"

        planner_context = build_planner_context(wallet)

        return render_template(
            "match_ip.html",
            wallet=wallet,
            selected_token_id=selected_token_id,
            selected_chicken=selected_chicken,
            breedable_chickens=breedable_chickens,
            potential_matches=potential_matches,
            min_ip=min_ip,
            ip_diff=ip_diff,
            ninuno_100_only=ninuno_100_only,
            sort_by=ip_sort_by,
            sort_dir=ip_sort_dir,
            selected_weakest_stat_column_label=selected_weakest_stat_column_label,
            auto_match=auto_match,
            auto_match_source=auto_match_source,
            auto_match_mode=auto_match_mode,
            popup_ip_diff=popup_ip_diff,
            popup_breed_diff=popup_breed_diff,
            popup_ninuno=popup_ninuno,
            popup_same_build=popup_same_build,
            popup_match_count=popup_match_count,
            multi_match_rows=multi_match_rows,
            multi_match_target=multi_match_target,
            multi_match_note=multi_match_note,
            auto_open_multi_match=auto_open_multi_match,
            available_pair_max=max(0, len(breedable_chickens) // 2),
            auto_open_template_id=build_auto_open_template_id("compare-ip", skip_auto_open, auto_match, potential_matches, multi_match_rows),
            auto_match_single_empty=auto_match_single_empty,
            planner_queue=planner_context["planner_queue"],
            planner_summary=planner_context["planner_summary"],
            wallet_summary=wallet_summary,
            ip_filter_type_values=ip_filter_type_values,
            ip_filter_generation_values=ip_filter_generation_values,
            ip_filter_breed_count_values=ip_filter_breed_count_values,
            ip_filter_ninuno=ip_filter_ninuno,
            ip_available_filter_options=ip_available_filter_options,
            ip_active_filters=ip_active_filters,
            ip_original_available_count=len(ip_original_available_pool),
            market_open=market_context["market_open"],
            show_featured_market_bar=market_context["show_featured_market_bar"],
            featured_feed=market_context["featured_feed"],
            error=error,
        )

    def match_gene_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        selected_token_id = request.args.get("selected_token_id", "").strip()
        auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
        ninuno_100_only = str(request.args.get("ninuno_100_only") or "").strip().lower() in {"1", "true", "on", "yes"}
        skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}
        auto_match_source = str(request.args.get("auto_match_source") or "").strip().lower()
        auto_match_mode = str(request.args.get("auto_match_mode") or "").strip().lower()
        popup_params = parse_build_popup_params()
        popup_build = popup_params["popup_build"]
        popup_min_build_count = popup_params["popup_min_build_count"]
        popup_ip_diff = None
        popup_breed_diff = popup_params["popup_breed_diff"]
        popup_ninuno = popup_params["popup_ninuno"]
        popup_same_build = popup_params["popup_same_build"]
        popup_same_instinct = popup_params["popup_same_instinct"]
        popup_match_count = popup_params["popup_match_count"]
        auto_match_mode = resolve_auto_match_mode(auto_match, popup_match_count, auto_match_mode)
        market_context = build_market_context(wallet)
        gene_sort_by = str(request.args.get("sort_by") or "build").strip().lower()
        gene_sort_dir = str(request.args.get("sort_dir") or "asc").strip().lower()
        if gene_sort_by not in {"token_id", "build", "build_match", "instinct", "ip", "generation", "breed_count", "ninuno"}:
            gene_sort_by = "build"
        if gene_sort_dir not in {"asc", "desc"}:
            gene_sort_dir = "asc"

        gene_filter_type_values = parse_gene_csv_query_values(",".join(request.args.getlist("gene_filter_type")))
        gene_filter_build = normalize_gene_available_build_filter(request.args.get("gene_filter_build"), build_order=GENE_BUILD_ORDER)
        gene_filter_build_match_values = parse_gene_csv_query_values(",".join(request.args.getlist("gene_filter_build_match")))
        gene_filter_build_source_values = []
        gene_filter_instinct_values = parse_gene_csv_query_values(",".join(request.args.getlist("gene_filter_instinct")))
        gene_min_ip = safe_int(request.args.get("gene_min_ip"))
        gene_filter_generation_values = parse_gene_csv_query_values(",".join(request.args.getlist("gene_filter_generation")))
        gene_filter_breed_count_values = parse_gene_csv_query_values(",".join(request.args.getlist("gene_filter_breed_count")))
        gene_filter_ninuno = normalize_gene_available_ninuno_filter(request.args.get("gene_filter_ninuno"))

        if not require_authorized_wallet(wallet):
            return redirect(url_for("index"))

        breedable_chickens = []
        selected_chicken = None
        potential_matches = []
        gene_enrichment_loaded = 0
        gene_enrichment_remaining = 0
        error = None
        multi_match_rows = []
        multi_match_target = 0
        multi_match_note = ""
        auto_open_multi_match = False
        auto_match_single_empty = False
        wallet_summary = None
        gene_original_available_pool = []
        gene_available_filter_options = {}
        available_auto_match_pool = []

        available_empty_state = make_empty_state("No chickens found", "No chickens matched the current Gene filters.", "Try removing some Gene filters or clear the 100% Ninuno option.")
        match_empty_state = make_empty_state("No matches found", "No valid gene pair was found for the selected chicken.", "Try another chicken or review a larger available pool.")
        auto_match_empty_state = make_empty_state("No valid auto-match", "Auto Match could not find a usable gene pair.", "Try wider popup filters or choose a chicken manually.")

        if wallet:
            try:
                chickens = get_wallet_chickens(wallet, ensure_loaded=True)
                if refresh_ultimate_primary_builds_if_needed(chickens, upsert_chicken, safe_int):
                    chickens = get_wallet_chickens(wallet, ensure_loaded=True)
                access_expiry = get_wallet_access_expiry_display(wallet)
                wallet_summary = build_wallet_summary(wallet=wallet, chickens=chickens, access_expiry=access_expiry)
                batch_result = enrich_missing_gene_data_in_batches(chickens=chickens, wallet=wallet, page_key="gene", batch_size=5, prioritized_token_id=selected_token_id or None)
                gene_enrichment_loaded = batch_result["loaded"]
                chickens = get_wallet_chickens(wallet, ensure_loaded=False)
                gene_enrichment_remaining = batch_result["remaining"]

                all_breedable = [
                    enrich_gene_available_chicken_row(row, enrich_chicken_media, get_best_available_gene_build_info, build_order=GENE_BUILD_ORDER)
                    for row in chickens
                    if is_breedable(row)
                ]
                gene_original_available_pool = filter_out_planner_tokens(all_breedable, wallet)
                gene_original_available_pool = [row for row in gene_original_available_pool if str(row.get("gene_build_key") or "").strip() and safe_int(row.get("gene_build_match_count"), 0) >= 2]
                if ninuno_100_only:
                    gene_original_available_pool = [row for row in gene_original_available_pool if row.get("is_complete") and float(row.get("ownership_percent") or 0) == 100.0]
                gene_available_filter_options = build_gene_available_filter_options(gene_original_available_pool, safe_int, build_order=GENE_BUILD_ORDER)
                breedable_chickens = [
                    row for row in gene_original_available_pool
                    if chicken_matches_gene_available_filters(
                        row, safe_int=safe_int, selected_types=gene_filter_type_values, selected_build=gene_filter_build,
                        selected_build_matches=gene_filter_build_match_values, selected_build_sources=[],
                        selected_instincts=gene_filter_instinct_values, min_ip=gene_min_ip, selected_generations=gene_filter_generation_values,
                        selected_breed_counts=gene_filter_breed_count_values, ninuno_mode=gene_filter_ninuno, build_order=GENE_BUILD_ORDER,
                    )
                ]
                breedable_chickens = sort_gene_available_chickens(breedable_chickens, sort_by=gene_sort_by, sort_dir=gene_sort_dir, build_order=GENE_BUILD_ORDER)
                match_empty_state = build_gene_match_empty_state(selected_chicken.get("build_type") if selected_chicken else "", ninuno_100_only=ninuno_100_only, auto_match=auto_match, same_instinct=False, min_build_count=None)
                auto_match_empty_state = build_gene_match_empty_state(selected_chicken.get("build_type") if selected_chicken else "", ninuno_100_only=ninuno_100_only, auto_match=True, same_instinct=popup_same_instinct, min_build_count=None)

                available_auto_match_pool = breedable_chickens
                available_pair_max = build_available_pair_max(available_auto_match_pool)
                if auto_match and auto_match_source == "available" and auto_match_mode == "multiple":
                    multi_match_target = min(popup_match_count, available_pair_max)
                    pair_candidates = build_gene_available_auto_candidates_same_build(available_auto_match_pool, min_build_count=None, ip_diff=popup_ip_diff, breed_diff=popup_breed_diff, same_instinct=popup_same_instinct, ninuno_mode="all", same_build=True)
                    multi_match_feedback = build_multi_match_feedback(
                        pick_multi_pairs_from_candidates(pair_candidates, multi_match_target),
                        multi_match_target,
                        auto_match=auto_match,
                        skip_auto_open=skip_auto_open,
                    )
                    multi_match_rows = multi_match_feedback["multi_match_rows"]
                    multi_match_note = multi_match_feedback["multi_match_note"]
                    auto_open_multi_match = multi_match_feedback["auto_open_multi_match"]
                elif auto_match and not selected_token_id:
                    if auto_match_source == "available" and auto_match_mode == "single":
                        pair_candidates = build_gene_available_auto_candidates_same_build(available_auto_match_pool, min_build_count=None, ip_diff=popup_ip_diff, breed_diff=popup_breed_diff, same_instinct=popup_same_instinct, ninuno_mode="all", same_build=True)
                        if pair_candidates:
                            selected_chicken, selected_token_id = select_left_pair_candidate(pair_candidates)
                            top_pair = pair_candidates[0]
                            potential_matches = [
                                {
                                    "candidate": top_pair.get("right") or {},
                                    "candidate_eval": top_pair.get("candidate_eval"),
                                    "selected_eval": top_pair.get("selected_eval"),
                                    "build_type": top_pair.get("build_type"),
                                    "added_missing_traits": top_pair.get("added_missing_traits") or 0,
                                    "combined_match_count": top_pair.get("combined_match_count", 0),
                                    "combined_match_total": top_pair.get("combined_match_total", 0),
                                    "selected_build_match_count": top_pair.get("selected_build_match_count", 0),
                                    "candidate_build_match_count": top_pair.get("candidate_build_match_count", 0),
                                    "gene_pair_metrics": top_pair.get("gene_pair_metrics") or {},
                                    "gene_priority_metrics": top_pair.get("gene_priority_metrics") or {},
                                    "ranking": top_pair.get("ranking"),
                                }
                            ]
                    else:
                        selected_chicken, potential_matches = pick_best_gene_auto_match_from_pool(breedable_chickens=breedable_chickens, popup_build="all", popup_min_build_count=None, popup_breed_diff=None, popup_ninuno="all")
                    if selected_chicken:
                        selected_token_id = str(selected_chicken.get("token_id") or "")
                    elif should_mark_auto_match_empty(
                        auto_match,
                        auto_match_source,
                        auto_match_mode,
                        potential_matches,
                        multi_match_rows,
                    ):
                        auto_match_single_empty = True

                selected_chicken = resolve_selected_chicken(breedable_chickens, selected_token_id)
                if selected_chicken:
                    if not potential_matches:
                        potential_matches = build_gene_potential_matches_strict(selected_chicken, breedable_chickens)
                    if should_mark_auto_match_empty(
                        auto_match,
                        auto_match_source,
                        auto_match_mode,
                        potential_matches,
                        multi_match_rows,
                    ):
                        auto_match_single_empty = True
                elif should_mark_auto_match_empty(
                    auto_match,
                    auto_match_source,
                    auto_match_mode,
                    potential_matches,
                    multi_match_rows,
                ):
                    auto_match_single_empty = True
                maybe_load_featured_feed("gene", market_context)
            except Exception as exc:
                error = f"Failed to load gene breeding matches: {exc}"
                available_pair_max = 0
        else:
            available_pair_max = 0

        planner_context = build_planner_context(wallet)

        return render_template(
            "match_gene.html",
            wallet=wallet,
            selected_token_id=selected_token_id,
            selected_chicken=selected_chicken,
            breedable_chickens=breedable_chickens,
            potential_matches=potential_matches,
            selected_build_type=(selected_chicken.get("build_type") if selected_chicken else ""),
            gene_filter_type_values=gene_filter_type_values,
            gene_filter_build=gene_filter_build,
            gene_filter_build_match_values=gene_filter_build_match_values,
            gene_filter_build_source_values=gene_filter_build_source_values,
            gene_filter_instinct_values=gene_filter_instinct_values,
            gene_min_ip=gene_min_ip,
            gene_filter_generation_values=gene_filter_generation_values,
            gene_filter_breed_count_values=gene_filter_breed_count_values,
            gene_filter_ninuno=gene_filter_ninuno,
            gene_available_filter_options=gene_available_filter_options,
            gene_active_filters=build_gene_active_filters(selected_types=gene_filter_type_values, selected_build=gene_filter_build, selected_build_matches=gene_filter_build_match_values, selected_build_sources=[], selected_instincts=gene_filter_instinct_values, min_ip=gene_min_ip, selected_generations=gene_filter_generation_values, selected_breed_counts=gene_filter_breed_count_values, ninuno_mode=gene_filter_ninuno, build_order=GENE_BUILD_ORDER),
            gene_original_available_count=len(gene_original_available_pool),
            ninuno_100_only=ninuno_100_only,
            sort_by=gene_sort_by,
            sort_dir=gene_sort_dir,
            auto_match=auto_match,
            auto_match_source=auto_match_source,
            auto_match_mode=auto_match_mode,
            popup_build=popup_build,
            popup_min_build_count=popup_min_build_count,
            popup_ip_diff=popup_ip_diff,
            popup_breed_diff=popup_breed_diff,
            popup_ninuno=popup_ninuno,
            popup_same_build=popup_same_build,
            popup_same_instinct=popup_same_instinct,
            popup_match_count=popup_match_count,
            multi_match_rows=multi_match_rows,
            multi_match_target=multi_match_target,
            multi_match_note=multi_match_note,
            auto_open_multi_match=auto_open_multi_match,
            available_pair_max=available_pair_max,
            auto_open_template_id=build_auto_open_template_id("compare-gene", skip_auto_open, auto_match, potential_matches, multi_match_rows),
            auto_match_single_empty=auto_match_single_empty,
            gene_enrichment_loaded=gene_enrichment_loaded,
            gene_enrichment_remaining=gene_enrichment_remaining,
            planner_queue=planner_context["planner_queue"],
            planner_summary=planner_context["planner_summary"],
            available_empty_state=available_empty_state,
            match_empty_state=match_empty_state,
            auto_match_empty_state=auto_match_empty_state,
            wallet_summary=wallet_summary,
            market_open=market_context["market_open"],
            show_featured_market_bar=market_context["show_featured_market_bar"],
            featured_feed=market_context["featured_feed"],
            error=error,
        )

    def match_ultimate_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        selected_token_id = request.args.get("selected_token_id", "").strip()
        auto_match = str(request.args.get("auto_match") or "").strip().lower() in {"1", "true", "on", "yes"}
        skip_auto_open = str(request.args.get("skip_auto_open") or "").strip().lower() in {"1", "true", "on", "yes"}
        auto_match_source = str(request.args.get("auto_match_source") or "").strip().lower()
        auto_match_mode = str(request.args.get("auto_match_mode") or "").strip().lower()
        popup_params = parse_build_popup_params()
        popup_build = popup_params["popup_build"]
        popup_min_build_count = popup_params["popup_min_build_count"]
        popup_ip_diff = popup_params["popup_ip_diff"]
        popup_breed_diff = popup_params["popup_breed_diff"]
        popup_ninuno = popup_params["popup_ninuno"]
        popup_same_build = popup_params["popup_same_build"]
        popup_match_count = popup_params["popup_match_count"]
        auto_match_mode = resolve_auto_match_mode(auto_match, popup_match_count, auto_match_mode)
        market_context = build_market_context(wallet)
        ultimate_relaxed_available = has_active_payment_access_in_db(wallet)
        ultimate_include_lower_arg = str(request.args.get("ultimate_include_lower") or "").strip().lower()
        ultimate_include_lower_values = False
        if wallet:
            relaxed_session_key = get_ultimate_relaxed_session_key(wallet)
            if not ultimate_relaxed_available:
                session.pop(relaxed_session_key, None)
            elif ultimate_include_lower_arg in {"1", "true", "on", "yes"}:
                session[relaxed_session_key] = True
            elif ultimate_include_lower_arg in {"0", "false", "off", "no"}:
                session.pop(relaxed_session_key, None)
            ultimate_include_lower_values = bool(session.get(relaxed_session_key))

        if not require_authorized_wallet(wallet):
            return redirect(url_for("index"))

        breedable_chickens = []
        selected_chicken = None
        potential_matches = []
        error = None
        multi_match_rows = []
        multi_match_target = 0
        multi_match_note = ""
        auto_open_multi_match = False
        auto_match_single_empty = False
        wallet_summary = None
        ultimate_original_available_pool = []
        ultimate_available_filter_options = {
            "type_options": [],
            "build_options": [],
            "build_match_options": [],
            "generation_options": [],
            "breed_count_options": [],
            "ninuno_options": [
                {"value": "all", "label": "All"},
                {"value": "100", "label": "100% only"},
                {"value": "gt0", "label": "Above 0%"},
            ],
        }
        available_auto_match_pool = []
        ultimate_sort_by = str(request.args.get("sort_by") or "ultimate_type").strip().lower()
        ultimate_sort_dir = str(request.args.get("sort_dir") or "asc").strip().lower()
        if ultimate_sort_by not in {"token_id", "ultimate_type", "build", "build_match", "ip", "generation", "breed_count", "ninuno"}:
            ultimate_sort_by = "ultimate_type"
        if ultimate_sort_dir not in {"asc", "desc"}:
            ultimate_sort_dir = "asc"
        ultimate_filter_type_values = parse_ultimate_csv_query_values(request.args.get("ultimate_filter_type"))
        ultimate_filter_build = (request.args.get("ultimate_filter_build") or "all").strip().lower()
        ultimate_filter_build_match_values = parse_ultimate_csv_query_values(request.args.get("ultimate_filter_build_match"))
        ultimate_min_ip = safe_int(request.args.get("ultimate_min_ip"))
        ultimate_filter_generation_values = parse_ultimate_csv_query_values(request.args.get("ultimate_filter_generation"))
        ultimate_filter_breed_count_values = parse_ultimate_csv_query_values(request.args.get("ultimate_filter_breed_count"))
        ultimate_filter_ninuno = normalize_ultimate_available_ninuno_filter(request.args.get("ultimate_filter_ninuno"))
        available_empty_state = build_ultimate_available_empty_state()
        match_empty_state = build_ultimate_match_empty_state(auto_match=auto_match, ninuno_mode="all", breed_diff=None)
        auto_match_empty_state = build_ultimate_match_empty_state(auto_match=True, ninuno_mode="all", breed_diff=popup_breed_diff)
        available_pair_max = 0

        if wallet:
            try:
                chickens = get_wallet_chickens(wallet, ensure_loaded=True)
                access_expiry = get_wallet_access_expiry_display(wallet)
                wallet_summary = build_wallet_summary(wallet=wallet, chickens=chickens, access_expiry=access_expiry)
                all_breedable = [row for row in chickens if is_breedable(row)]
                ultimate_original_available_pool = filter_out_planner_tokens([
                    enrich_ultimate_available_chicken_row(
                        chicken=enrich_ultimate_display(row),
                        enrich_chicken_media=enrich_chicken_media,
                        get_ultimate_type_display_fn=get_ultimate_type_display,
                        get_ultimate_build_display_fn=get_ultimate_build_display,
                        safe_int=safe_int,
                    )
                    for row in all_breedable if ultimate_include_lower_values or is_ultimate_eligible(row)
                ], wallet)
                ultimate_available_filter_options = build_ultimate_available_filter_options(ultimate_original_available_pool, safe_int=safe_int)
                breedable_chickens = [
                    row for row in ultimate_original_available_pool
                    if chicken_matches_ultimate_available_filters(row, safe_int=safe_int, selected_types=ultimate_filter_type_values, selected_build=ultimate_filter_build, selected_build_matches=ultimate_filter_build_match_values, min_ip=ultimate_min_ip, selected_generations=ultimate_filter_generation_values, selected_breed_counts=ultimate_filter_breed_count_values, ninuno_mode=ultimate_filter_ninuno)
                ]
                breedable_chickens = sort_ultimate_available_table_chickens(breedable_chickens, sort_by=ultimate_sort_by, sort_dir=ultimate_sort_dir, sort_key_int=safe_int)
                if ultimate_original_available_pool and not breedable_chickens:
                    available_empty_state = {"kicker": "No filtered chickens", "title": "No chickens match the current filters.", "body": "Try removing one or more filters to widen the available Ultimate pool."}

                available_auto_match_pool = breedable_chickens
                available_pair_max = build_available_pair_max(available_auto_match_pool)
                if auto_match and auto_match_source == "available" and auto_match_mode == "multiple":
                    multi_match_target = min(popup_match_count, available_pair_max)
                    pair_candidates = build_ultimate_available_auto_candidates(
                        available_auto_match_pool,
                        ip_diff=popup_ip_diff,
                        breed_diff=popup_breed_diff,
                        ninuno_mode="all",
                        include_lower_values=ultimate_include_lower_values,
                        same_build=True,
                    )
                    multi_match_feedback = build_multi_match_feedback(
                        pick_multi_pairs_from_candidates(pair_candidates, multi_match_target),
                        multi_match_target,
                        auto_match=auto_match,
                        skip_auto_open=skip_auto_open,
                    )
                    multi_match_rows = multi_match_feedback["multi_match_rows"]
                    multi_match_note = multi_match_feedback["multi_match_note"]
                    auto_open_multi_match = multi_match_feedback["auto_open_multi_match"]
                else:
                    selected_chicken = resolve_selected_chicken(breedable_chickens, selected_token_id)
                    if auto_match and not selected_token_id:
                        if auto_match_source == "available" and auto_match_mode == "single":
                            pair_candidates = build_ultimate_available_auto_candidates(
                                available_auto_match_pool,
                                ip_diff=popup_ip_diff,
                                breed_diff=popup_breed_diff,
                                ninuno_mode="all",
                                include_lower_values=ultimate_include_lower_values,
                                same_build=True,
                            )
                            if pair_candidates:
                                selected_chicken, selected_token_id = select_left_pair_candidate(pair_candidates)
                                top_pair = pair_candidates[0]
                                potential_matches = [
                                    {
                                        "candidate": top_pair.get("right") or {},
                                        "selected_build": top_pair.get("build_type"),
                                        "build_type": top_pair.get("build_type"),
                                        "left_item": top_pair.get("left_item"),
                                        "right_item": top_pair.get("right_item"),
                                        "ranking": top_pair.get("ranking"),
                                        "ultimate_build_metrics": top_pair.get("ultimate_build_metrics") or {},
                                        "ultimate_ip_metrics": top_pair.get("ultimate_ip_metrics") or {},
                                        "ultimate_build_priority_metrics": top_pair.get("ultimate_build_priority_metrics") or {},
                                        "ultimate_ip_priority_metrics": top_pair.get("ultimate_ip_priority_metrics") or {},
                                        "ultimate_ip_threshold_metrics": top_pair.get("ultimate_ip_threshold_metrics") or {},
                                        "ultimate_ip_burden_metrics": top_pair.get("ultimate_ip_burden_metrics") or {},
                                    }
                                ]
                            else:
                                auto_match_single_empty = True
                        else:
                            selected_chicken, potential_matches = pick_best_ultimate_auto_match(
                                breedable_chickens,
                                include_lower_values=ultimate_include_lower_values,
                            )
                            selected_token_id = resolve_selected_token_id(selected_chicken, selected_token_id)
                    if selected_chicken and not potential_matches:
                        candidate_pool = [
                            row for row in breedable_chickens
                            if str(row["token_id"]) != selected_token_id
                            and not is_parent_offspring(selected_chicken, row)
                            and not is_full_siblings(selected_chicken, row)
                            and is_generation_gap_allowed(selected_chicken, row, max_gap=match_settings["max_generation_gap"])
                        ]
                        potential_matches = filter_and_sort_ultimate_candidates(
                            selected_chicken,
                            candidate_pool,
                            include_lower_values=ultimate_include_lower_values,
                        )
                    if should_mark_auto_match_empty(
                        auto_match,
                        auto_match_source,
                        auto_match_mode,
                        potential_matches,
                        multi_match_rows,
                    ):
                        auto_match_single_empty = True
                maybe_load_featured_feed("ultimate", market_context)
            except Exception as exc:
                error = f"Failed to load ultimate breeding matches: {exc}"

        planner_context = build_planner_context(wallet)

        return render_template(
            "match_ultimate.html",
            wallet=wallet,
            selected_token_id=selected_token_id,
            selected_chicken=selected_chicken,
            breedable_chickens=breedable_chickens,
            sort_by=ultimate_sort_by,
            sort_dir=ultimate_sort_dir,
            ultimate_filter_type_values=ultimate_filter_type_values,
            ultimate_min_ip=ultimate_min_ip,
            ultimate_filter_build=ultimate_filter_build,
            ultimate_filter_build_match_values=ultimate_filter_build_match_values,
            ultimate_filter_generation_values=ultimate_filter_generation_values,
            ultimate_filter_breed_count_values=ultimate_filter_breed_count_values,
            ultimate_filter_ninuno=ultimate_filter_ninuno,
            ultimate_available_filter_options=ultimate_available_filter_options,
            ultimate_active_filters=build_ultimate_active_filters(selected_types=ultimate_filter_type_values, selected_build=ultimate_filter_build, selected_build_matches=ultimate_filter_build_match_values, min_ip=ultimate_min_ip, selected_generations=ultimate_filter_generation_values, selected_breed_counts=ultimate_filter_breed_count_values, ninuno_mode=ultimate_filter_ninuno),
            ultimate_original_available_count=len(ultimate_original_available_pool),
            ultimate_relaxed_available=ultimate_relaxed_available,
            ultimate_include_lower_values=ultimate_include_lower_values,
            potential_matches=potential_matches,
            auto_match=auto_match,
            auto_match_source=auto_match_source,
            auto_match_mode=auto_match_mode,
            popup_ip_diff=popup_ip_diff,
            popup_breed_diff=popup_breed_diff,
            popup_ninuno=popup_ninuno,
            popup_same_build=popup_same_build,
            popup_match_count=popup_match_count,
            popup_build=popup_build,
            popup_min_build_count=popup_min_build_count,
            multi_match_rows=multi_match_rows,
            multi_match_target=multi_match_target,
            multi_match_note=multi_match_note,
            auto_open_multi_match=auto_open_multi_match,
            available_pair_max=available_pair_max,
            auto_open_template_id=build_auto_open_template_id("compare-ultimate", skip_auto_open, auto_match, potential_matches, multi_match_rows),
            auto_match_single_empty=auto_match_single_empty,
            planner_queue=planner_context["planner_queue"],
            planner_summary=planner_context["planner_summary"],
            available_empty_state=available_empty_state,
            match_empty_state=match_empty_state,
            auto_match_empty_state=auto_match_empty_state,
            wallet_summary=wallet_summary,
            market_open=market_context["market_open"],
            show_featured_market_bar=market_context["show_featured_market_bar"],
            featured_feed=market_context["featured_feed"],
            error=error,
        )

    def process_gene_batch():
        wallet = request.form.get("wallet_address", "").strip().lower()
        selected_token_id = request.form.get("selected_token_id", "").strip()
        if not require_authorized_wallet(wallet):
            return {"ok": False, "error": "Unauthorized"}, 403
        try:
            chickens = get_wallet_chickens(wallet, ensure_loaded=True)
            batch_result = enrich_missing_recessive_data_in_batches(chickens=chickens, wallet=wallet, page_key="gene", batch_size=5, prioritized_token_id=selected_token_id or None)
            return {"ok": True, "loaded": batch_result["loaded"], "remaining": batch_result["remaining"]}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}, 500

    def complete_ninuno():
        anchor_id = request.form.get("anchor_id", "").strip()
        wallet = request.form.get("wallet_address", "").strip().lower()
        token_id = request.form.get("token_id", "").strip()
        selected_token_id = request.form.get("selected_token_id", "").strip()
        if not require_authorized_wallet(wallet):
            return redirect(url_for("index"))
        if not wallet or not token_id:
            return redirect(url_for("index", wallet_address=wallet))
        chickens = get_chickens_by_wallet(wallet)
        owned_token_ids = {str(row["token_id"]) for row in chickens}
        summary = complete_ninuno_via_lineage_with_resume(wallet_address=wallet, token_id=token_id, owned_token_ids=owned_token_ids, depth=3, max_tokens=300, contract_addresses=CONTRACTS)
        upsert_family_root_summary(wallet, summary)
        referrer = request.referrer or ""
        if referrer:
            base_referrer = referrer.split("#")[0]
            separator = "&" if "?" in base_referrer else "?"
            if anchor_id:
                return redirect(f"{base_referrer}{separator}skip_auto_open=1#{anchor_id}")
            return redirect(f"{base_referrer}{separator}skip_auto_open=1")
        return redirect(url_for("match_ip_page", wallet_address=wallet, selected_token_id=selected_token_id or token_id))

    app.add_url_rule("/match/ip", endpoint="match_ip_page", view_func=match_ip_page, methods=["GET"])
    app.add_url_rule("/match/gene", endpoint="match_gene_page", view_func=match_gene_page, methods=["GET"])
    app.add_url_rule("/match/ultimate", endpoint="match_ultimate_page", view_func=match_ultimate_page, methods=["GET"])
    app.add_url_rule("/match/gene/process-batch", endpoint="process_gene_batch", view_func=process_gene_batch, methods=["POST"])
    app.add_url_rule("/complete-ninuno", endpoint="complete_ninuno", view_func=complete_ninuno, methods=["POST"])
