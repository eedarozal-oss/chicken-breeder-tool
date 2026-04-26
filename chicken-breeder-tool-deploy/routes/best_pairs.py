from flask import redirect, render_template, request, url_for

from services.best_pair_selection import build_best_pair_suggestions, normalize_cost_preference


COST_PREFERENCE_OPTIONS = [
    {"value": "any", "label": "Any breed count"},
    {"value": "prefer_low", "label": "Prefer low breed count"},
    {"value": "max_1", "label": "Max breed count: 1"},
    {"value": "max_2", "label": "Max breed count: 2"},
    {"value": "max_3", "label": "Max breed count: 3"},
]


def register_best_pair_routes(app, deps):
    def build_planner_context(wallet):
        planner_queue = deps["get_breeding_planner_queue"](wallet)
        return {
            "planner_queue": planner_queue,
            "planner_summary": deps["build_planner_summary"](planner_queue),
        }

    def build_best_pair_pools(wallet):
        chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
        breedable = [row for row in chickens if deps["is_breedable"](row)]

        ip_pool = deps["filter_out_planner_tokens"](
            [
                deps["enrich_ip_available_chicken_row"](
                    row,
                    enrich_chicken_media=deps["enrich_chicken_media"],
                    get_weakest_ip_stat_info=deps["get_weakest_ip_stat_info"],
                )
                for row in breedable
            ],
            wallet,
        )
        gene_pool = deps["filter_out_planner_tokens"](
            [deps["enrich_gene_available_display"](row) for row in breedable],
            wallet,
        )
        ultimate_pool = deps["filter_out_planner_tokens"](
            [
                deps["enrich_ultimate_available_chicken_row"](
                    chicken=deps["enrich_ultimate_display"](row),
                    enrich_chicken_media=deps["enrich_chicken_media"],
                    get_ultimate_type_display_fn=deps["get_ultimate_type_display"],
                    get_ultimate_build_display_fn=deps["get_ultimate_build_display"],
                    safe_int=deps["safe_int"],
                )
                for row in breedable
                if deps["is_ultimate_eligible"](row)
            ],
            wallet,
        )

        return chickens, ultimate_pool, gene_pool, ip_pool

    def best_pairs_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        cost_preference = normalize_cost_preference(request.args.get("cost_preference"))
        should_generate = str(request.args.get("generate") or "").strip().lower() in {"1", "true", "yes", "on"}
        status = str(request.args.get("status") or "").strip().lower()
        message = str(request.args.get("message") or "").strip()

        results = {"pairs": [], "counts": {"ultimate": 0, "gene": 0, "ip": 0}, "cost_preference": cost_preference}
        wallet_summary = None
        error = None

        try:
            chickens, ultimate_pool, gene_pool, ip_pool = build_best_pair_pools(wallet)
            access_expiry = deps["get_wallet_access_expiry_display"](wallet)
            wallet_summary = deps["build_wallet_summary"](
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            if should_generate:
                results = build_best_pair_suggestions(
                    ultimate_pool=ultimate_pool,
                    gene_pool=gene_pool,
                    ip_pool=ip_pool,
                    cost_preference=cost_preference,
                )
        except Exception as exc:
            error = f"Failed to load best pair suggestions: {exc}"

        planner_context = build_planner_context(wallet)

        return render_template(
            "best_pairs.html",
            wallet=wallet,
            cost_preference=cost_preference,
            cost_preference_options=COST_PREFERENCE_OPTIONS,
            generated=should_generate,
            results=results,
            pairs=results.get("pairs") or [],
            mode_counts=results.get("counts") or {},
            planner_queue=planner_context["planner_queue"],
            planner_summary=planner_context["planner_summary"],
            wallet_summary=wallet_summary,
            status=status,
            message=message,
            error=error,
        )

    def best_pairs_planner_fragment():
        wallet = request.args.get("wallet_address", "").strip().lower()
        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        planner_context = build_planner_context(wallet)
        return render_template(
            "partials/best_pairs_planner.html",
            wallet=wallet,
            planner_queue=planner_context["planner_queue"],
            planner_summary=planner_context["planner_summary"],
        )

    def add_all_best_pairs_to_planner():
        wallet = request.form.get("wallet_address", "").strip().lower()
        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        pair_count = deps["safe_int"](request.form.get("pair_count"), 0) or 0
        chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
        chicken_lookup = {
            str(row.get("token_id") or "").strip(): deps["enrich_chicken_media"](row)
            for row in chickens
        }
        queue_rows = deps["get_breeding_planner_queue"](wallet)
        added_count = 0
        skipped_count = 0

        for index in range(pair_count):
            prefix = f"pairs[{index}]"
            mode = str(request.form.get(f"{prefix}[mode]") or "").strip().lower()
            build_type = str(request.form.get(f"{prefix}[build_type]") or "").strip().lower()
            left_token_id = str(request.form.get(f"{prefix}[left_token_id]") or "").strip()
            right_token_id = str(request.form.get(f"{prefix}[right_token_id]") or "").strip()
            pair_quality = str(request.form.get(f"{prefix}[pair_quality]") or "").strip()
            left_item_name = str(request.form.get(f"{prefix}[left_item_name]") or "").strip()
            right_item_name = str(request.form.get(f"{prefix}[right_item_name]") or "").strip()

            left = chicken_lookup.get(left_token_id)
            right = chicken_lookup.get(right_token_id)
            if mode not in {"ultimate", "gene", "ip"} or not left or not right:
                skipped_count += 1
                continue
            if deps["planner_pair_exists"](wallet, left_token_id, right_token_id):
                skipped_count += 1
                continue

            queue_rows.append(
                deps["build_planner_queue_row"](
                    mode=mode,
                    left=left,
                    right=right,
                    left_item={"name": left_item_name, "reason": ""} if left_item_name else None,
                    right_item={"name": right_item_name, "reason": ""} if right_item_name else None,
                    pair_quality=pair_quality,
                    build_type=build_type,
                )
            )
            added_count += 1

        deps["save_breeding_planner_queue"](wallet, queue_rows)
        message = f"Added {added_count} pair(s) to the planner."
        if skipped_count:
            message = f"{message} Skipped {skipped_count} already-added or unavailable pair(s)."
        return redirect(url_for("best_pairs_page", wallet_address=wallet, status="success", message=message))

    app.add_url_rule("/best-pairs", endpoint="best_pairs_page", view_func=best_pairs_page, methods=["GET"])
    app.add_url_rule(
        "/best-pairs/planner-fragment",
        endpoint="best_pairs_planner_fragment",
        view_func=best_pairs_planner_fragment,
        methods=["GET"],
    )
    app.add_url_rule(
        "/best-pairs/add-all",
        endpoint="add_all_best_pairs_to_planner",
        view_func=add_all_best_pairs_to_planner,
        methods=["POST"],
    )
