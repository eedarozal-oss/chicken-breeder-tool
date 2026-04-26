from datetime import datetime, timezone

from flask import redirect, render_template, request, send_file, url_for

from services.planner_bookmarklet import MAX_MASS_BREEDING_PAIRS


def register_planner_routes(app, deps):
    def add_to_breeding_planner():
        wallet = request.form.get("wallet_address", "").strip().lower()
        mode = str(request.form.get("mode") or "").strip().lower()
        return_endpoint = str(request.form.get("return_endpoint") or "match_ip_page").strip()
        build_type = str(request.form.get("build_type") or "").strip().lower()
        left_token_id = str(request.form.get("left_token_id") or "").strip()
        right_token_id = str(request.form.get("right_token_id") or "").strip()
        pair_quality = str(request.form.get("pair_quality") or "").strip()
        left_item_name = str(request.form.get("left_item_name") or "").strip()
        right_item_name = str(request.form.get("right_item_name") or "").strip()

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        redirect_kwargs = {"wallet_address": wallet}
        selected_token_id = str(request.form.get("selected_token_id") or "").strip()
        sort_by = str(request.form.get("sort_by") or "").strip()
        sort_dir = str(request.form.get("sort_dir") or "").strip()
        if selected_token_id:
            redirect_kwargs["selected_token_id"] = selected_token_id
        if sort_by:
            redirect_kwargs["sort_by"] = sort_by
        if sort_dir:
            redirect_kwargs["sort_dir"] = sort_dir
        if mode == "ip":
            if request.form.get("min_ip") not in (None, ""):
                redirect_kwargs["min_ip"] = request.form.get("min_ip")
            if request.form.get("ip_diff") not in (None, ""):
                redirect_kwargs["ip_diff"] = request.form.get("ip_diff")
            if str(request.form.get("ninuno_100_only") or "").strip() in {"1", "true", "on", "yes"}:
                redirect_kwargs["ninuno_100_only"] = 1
        elif mode == "gene":
            if build_type:
                redirect_kwargs["build_type"] = build_type
            if str(request.form.get("ninuno_100_only") or "").strip() in {"1", "true", "on", "yes"}:
                redirect_kwargs["ninuno_100_only"] = 1

        chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
        chicken_lookup = {
            str(row.get("token_id") or ""): deps["enrich_chicken_media"](row)
            for row in chickens
        }
        left = chicken_lookup.get(left_token_id)
        right = chicken_lookup.get(right_token_id)
        if left and right and not deps["planner_pair_exists"](wallet, left_token_id, right_token_id):
            left_item = {"name": left_item_name, "reason": ""} if left_item_name else None
            right_item = {"name": right_item_name, "reason": ""} if right_item_name else None
            queue_rows = deps["get_breeding_planner_queue"](wallet)
            queue_rows.append(
                deps["build_planner_queue_row"](
                    mode=mode,
                    left=left,
                    right=right,
                    left_item=left_item,
                    right_item=right_item,
                    pair_quality=pair_quality,
                    build_type=build_type,
                )
            )
            deps["save_breeding_planner_queue"](wallet, queue_rows)
        redirect_kwargs["skip_auto_open"] = 1
        return redirect(url_for(return_endpoint, **redirect_kwargs))

    def remove_from_breeding_planner():
        wallet = request.form.get("wallet_address", "").strip().lower()
        return_endpoint = str(request.form.get("return_endpoint") or "match_ip_page").strip()
        pair_key = str(request.form.get("pair_key") or "").strip()
        queue_rows = [
            row for row in deps["get_breeding_planner_queue"](wallet)
            if str(row.get("pair_key") or "") != pair_key
        ]
        deps["save_breeding_planner_queue"](wallet, queue_rows)
        if return_endpoint == "planner_modal":
            return redirect(url_for("match_ip_page", wallet_address=wallet, skip_auto_open=1))
        redirect_kwargs = {"wallet_address": wallet, "skip_auto_open": 1}
        for key in ["selected_token_id", "sort_by", "sort_dir", "build_type", "min_ip", "ip_diff"]:
            value = request.form.get(key)
            if value not in (None, ""):
                redirect_kwargs[key] = value
        if str(request.form.get("ninuno_100_only") or "").strip() in {"1", "true", "on", "yes"}:
            redirect_kwargs["ninuno_100_only"] = 1
        return redirect(url_for(return_endpoint, **redirect_kwargs))

    def export_breeding_planner():
        wallet = request.args.get("wallet_address", "").strip().lower()
        if wallet and not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))
        output = deps["export_breeding_planner_excel"](deps["get_breeding_planner_queue"](wallet))
        filename = f"breeding_planner_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def planner_items_check():
        wallet = request.args.get("wallet_address", "").strip().lower()
        source_page = str(request.args.get("source_page") or "").strip().lower()

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        error = None
        planner_queue = deps["get_breeding_planner_queue"](wallet)
        script_pair_limit = MAX_MASS_BREEDING_PAIRS
        script_queue = list(planner_queue or [])[:script_pair_limit]
        script_pair_count = len(script_queue)
        skipped_pair_count = max(0, len(planner_queue or []) - script_pair_count)
        summary = {
            "overall_status": "unknown",
            "all_available": False,
            "has_unknown": True,
            "total_item_types": 0,
            "total_required_count": 0,
            "total_missing_count": 0,
            "items": [],
            "missing_items": [],
            "ready_items": [],
            "wallet_address": wallet,
        }
        per_pair_status_rows = []
        wallet_summary = None

        try:
            chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
            access_expiry = deps["get_wallet_access_expiry_display"](wallet)
            wallet_summary = deps["build_wallet_summary"](
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            summary = deps["build_wallet_planner_item_requirements_summary"](
                wallet_address=wallet,
                queue_rows=script_queue,
            )
            inventory_lookup = deps["build_wallet_inventory_lookup"](wallet)
            per_pair_status_rows = [
                deps["build_per_pair_item_status"](row, inventory_lookup)
                for row in script_queue
            ]
        except Exception as exc:
            error = f"Failed to check planner items: {exc}"

        return render_template(
            "planner_items_check.html",
            wallet=wallet,
            wallet_summary=wallet_summary,
            planner_queue=planner_queue,
            script_queue=script_queue,
            script_pair_limit=script_pair_limit,
            script_pair_count=script_pair_count,
            skipped_pair_count=skipped_pair_count,
            planner_summary=deps["build_planner_summary"](planner_queue),
            item_check_summary=summary,
            per_pair_status_rows=per_pair_status_rows,
            source_page=source_page,
            error=error,
        )

    def planner_script_generate():
        wallet = request.args.get("wallet_address", "").strip().lower()
        source_page = str(request.args.get("source_page") or "").strip().lower()

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        planner_queue = deps["get_breeding_planner_queue"](wallet)
        script_pair_limit = MAX_MASS_BREEDING_PAIRS
        script_queue = list(planner_queue or [])[:script_pair_limit]
        script_pair_count = len(script_queue)
        skipped_pair_count = max(0, len(planner_queue or []) - script_pair_count)
        wallet_summary = None
        error = None
        summary = {
            "overall_status": "unknown",
            "all_available": False,
            "has_unknown": True,
            "total_item_types": 0,
            "total_required_count": 0,
            "total_missing_count": 0,
            "items": [],
            "missing_items": [],
            "ready_items": [],
            "wallet_address": wallet,
        }
        bookmarklet_code = ""
        inventory_name_lookup = {}

        script_mode = str(request.args.get("script_mode") or "full").strip().lower()
        if script_mode not in {"full", "partial", "no_items"}:
            script_mode = "full"

        try:
            chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
            access_expiry = deps["get_wallet_access_expiry_display"](wallet)
            wallet_summary = deps["build_wallet_summary"](
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            summary = deps["build_wallet_planner_item_requirements_summary"](
                wallet_address=wallet,
                queue_rows=planner_queue,
            )

            if script_mode == "partial":
                inventory_name_lookup = deps["build_bookmarklet_inventory_name_lookup"](wallet)

            if script_mode == "full" and summary.get("overall_status") != "ready":
                if source_page == "best_pairs":
                    return redirect(url_for("planner_items_check", wallet_address=wallet, source_page="best_pairs"))
                if source_page == "gene":
                    return redirect(url_for("planner_items_check", wallet_address=wallet, source_page="gene"))
                if source_page == "ultimate":
                    return redirect(url_for("planner_items_check", wallet_address=wallet, source_page="ultimate"))
                return redirect(url_for("planner_items_check", wallet_address=wallet, source_page="ip"))

            bookmarklet_code = deps["build_apex_breeder_bookmarklet_code"](
                planner_queue,
                script_mode=script_mode,
                inventory_name_lookup=inventory_name_lookup,
                max_pairs=script_pair_limit,
            )
        except Exception as exc:
            error = f"Failed to generate script page: {exc}"

        return render_template(
            "planner_script_generate.html",
            wallet=wallet,
            source_page=source_page,
            wallet_summary=wallet_summary,
            planner_queue=planner_queue,
            script_queue=script_queue,
            script_pair_limit=script_pair_limit,
            script_pair_count=script_pair_count,
            skipped_pair_count=skipped_pair_count,
            planner_summary=deps["build_planner_summary"](planner_queue),
            item_check_summary=summary,
            bookmarklet_code=bookmarklet_code,
            script_mode=script_mode,
            error=error,
        )

    app.add_url_rule("/planner/add", endpoint="add_to_breeding_planner", view_func=add_to_breeding_planner, methods=["POST"])
    app.add_url_rule("/planner/remove", endpoint="remove_from_breeding_planner", view_func=remove_from_breeding_planner, methods=["POST"])
    app.add_url_rule("/planner/export", endpoint="export_breeding_planner", view_func=export_breeding_planner, methods=["GET"])
    app.add_url_rule("/planner/items-check", endpoint="planner_items_check", view_func=planner_items_check, methods=["GET"])
    app.add_url_rule("/planner/script-generate", endpoint="planner_script_generate", view_func=planner_script_generate, methods=["GET"])
