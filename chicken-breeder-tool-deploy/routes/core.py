from datetime import datetime, timezone

from flask import redirect, render_template, request, send_file, session, url_for


def register_core_routes(app, deps):
    def index():
        error = None
        success = None
        redirect_url = None
        wallet = request.values.get("wallet_address", "").strip().lower()

        if request.method == "POST":
            if not wallet:
                error = "Enter a wallet address to continue."
            elif not deps["is_valid_wallet"](wallet):
                error = "Enter a valid 0x wallet address."
            else:
                try:
                    if not deps["has_wallet_access"](wallet):
                        error = "This wallet has no active access. Send at least 0.1 RON to 0x9933199Fa3D96D7696d2B2A4CfBa48d99E47a079 to gain access."
                    else:
                        deps["set_authorized_wallet"](wallet)
                        deps["get_wallet_chickens"](wallet, ensure_loaded=True)

                        expiry_display = deps["get_wallet_access_expiry_display"](wallet)
                        if expiry_display:
                            success = f"Wallet approved. Access is active until {expiry_display}."
                        else:
                            success = "Wallet approved. Access is active for 30 days."

                        redirect_url = url_for("landing_page", wallet_address=wallet)
                except Exception as exc:
                    error = f"Failed to validate wallet access: {exc}"
        elif wallet:
            if deps["is_valid_wallet"](wallet) and deps["is_authorized_wallet"](wallet):
                return redirect(url_for("landing_page", wallet_address=wallet))

        return render_template(
            "index.html",
            wallet=wallet,
            error=error,
            success=success,
            redirect_url=redirect_url,
        )

    def owner_grant_access_page():
        action = request.values.get("action", "grant_access").strip()
        wallet = request.values.get("wallet_address", "").strip().lower()
        owner_password = request.values.get("owner_password", "").strip()
        duration_days = request.values.get("duration_days", "").strip()
        error = None
        success = None
        sync_results = []

        if request.method == "POST":
            parsed_days = deps["safe_int"](duration_days)
            locked, remaining_minutes = deps["is_owner_admin_locked"]()

            if not deps["get_owner_admin_password"]():
                error = "Owner admin password is not configured on the server."
            elif locked:
                error = f"Owner access is temporarily locked. Try again in {remaining_minutes} minute(s)."
            elif not deps["owner_password_is_valid"](owner_password):
                deps["register_owner_admin_failure"]()
                error = "Invalid owner password."
            elif action == "sync_static_cache":
                deps["clear_owner_admin_failures"]()
                try:
                    sync_results = deps["sync_static_export_tables_to_main_db"]()
                    if sync_results:
                        table_summary = ", ".join(
                            f"{row['table']} ({row['row_count']} rows)"
                            for row in sync_results
                        )
                        success = f"Static cache sync completed: {table_summary}."
                    else:
                        success = "Static cache sync completed, but no tables were copied."
                    owner_password = ""
                except Exception as exc:
                    error = f"Failed to sync static cache DB: {exc}"
            else:
                deps["clear_owner_admin_failures"]()
                if not wallet:
                    error = "Enter a wallet address."
                elif not deps["is_valid_wallet"](wallet):
                    error = "Enter a valid 0x wallet address."
                elif parsed_days is None or parsed_days <= 0:
                    error = "Duration must be a whole number greater than 0."
                else:
                    try:
                        deps["grant_manual_access"](
                            wallet=wallet,
                            notes=f"Owner manual grant for {parsed_days} day(s)",
                            duration_days=parsed_days,
                        )
                        expiry_display = deps["get_wallet_access_expiry_display"](wallet)
                        success = f"Access granted to {wallet} for {parsed_days} day(s). Active until {expiry_display}."
                        duration_days = ""
                        owner_password = ""
                    except Exception as exc:
                        error = f"Failed to grant access: {exc}"

        access_rows = deps["format_wallet_access_rows"](deps["get_wallet_access_rows"](limit=300))

        return render_template(
            "admin_whitelist.html",
            wallet=wallet,
            duration_days=duration_days,
            owner_password=owner_password,
            error=error,
            success=success,
            sync_results=sync_results,
            access_rows=access_rows,
            static_export_db_path=deps["static_export_db_path"],
        )

    def landing_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        breedable_chickens = []
        error = None
        success = None
        access_expiry = None
        refresh_status = str(request.args.get("refresh_status") or "").strip().lower()
        refresh_message = str(request.args.get("refresh_message") or "").strip()
        wallet_summary = None

        if refresh_status == "success" and refresh_message:
            success = refresh_message
        elif refresh_status == "error" and refresh_message:
            error = refresh_message

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        try:
            chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
            breedable_chickens = [deps["enrich_chicken_media"](row) for row in chickens if deps["is_breedable"](row)]
            access_expiry = deps["get_wallet_access_expiry_display"](wallet)
            wallet_summary = deps["build_wallet_summary"](
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
        except Exception as exc:
            error = f"Failed to load wallet data: {exc}"

        return render_template(
            "landing.html",
            wallet=wallet,
            breedable_count=len(breedable_chickens),
            access_expiry=access_expiry,
            wallet_summary=wallet_summary,
            error=error,
            success=success,
        )

    def refresh_wallet():
        wallet = request.form.get("wallet_address", "").strip().lower()

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        cooldown_key = f"wallet_refresh_last_clicked_{wallet}"
        now = datetime.now(timezone.utc)
        last_clicked_raw = session.get(cooldown_key)

        if last_clicked_raw:
            try:
                last_clicked = datetime.fromisoformat(last_clicked_raw)
                seconds_since = (now - last_clicked).total_seconds()
                if seconds_since < 60:
                    remaining = max(1, int(60 - seconds_since))
                    return redirect(url_for(
                        "landing_page",
                        wallet_address=wallet,
                        refresh_status="error",
                        refresh_message=f"Wallet was refreshed recently. Please wait {remaining} second(s) before refreshing again.",
                    ))
            except Exception:
                pass

        try:
            deps["sync_wallet_data"](wallet)
            session[cooldown_key] = now.isoformat()
            return redirect(url_for(
                "landing_page",
                wallet_address=wallet,
                refresh_status="success",
                refresh_message="Wallet refreshed successfully.",
            ))
        except Exception as exc:
            return redirect(url_for(
                "landing_page",
                wallet_address=wallet,
                refresh_status="error",
                refresh_message=f"Failed to refresh wallet data: {exc}",
            ))

    def available_chickens_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        breedable_chickens = []
        error = None
        wallet_summary = None
        available_dashboard = None

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        try:
            chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)

            for row in chickens:
                if not deps["is_breedable"](row):
                    continue

                chicken = deps["enrich_chicken_media"](row)
                best_gene_build = deps["get_best_available_gene_build_info"](chicken)
                chicken["available_build_display"] = best_gene_build.get("build_label") or ""
                chicken["available_build_count_display"] = best_gene_build.get("build_count_display") or ""
                breedable_chickens.append(chicken)

            access_expiry = deps["get_wallet_access_expiry_display"](wallet)
            wallet_summary = deps["build_wallet_summary"](
                wallet=wallet,
                chickens=chickens,
                access_expiry=access_expiry,
            )
            available_dashboard = deps["build_available_chickens_dashboard"](
                chickens=chickens,
                breedable_chickens=breedable_chickens,
            )
        except Exception as exc:
            error = f"Failed to load available chickens: {exc}"

        return render_template(
            "available_chickens.html",
            wallet=wallet,
            breedable_chickens=breedable_chickens,
            wallet_summary=wallet_summary,
            available_dashboard=available_dashboard,
            error=error,
        )

    def inventory():
        wallet = request.args.get("wallet_address", "").strip().lower()
        chickens = []
        error = None

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        try:
            chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
        except Exception as exc:
            error = f"Failed to load inventory: {exc}"

        return render_template(
            "inventory.html",
            wallet=wallet,
            chickens=chickens,
            error=error,
        )

    app.add_url_rule("/", endpoint="index", view_func=index, methods=["GET", "POST"])
    app.add_url_rule(
        deps["owner_whitelist_route"],
        endpoint="owner_grant_access_page",
        view_func=owner_grant_access_page,
        methods=["GET", "POST"],
    )
    app.add_url_rule("/landing", endpoint="landing_page", view_func=landing_page, methods=["GET"])
    app.add_url_rule("/refresh-wallet", endpoint="refresh_wallet", view_func=refresh_wallet, methods=["POST"])
    app.add_url_rule(
        "/available-chickens",
        endpoint="available_chickens_page",
        view_func=available_chickens_page,
        methods=["GET"],
    )
    app.add_url_rule("/inventory", endpoint="inventory", view_func=inventory, methods=["GET"])
