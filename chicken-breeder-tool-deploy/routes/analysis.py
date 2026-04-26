import requests

from flask import redirect, render_template, request, url_for


def register_analysis_routes(app, deps):
    def safe_token(value):
        token = str(value or "").strip()
        return token if token.isdigit() else ""

    def get_build_key(chicken):
        return str(
            (chicken or {}).get("build_type")
            or (chicken or {}).get("gene_build_key")
            or (chicken or {}).get("primary_build")
            or ""
        ).strip().lower()

    def format_instinct_alignment(chicken, build_key=None):
        instinct = str((chicken or {}).get("instinct") or "").strip()
        if not instinct:
            return ""
        build_key = build_key or get_build_key(chicken)
        if not build_key:
            return instinct
        return f"{instinct} ({'+' if deps['build_prefers_instinct'](chicken, build_key) else '-'})"

    def get_weakest_stat_summary(chicken):
        info = deps["get_weakest_ip_stat_info"](chicken or {})
        return {
            "name": info.get("name") or "",
            "label": info.get("label") or "",
            "value": info.get("value"),
            "display": info.get("display") or "",
        }

    def get_partner_stat_answer(target, candidate):
        weakest = get_weakest_stat_summary(target)
        stat_name = weakest.get("name")
        if not stat_name:
            return ""
        value = deps["get_effective_ip_stat"](candidate or {}, stat_name)
        label = weakest.get("label") or stat_name.title()
        return f"{label}: {value}"

    def get_ip_difference(left, right):
        try:
            left_ip = int(left.get("ip"))
            right_ip = int(right.get("ip"))
        except (TypeError, ValueError, AttributeError):
            return ""
        return abs(left_ip - right_ip)

    def safe_int(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def is_genesis_or_legacy_token(token_id):
        try:
            return int(token_id) < 11110
        except (TypeError, ValueError):
            return False

    def parse_live_chicken_payload(data, token_id):
        if not isinstance(data, dict):
            return None

        for key in ("data", "chicken", "item", "nft", "token"):
            nested = data.get(key)
            if isinstance(nested, dict):
                data = nested
                break

        metadata = data.get("metadata")
        if not isinstance(metadata, dict):
            metadata = data

        attributes = metadata.get("attributes") or data.get("attributes") or []
        if isinstance(attributes, dict):
            attributes = [
                {"trait_type": key, "value": value}
                for key, value in attributes.items()
            ]
        if not isinstance(attributes, list):
            return None

        normalized_attributes = []
        for attr in attributes:
            if not isinstance(attr, dict):
                continue
            trait_type = attr.get("trait_type") or attr.get("traitType") or attr.get("name")
            if not trait_type:
                continue
            normalized_attributes.append({
                "trait_type": trait_type,
                "value": attr.get("value"),
            })

        if not normalized_attributes:
            return None

        item = {
            "tokenId": str(data.get("tokenId") or data.get("token_id") or data.get("id") or token_id),
            "contractAddress": data.get("contractAddress") or data.get("contract_address"),
            "tokenURI": data.get("tokenURI") or data.get("token_uri"),
            "metadata": {
                "name": metadata.get("name") or data.get("name"),
                "nickname": metadata.get("nickname") or data.get("nickname"),
                "image": metadata.get("image") or data.get("image"),
                "attributes": normalized_attributes,
            },
        }
        return deps["parse_chicken_record"](None, item)

    def fetch_live_chicken_record(token_id):
        token_id = safe_token(token_id)
        if not token_id:
            return None

        try:
            response = requests.get(
                f"https://chicken-api-ivory.vercel.app/api/{token_id}",
                timeout=(3, 8),
            )
            response.raise_for_status()
            return parse_live_chicken_payload(response.json(), token_id)
        except Exception:
            return None

    def resolve_token_record(token_id, *, allow_api=True):
        token_id = safe_token(token_id)
        if not token_id:
            return None, "invalid"

        record = deps["get_chicken_by_token"](token_id)
        source = "local"
        is_local_egg = bool(record and (
            record.get("is_egg")
            or str(record.get("type") or "").strip().lower() == "egg"
        ))

        needs_api = (
            not record
            or is_local_egg
            or record.get("ip") in (None, "")
            or not str(record.get("parent_1") or record.get("parent_2") or "").strip()
        )
        if allow_api and needs_api:
            parsed = fetch_live_chicken_record(token_id)
            if parsed:
                deps["upsert_chicken"](parsed)
                record = parsed
                source = "api"
            else:
                raw_item = deps["fetch_chicken_by_token"](token_id, deps["CONTRACTS"])
                parsed = deps["parse_chicken_record"](None, raw_item) if raw_item else None
            if parsed and source != "api":
                deps["upsert_chicken"](parsed)
                record = parsed
                source = "api"

        if not record:
            return None, source

        enriched = deps["enrich_chicken_record"](record)
        deps["upsert_chicken"](enriched)
        return deps["enrich_chicken_media"](enriched), source

    def summarize_chicken(chicken):
        if not chicken:
            return None
        build_key = get_build_key(chicken)
        is_egg = bool(
            (chicken or {}).get("is_egg")
            or str((chicken or {}).get("type") or "").strip().lower() == "egg"
        )
        return {
            "token_id": str(chicken.get("token_id") or ""),
            "image": chicken.get("image") or "",
            "build": chicken.get("build_display")
            or chicken.get("ultimate_build_display")
            or str(build_key).title(),
            "build_key": build_key,
            "build_match": chicken.get("build_match_display")
            or chicken.get("ultimate_build_match_display")
            or (
                f"{chicken.get('primary_build_match_count')}/{chicken.get('primary_build_match_total')}"
                if chicken.get("primary_build_match_total") else ""
            ),
            "is_egg": is_egg,
            "ip": chicken.get("ip"),
            "instinct": chicken.get("instinct") or "",
            "instinct_display": format_instinct_alignment(chicken, build_key),
            "breed_count": chicken.get("breed_count"),
            "generation_text": chicken.get("generation_text") or "",
            "weakest_stat": get_weakest_stat_summary(chicken),
        }

    def summarize_primary_build_chicken(chicken):
        if not chicken:
            return None
        summary = summarize_chicken(chicken)
        if summary.get("is_egg"):
            summary["build"] = ""
            summary["build_key"] = ""
            summary["build_match"] = ""
            summary["ip"] = None
            summary["instinct_display"] = ""
            return summary
        primary_build = str((chicken or {}).get("primary_build") or "").strip().lower()
        primary_total = chicken.get("primary_build_match_total")
        if primary_build:
            summary["build"] = primary_build.title()
            summary["build_key"] = primary_build
        if primary_total:
            summary["build_match"] = f"{chicken.get('primary_build_match_count') or 0}/{primary_total}"
        if not summary.get("build_match"):
            evaluations = (chicken or {}).get("primary_build_evaluations") or {}
            candidates = [
                item for item in evaluations.values()
                if isinstance(item, dict) and item.get("match_total")
            ]
            if candidates:
                best = sorted(
                    candidates,
                    key=lambda item: (
                        -(int(item.get("match_count") or 0)),
                        int(item.get("match_total") or 999),
                        str(item.get("build") or item.get("label") or ""),
                    ),
                )[0]
                summary["build"] = best.get("label") or str(best.get("build") or "").title()
                summary["build_key"] = str(best.get("build") or "").strip().lower()
                summary["build_match"] = f"{best.get('match_count') or 0}/{best.get('match_total')}"
        return summary

    def resolve_family_row(row):
        token_id = safe_token((row or {}).get("token_id"))
        if token_id:
            resolved, _source = resolve_token_record(token_id, allow_api=True)
            if resolved:
                return resolved
        if not row:
            return None
        enriched = deps["enrich_chicken_record"](dict(row))
        deps["upsert_chicken"](enriched)
        return deps["enrich_chicken_media"](enriched)

    def collect_token_ids(value):
        found = []
        if isinstance(value, dict):
            for key in ("token_id", "tokenId", "id", "chickenId", "childId", "offspringId"):
                token = safe_token(value.get(key))
                if token:
                    found.append(token)
            for child_value in value.values():
                found.extend(collect_token_ids(child_value))
        elif isinstance(value, list):
            for item in value:
                found.extend(collect_token_ids(item))
        return found

    def fetch_offspring_token_ids(token_id, expected_count=0):
        token_id = safe_token(token_id)
        if not token_id:
            return []

        max_known_id = deps["get_max_known_chicken_token_id"]()
        expected_count = safe_int(expected_count, 0) or 0
        parent_id = safe_int(token_id, 0) or 0
        window_size = 500
        urls = []
        recent_start = max_known_id + 1
        urls.append(
            f"https://chicken-lineage.vercel.app/api/batch?parent={token_id}&start={recent_start}&end={recent_start + window_size}"
        )
        if parent_id:
            scan_limit = max(parent_id + 10_000, max_known_id if max_known_id < parent_id + 10_000 else parent_id)
            scan_limit = min(scan_limit, max_known_id)
            start = parent_id + 1
            while start <= scan_limit:
                end = min(start + window_size - 1, scan_limit)
                urls.append(
                    f"https://chicken-lineage.vercel.app/api/batch?parent={token_id}&start={start}&end={end}"
                )
                start = end + 1
        urls.append(f"https://chicken-api-ivory.vercel.app/api/offspring/{token_id}")
        unique = []
        seen = set()

        for url in urls:
            data = None
            try:
                response = requests.get(url, timeout=(2, 5))
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                data = response.json()
            except Exception:
                continue

            for child_id in collect_token_ids(data):
                if child_id == token_id or child_id in seen:
                    continue
                seen.add(child_id)
                unique.append(child_id)
            if expected_count and len(unique) >= expected_count:
                break
            if unique and "chicken-api-ivory.vercel.app/api/offspring" in url:
                break

        return unique[:20]

    def fetch_missing_offspring_rows(token_id, expected_count=0):
        rows = []
        for child_id in fetch_offspring_token_ids(token_id, expected_count=expected_count):
            child, _source = resolve_token_record(child_id, allow_api=True)
            if child:
                rows.append(child)
        if not rows:
            return []

        refreshed = deps["get_chickens_by_parent_token"](token_id)
        if refreshed:
            merged = {
                str(row.get("token_id") or ""): row
                for row in rows
                if str(row.get("token_id") or "")
            }
            for row in refreshed:
                row_id = str((row or {}).get("token_id") or "")
                if row_id:
                    merged[row_id] = deps["enrich_chicken_media"](row)
            return list(merged.values())
        return rows

    def build_match_summary(mode, selected, row):
        if not row:
            return {
                "mode": mode,
                "candidate": None,
                "quality": "",
                "matched_stat": "",
                "ip_difference": "",
                "build_instinct": "",
            }
        candidate = row.get("candidate") or row.get("right") or {}
        if not candidate:
            return {
                "mode": mode,
                "candidate": None,
                "quality": "",
                "matched_stat": "",
                "ip_difference": "",
                "build_instinct": "",
            }
        candidate_summary = summarize_chicken(candidate)
        candidate_build = row.get("selected_build") or row.get("build_type") or get_build_key(candidate)
        return {
            "mode": mode,
            "candidate": candidate_summary,
            "quality": (
                deps["build_ip_pair_quality"](selected, candidate, row)
                if mode == "IP"
                else deps["build_gene_pair_quality"](row)
                if mode == "Gene"
                else deps["build_ultimate_pair_quality_from_items"](
                    selected,
                    candidate,
                    row.get("selected_build") or row.get("build_type") or get_build_key(selected),
                    row.get("left_item"),
                    row.get("right_item"),
                )
            ),
            "matched_stat": get_partner_stat_answer(selected, candidate),
            "ip_difference": get_ip_difference(selected, candidate),
            "build_instinct": format_instinct_alignment(candidate, candidate_build) if mode != "IP" else "",
        }

    def is_recommendation_pair_allowed(selected, candidate):
        if not selected or not candidate:
            return False

        selected_id = str((selected or {}).get("token_id") or "").strip()
        candidate_id = str((candidate or {}).get("token_id") or "").strip()
        if not selected_id or not candidate_id or selected_id == candidate_id:
            return False

        if deps.get("is_parent_offspring") and deps["is_parent_offspring"](selected, candidate):
            return False

        if deps.get("is_full_siblings") and deps["is_full_siblings"](selected, candidate):
            return False

        if deps.get("is_generation_gap_allowed") and not deps["is_generation_gap_allowed"](
            selected,
            candidate,
            max_gap=deps["match_settings"].get("max_generation_gap", 3),
        ):
            return False

        return True

    def filter_recommendation_pool(target, candidates):
        return [
            row for row in candidates
            if is_recommendation_pair_allowed(target, row)
        ]

    def first_allowed_recommendation(selected, rows):
        for row in rows or []:
            candidate = (row or {}).get("candidate") or (row or {}).get("right") or {}
            if is_recommendation_pair_allowed(selected, candidate):
                return row
        return None

    def build_recommendations(target, wallet_chickens):
        wallet_pool = [
            deps["enrich_chicken_media"](dict(row or {}))
            for row in (wallet_chickens or [])
            if deps["is_breedable"](row)
            and str(row.get("token_id") or "") != str(target.get("token_id") or "")
        ]
        wallet_pool = filter_recommendation_pool(target, wallet_pool)

        ip_rows = deps["find_potential_matches"](target, wallet_pool, settings=deps["match_settings"])
        ip_rows = [
            row for row in ip_rows
            if row.get("evaluation", {}).get("is_ip_recommended")
            and row.get("evaluation", {}).get("is_breed_count_recommended")
            and deps["pair_has_usable_ip_items"](target, row.get("candidate"))
        ]
        ip_rows = deps["sort_ip_match_rows"](target, ip_rows)
        ip_row = first_allowed_recommendation(target, ip_rows)

        gene_target = deps["enrich_gene_available_display"](target)
        gene_pool = [deps["enrich_gene_available_display"](row) for row in wallet_pool]
        gene_rows = deps["build_gene_potential_matches_strict"](gene_target, gene_pool)
        gene_row = first_allowed_recommendation(gene_target, gene_rows)

        ultimate_target = deps["enrich_ultimate_display"](target)
        ultimate_pool = [deps["enrich_ultimate_display"](row) for row in wallet_pool]
        ultimate_rows = deps["filter_and_sort_ultimate_candidates"](
            ultimate_target,
            ultimate_pool,
            include_lower_values=True,
        )
        ultimate_row = first_allowed_recommendation(ultimate_target, ultimate_rows)

        return [
            build_match_summary("IP", target, ip_row),
            build_match_summary("Gene", gene_target, gene_row),
            build_match_summary("Ultimate", ultimate_target, ultimate_row),
        ]

    def build_family_context(target):
        token_id = str(target.get("token_id") or "")
        parent_ids = [
            safe_token(target.get("parent_1")),
            safe_token(target.get("parent_2")),
        ]
        parent_rows = []
        for parent_id in [item for item in parent_ids if item]:
            parent, source = resolve_token_record(parent_id, allow_api=True)
            parent_rows.append({
                "token_id": parent_id,
                "source": source,
                "chicken": summarize_primary_build_chicken(parent) if parent else {"token_id": parent_id},
            })

        offspring_source = "local"
        offspring = [
            row for row in [
                resolve_family_row(row)
                for row in deps["get_chickens_by_parent_token"](token_id)
            ]
            if row
        ]
        offspring_by_id = {
            str(row.get("token_id") or ""): row
            for row in offspring
            if str(row.get("token_id") or "")
        }
        for static_row in deps["get_static_chickens_by_parent_token"](token_id):
            static_id = str((static_row or {}).get("token_id") or "")
            if not static_id or static_id in offspring_by_id:
                continue
            resolved = resolve_family_row(static_row)
            if resolved:
                offspring_by_id[static_id] = resolved
        if len(offspring_by_id) > len(offspring):
            offspring = sorted(
                offspring_by_id.values(),
                key=lambda row: safe_int(row.get("token_id"), 0),
                reverse=True,
            )
            offspring_source = "static"

        expected_offspring_count = safe_int(target.get("breed_count"), 0) or 0
        if len(offspring) < expected_offspring_count:
            api_offspring = fetch_missing_offspring_rows(token_id, expected_count=expected_offspring_count)
            if api_offspring:
                offspring_by_id = {
                    str(row.get("token_id") or ""): row
                    for row in offspring
                    if str(row.get("token_id") or "")
                }
                for row in [resolve_family_row(row) for row in api_offspring]:
                    if not row:
                        continue
                    row_id = str(row.get("token_id") or "")
                    if row_id:
                        offspring_by_id[row_id] = row
                offspring = sorted(
                    offspring_by_id.values(),
                    key=lambda row: safe_int(row.get("token_id"), 0),
                    reverse=True,
                )
                offspring_source = "api"
        partner_lookup = {}
        partner_children = {}
        for child in offspring:
            other_parent = ""
            if str(child.get("parent_1") or "") == token_id:
                other_parent = safe_token(child.get("parent_2"))
            elif str(child.get("parent_2") or "") == token_id:
                other_parent = safe_token(child.get("parent_1"))
            if other_parent:
                partner_lookup[other_parent] = other_parent
                partner_children.setdefault(other_parent, []).append(child)

        partners = []
        partner_rows = []
        for partner_id in sorted(partner_lookup, key=lambda value: int(value)):
            partner, source = resolve_token_record(partner_id, allow_api=True)
            partner_summary = summarize_primary_build_chicken(partner) if partner else {"token_id": partner_id}
            partners.append({
                "token_id": partner_id,
                "source": source,
                "chicken": partner_summary,
            })
            partner_rows.append({
                "partner": partner_summary,
                "offspring": [summarize_primary_build_chicken(row) for row in partner_children.get(partner_id, [])],
            })

        return {
            "is_genesis_or_legacy": is_genesis_or_legacy_token(token_id),
            "parents": parent_rows,
            "partners": partners,
            "partner_rows": partner_rows,
            "offspring": [summarize_primary_build_chicken(row) for row in offspring],
            "offspring_source": offspring_source,
        }

    def chicken_analysis_page():
        wallet = request.args.get("wallet_address", "").strip().lower()
        token_id = safe_token(request.args.get("chicken_id"))
        error = None
        target = None
        analysis = None
        recommendations = []
        family = {"parents": [], "partners": [], "partner_rows": [], "offspring": [], "offspring_source": "local"}

        if not deps["require_authorized_wallet"](wallet):
            return redirect(url_for("index"))

        donor_access = deps["has_active_payment_access_in_db"](wallet)
        if not donor_access:
            error = "This exclusive analysis tool is available to active supporters."
        elif token_id:
            target, source = resolve_token_record(token_id, allow_api=True)
            if not target:
                error = f"Chicken #{token_id} could not be found."
            else:
                wallet_chickens = deps["get_wallet_chickens"](wallet, ensure_loaded=True)
                display_target = deps["enrich_gene_available_display"](target)
                instinct_aligned = deps["build_prefers_instinct"](display_target, display_target.get("build_type"))
                analysis = {
                    "source": source,
                    "chicken": summarize_chicken(display_target),
                    "instinct_aligned": instinct_aligned,
                    "build_match": display_target.get("build_match_display") or "",
                    "weakest_stat": get_weakest_stat_summary(display_target),
                    "state": target.get("state") or "",
                    "type": target.get("type") or "",
                }
                recommendations = build_recommendations(target, wallet_chickens)
                family = build_family_context(target)

        return render_template(
            "chicken_analysis.html",
            wallet=wallet,
            token_id=token_id,
            donor_access=donor_access,
            analysis=analysis,
            recommendations=recommendations,
            family=family,
            error=error,
        )

    app.add_url_rule(
        "/chicken-analysis",
        endpoint="chicken_analysis_page",
        view_func=chicken_analysis_page,
        methods=["GET"],
    )
