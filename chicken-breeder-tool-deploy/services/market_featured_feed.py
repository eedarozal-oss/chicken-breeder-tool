from services.db.connection import get_connection
from services.marketplace_listings import fetch_market_listings_for_candidate_ids

DEFAULT_BATCH_SIZE = 200
DEFAULT_TARGET_COUNT = 8
MAX_FEATURED_PRICE_WEI = 999 * 10**18


def safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def safe_lower(value):
    return str(value or "").strip().lower()


def build_market_card_row(candidate_row, market_row):
    candidate_row = dict(candidate_row or {})
    market_row = dict(market_row or {})

    token_id = str(candidate_row.get("token_id") or market_row.get("token_id") or "").strip()
    best_build_name = str(candidate_row.get("best_build_name") or "").strip().lower()
    best_build_count = safe_int(candidate_row.get("best_build_count"))
    best_build_total = safe_int(candidate_row.get("best_build_total"))

    build_display = ""
    if best_build_name and best_build_total > 0:
        build_display = f"{best_build_count}/{best_build_total}"

    return {
        "token_id": token_id,
        "image": str(candidate_row.get("image") or "").strip(),
        "breed_count": safe_int(candidate_row.get("breed_count")),
        "price_display": str(market_row.get("price_display") or "").strip(),
        "price_wei": str(market_row.get("price_wei") or "").strip(),
        "market_url": str(candidate_row.get("market_url") or "").strip(),
        "total_ip": safe_int(candidate_row.get("total_ip")),
        "build_name": best_build_name,
        "build_name_display": best_build_name.title() if best_build_name else "",
        "build_count": best_build_count,
        "build_total": best_build_total,
        "build_display": build_display,
    }


def fetch_market_candidate_batch(mode, offset=0, batch_size=DEFAULT_BATCH_SIZE):
    mode_key = safe_lower(mode)

    if mode_key == "ip":
        where_sql = "WHERE qualifies_ip = 1 AND COALESCE(market_skip, 0) = 0"
        order_sql = "ORDER BY total_ip DESC, CAST(token_id AS INTEGER) ASC"
    elif mode_key == "gene":
        where_sql = "WHERE qualifies_gene = 1 AND COALESCE(market_skip, 0) = 0"
        order_sql = """
            ORDER BY
                best_build_count DESC,
                best_build_total DESC,
                total_ip DESC,
                CAST(token_id AS INTEGER) ASC
        """
    elif mode_key == "ultimate":
        where_sql = "WHERE qualifies_ultimate = 1 AND COALESCE(market_skip, 0) = 0"
        order_sql = """
            ORDER BY
                best_build_count DESC,
                total_ip DESC,
                best_build_total DESC,
                CAST(token_id AS INTEGER) ASC
        """
    else:
        raise ValueError(f"Unsupported featured market mode: {mode}")

    sql = f"""
        SELECT
            token_id,
            image,
            breed_count,
            total_ip,
            best_build_name,
            best_build_count,
            best_build_total,
            qualifies_ip,
            qualifies_gene,
            qualifies_ultimate,
            CASE
                WHEN CAST(token_id AS INTEGER) < 2223
                THEN 'https://marketplace.roninchain.com/collections/sabong-saga-genesis/' || token_id
                ELSE 'https://marketplace.roninchain.com/collections/sabong-saga-chickens/' || token_id
            END AS market_url
        FROM chicken_market_candidates
        {where_sql}
        {order_sql}
        LIMIT ? OFFSET ?
    """

    with get_connection() as conn:
        rows = conn.execute(sql, (int(batch_size), int(offset))).fetchall()

    return [dict(row) for row in rows]


def get_featured_market_rows(mode, target_count=DEFAULT_TARGET_COUNT, batch_size=DEFAULT_BATCH_SIZE):
    target_count = max(1, int(target_count or DEFAULT_TARGET_COUNT))
    batch_size = max(1, int(batch_size or DEFAULT_BATCH_SIZE))

    results = []
    offset = 0

    while len(results) < target_count:
        candidate_batch = fetch_market_candidate_batch(
            mode=mode,
            offset=offset,
            batch_size=batch_size,
        )

        if not candidate_batch:
            break

        candidate_by_id = {
            str(row.get("token_id") or "").strip(): row
            for row in candidate_batch
            if str(row.get("token_id") or "").strip()
        }

        candidate_ids_in_order = list(candidate_by_id.keys())

        market_rows = fetch_market_listings_for_candidate_ids(
            candidate_ids=candidate_ids_in_order,
            target_count=target_count - len(results),
            page_size=50,
        )

        for market_row in market_rows:
            token_id = str(market_row.get("token_id") or "").strip()
            candidate_row = candidate_by_id.get(token_id)
            if not candidate_row:
                continue

            price_wei = safe_int(market_row.get("price_wei"), 0)
            if price_wei > MAX_FEATURED_PRICE_WEI:
                continue

            results.append(build_market_card_row(candidate_row, market_row))

            if len(results) >= target_count:
                break

        offset += batch_size

    return {
        "mode": safe_lower(mode),
        "count": len(results),
        "target_count": target_count,
        "rows": results,
    }
