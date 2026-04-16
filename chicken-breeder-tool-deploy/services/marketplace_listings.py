from decimal import Decimal, InvalidOperation
from services.market_listing_cache import (
    get_cached_market_listing_page,
    save_cached_market_listing_page,
)

import requests

RONIN_MARKET_GRAPHQL_URL = "https://marketplace-graphql.skymavis.com/graphql"

GENESIS_TOKEN_ADDRESS = "0xee9436518030616bc315665678738a4348463df4"
CHICKEN_TOKEN_ADDRESS = "0x322b3d98ddbd589dc2e8dd83659bb069828231e0"


def safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def wei_to_ron_display(wei_value):
    try:
        wei_num = Decimal(str(wei_value or "0"))
        ron_value = wei_num / Decimal("1000000000000000000")
        text = format(ron_value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return f"{text} RON"
    except (InvalidOperation, ValueError, TypeError):
        return ""


def get_market_token_address(token_id):
    token_num = safe_int(token_id, 0)
    if 0 < token_num < 2223:
        return GENESIS_TOKEN_ADDRESS
    return CHICKEN_TOKEN_ADDRESS


def post_market_graphql(payload, timeout=20):
    response = requests.post(
        RONIN_MARKET_GRAPHQL_URL,
        json=payload,
        timeout=timeout,
        headers={
            "Content-Type": "application/json",
            "Origin": "https://marketplace.roninchain.com",
            "Referer": "https://marketplace.roninchain.com/",
        },
    )

    try:
        data = response.json()
    except Exception:
        response.raise_for_status()
        raise

    if response.status_code >= 400:
        raise ValueError(f"Marketplace GraphQL HTTP {response.status_code}: {data}")

    if data.get("errors"):
        raise ValueError(f"Marketplace GraphQL error: {data['errors']}")

    return data


def build_tokens_list_payload(token_address, offset=0, size=50, sort="PriceAsc"):
    query = """
    query GetERC721TokensList(
      $tokenAddress: String,
      $slug: String,
      $owner: String,
      $auctionType: AuctionType,
      $criteria: [SearchCriteria!],
      $from: Int!,
      $size: Int!,
      $sort: SortBy,
      $name: String,
      $priceRange: InputRange,
      $rangeCriteria: [RangeSearchCriteria!],
      $excludeAddress: String
    ) {
      erc721Tokens(
        tokenAddress: $tokenAddress
        slug: $slug
        owner: $owner
        auctionType: $auctionType
        criteria: $criteria
        from: $from
        size: $size
        sort: $sort
        name: $name
        priceRange: $priceRange
        rangeCriteria: $rangeCriteria
        excludeAddress: $excludeAddress
      ) {
        total
        results {
          tokenAddress
          tokenId
          slug
          owner
          name
          order {
            id
            currentPrice
            basePrice
            orderStatus
            __typename
          }
          __typename
        }
        __typename
      }
    }
    """

    return {
        "operationName": "GetERC721TokensList",
        "query": query,
        "variables": {
            "from": int(offset),
            "auctionType": "Sale",
            "size": int(size),
            "sort": sort,
            "rangeCriteria": [],
            "tokenAddress": token_address,
        },
    }

def normalize_market_list_row(row):
    row = dict(row or {})
    token_id = str(row.get("tokenId") or "").strip()
    order = row.get("order") or {}

    price_wei = str(
        order.get("currentPrice")
        or order.get("basePrice")
        or ""
    ).strip()

    order_status = str(order.get("orderStatus") or "").strip().upper()

    return {
        "token_id": token_id,
        "token_address": str(row.get("tokenAddress") or "").strip().lower(),
        "is_listed": bool(order) and order_status == "OPEN",
        "order_status": order_status,
        "price_wei": price_wei,
        "price_display": wei_to_ron_display(price_wei) if price_wei else "",
        "raw_row": row,
    }


def fetch_market_sale_page(token_address, offset=0, size=50, sort="PriceAsc", timeout=20):
    cached = get_cached_market_listing_page(
        token_address=token_address,
        page_offset=offset,
        page_size=size,
    )
    if cached:
        return cached

    payload = build_tokens_list_payload(
        token_address=token_address,
        offset=offset,
        size=size,
        sort=sort,
    )
    data = post_market_graphql(payload, timeout=timeout)

    listing_root = (data.get("data") or {}).get("erc721Tokens") or {}
    total = safe_int(listing_root.get("total"), 0)
    results = listing_root.get("results") or []

    rows = [normalize_market_list_row(row) for row in results]

    page = {
        "total": total,
        "count": len(rows),
        "offset": int(offset),
        "size": int(size),
        "rows": rows,
    }

    save_cached_market_listing_page(
        token_address=token_address,
        page_offset=offset,
        page_size=size,
        payload=page,
    )

    return page


def fetch_market_listings_for_candidate_ids(
    candidate_ids,
    target_count=8,
    page_size=50,
    timeout=20,
    sort="PriceAsc",
):
    ordered_candidate_ids = [
        str(token_id).strip()
        for token_id in (candidate_ids or [])
        if str(token_id).strip()
    ]
    candidate_id_set = set(ordered_candidate_ids)
    target_count = max(1, int(target_count or 8))
    page_size = max(1, int(page_size or 50))

    if not ordered_candidate_ids:
        return []

    grouped_ids = {
        GENESIS_TOKEN_ADDRESS: {
            token_id for token_id in ordered_candidate_ids
            if 0 < safe_int(token_id, 0) < 2223
        },
        CHICKEN_TOKEN_ADDRESS: {
            token_id for token_id in ordered_candidate_ids
            if safe_int(token_id, 0) >= 2223
        },
    }

    matched_rows = {}
    checked_contracts = set()

    for token_address in (GENESIS_TOKEN_ADDRESS, CHICKEN_TOKEN_ADDRESS):
        contract_ids = grouped_ids.get(token_address) or set()
        if not contract_ids:
            continue

        checked_contracts.add(token_address)
        offset = 0

        while len(matched_rows) < target_count:
            page = fetch_market_sale_page(
                token_address=token_address,
                offset=offset,
                size=page_size,
                sort=sort,
                timeout=timeout,
            )

            page_rows = page.get("rows") or []
            if not page_rows:
                break

            for row in page_rows:
                token_id = str(row.get("token_id") or "").strip()
                if not token_id:
                    continue
                if token_id not in contract_ids:
                    continue
                if not row.get("is_listed"):
                    continue
                if token_id not in matched_rows:
                    matched_rows[token_id] = row

                if len(matched_rows) >= target_count:
                    break

            offset += page_size

            total = safe_int(page.get("total"), 0)
            if total and offset >= total:
                break

    ordered = []
    for token_id in ordered_candidate_ids:
        if token_id in matched_rows:
            ordered.append(matched_rows[token_id])

    return ordered[:target_count]
