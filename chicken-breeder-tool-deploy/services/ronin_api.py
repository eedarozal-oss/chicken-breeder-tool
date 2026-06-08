import requests
from urllib.parse import urlencode

BLOCKSCOUT_BASE_URL = "https://explorer.roninchain.com/api/v2"
SKYNET_BASE_URL = "https://skynet-api.roninchain.com/ronin/explorer/v2"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def normalize_contract_address(value: str) -> str:
    return str(value or "").strip().lower()


def normalize_blockscout_nft_item(item: dict, fallback_contract_address: str = "") -> dict:
    token = item.get("token") or {}
    metadata = item.get("metadata") or {}
    contract_address = token.get("address_hash") or fallback_contract_address

    return {
        "tokenId": str(item.get("id") or item.get("token_id") or "").strip(),
        "contractAddress": normalize_contract_address(contract_address),
        "tokenName": token.get("name"),
        "tokenSymbol": token.get("symbol"),
        "tokenStandard": token.get("type") or item.get("token_type"),
        "tokenURI": item.get("token_uri") or item.get("external_app_url"),
        "balance": str(item.get("value") or "1"),
        "metadata": metadata,
    }


def normalize_blockscout_balance_item(item: dict, fallback_contract_address: str = "") -> dict:
    token = item.get("token") or {}
    token_instance = item.get("token_instance") or {}
    contract_address = token.get("address_hash") or fallback_contract_address
    token_id = item.get("token_id") or token_instance.get("id")

    normalized = {
        "tokenId": str(token_id or "").strip(),
        "contractAddress": normalize_contract_address(contract_address),
        "tokenName": token.get("name"),
        "tokenSymbol": token.get("symbol"),
        "tokenStandard": token.get("type"),
        "tokenURI": token_instance.get("token_uri") or token_instance.get("external_app_url"),
        "balance": str(item.get("value") or "0"),
    }

    metadata = token_instance.get("metadata")
    if isinstance(metadata, dict):
        normalized["metadata"] = metadata

    return normalized


def blockscout_get_json(url, timeout=(5, 10)):
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def skynet_get_json(url, timeout=(5, 5)):
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def skynet_post_json(url, payload, timeout=(5, 10)):
    response = requests.post(url, headers=HEADERS, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_blockscout_balances(wallet_address: str, contract_address: str):
    wallet_address = normalize_contract_address(wallet_address)
    contract_address = normalize_contract_address(contract_address)
    if not wallet_address or not contract_address:
        return []

    url = f"{BLOCKSCOUT_BASE_URL}/addresses/{wallet_address}/token-balances"
    data = blockscout_get_json(url)
    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("value", []) if isinstance(data, dict) else []

    result = []
    for row in rows or []:
        token = row.get("token") or {}
        row_contract = normalize_contract_address(token.get("address_hash"))
        if row_contract != contract_address:
            continue

        normalized = normalize_blockscout_balance_item(row, fallback_contract_address=contract_address)
        if normalized.get("tokenId"):
            result.append(normalized)

    return result


def fetch_skynet_balances(wallet_address: str, contract_address: str, limit: int = 25):
    offset = 0
    all_items = []

    while True:
        url = (
            f"{SKYNET_BASE_URL}/accounts/{wallet_address}/balances/{contract_address}"
            f"?limit={limit}&offset={offset}"
        )

        data = skynet_get_json(url, timeout=(5, 5))

        items = data.get("result", {}).get("items", [])
        if not items:
            break

        all_items.extend(items)

        if len(items) < limit:
            break

        offset += limit

    return all_items


def fetch_balances(wallet_address: str, contract_address: str, limit: int = 25):
    try:
        return fetch_blockscout_balances(wallet_address, contract_address)
    except Exception:
        return fetch_skynet_balances(wallet_address, contract_address, limit=limit)


def fetch_blockscout_nft_details(nft_ids: list):
    items = []

    for item in nft_ids or []:
        token_id = str(item.get("tokenId") or "").strip()
        contract_address = normalize_contract_address(item.get("contractAddress"))
        if not token_id or not contract_address:
            continue

        url = f"{BLOCKSCOUT_BASE_URL}/tokens/{contract_address}/instances/{token_id}"
        response = requests.get(url, headers=HEADERS, timeout=(5, 10))
        if response.status_code == 404:
            continue
        response.raise_for_status()

        items.append(
            normalize_blockscout_nft_item(
                response.json(),
                fallback_contract_address=contract_address,
            )
        )

    return items


def fetch_blockscout_owned_nfts(wallet_address: str, contract_address: str):
    wallet_address = normalize_contract_address(wallet_address)
    contract_address = normalize_contract_address(contract_address)
    if not wallet_address or not contract_address:
        return []

    url = f"{BLOCKSCOUT_BASE_URL}/addresses/{wallet_address}/nft"
    all_items = []

    while url:
        payload = blockscout_get_json(url)
        page_items = payload.get("items", []) if isinstance(payload, dict) else []

        for item in page_items or []:
            token = item.get("token") or {}
            row_contract = normalize_contract_address(token.get("address_hash"))
            if row_contract == contract_address:
                normalized = normalize_blockscout_nft_item(
                    item,
                    fallback_contract_address=contract_address,
                )
                if normalized.get("tokenId"):
                    all_items.append(normalized)

        next_page_params = payload.get("next_page_params") if isinstance(payload, dict) else None
        if not next_page_params:
            break

        url = f"{BLOCKSCOUT_BASE_URL}/addresses/{wallet_address}/nft?{urlencode(next_page_params)}"

    return all_items


def fetch_skynet_nft_details(nft_ids: list, batch_size: int = 20):
    if not nft_ids:
        return []

    clean_ids = []
    for item in nft_ids:
        token_id = str(item.get("tokenId") or "").strip()
        contract_address = str(item.get("contractAddress") or "").strip()
        balance = str(item.get("balance") or "1").strip()

        if not token_id or not contract_address:
            continue

        clean_ids.append(
            {
                "tokenId": token_id,
                "contractAddress": contract_address,
                "balance": balance,
            }
        )

    if not clean_ids:
        return []

    url = f"{SKYNET_BASE_URL}/collections/nfts"
    all_items = []

    for i in range(0, len(clean_ids), batch_size):
        batch = clean_ids[i:i + batch_size]
        payload = {"nftIds": batch}

        data = skynet_post_json(url, payload=payload, timeout=(5, 10))

        items = data.get("result", {}).get("items", [])
        all_items.extend(items)

    return all_items


def fetch_nft_details(nft_ids: list, batch_size: int = 20):
    if not nft_ids:
        return []

    blockscout_items = []
    try:
        blockscout_items = fetch_blockscout_nft_details(nft_ids)
    except Exception:
        return fetch_skynet_nft_details(nft_ids, batch_size=batch_size)

    found_keys = {
        (
            normalize_contract_address(item.get("contractAddress")),
            str(item.get("tokenId") or "").strip(),
        )
        for item in blockscout_items
    }
    missing_ids = [
        item for item in nft_ids
        if (
            normalize_contract_address(item.get("contractAddress")),
            str(item.get("tokenId") or "").strip(),
        ) not in found_keys
    ]

    if not missing_ids:
        return blockscout_items

    return blockscout_items + fetch_skynet_nft_details(missing_ids, batch_size=batch_size)


def fetch_all_owned_chickens(wallet_address: str, contract_addresses: list):
    blockscout_items = []
    try:
        for contract in contract_addresses or []:
            blockscout_items.extend(fetch_blockscout_owned_nfts(wallet_address, contract))
    except Exception:
        blockscout_items = []

    if blockscout_items:
        missing_metadata = [
            {
                "tokenId": item.get("tokenId"),
                "contractAddress": item.get("contractAddress"),
                "balance": item.get("balance") or "1",
            }
            for item in blockscout_items
            if not isinstance(item.get("metadata"), dict) or not item.get("metadata")
        ]
        if not missing_metadata:
            return blockscout_items

        enriched_items = fetch_nft_details(missing_metadata)
        enriched_lookup = {
            (
                normalize_contract_address(item.get("contractAddress")),
                str(item.get("tokenId") or "").strip(),
            ): item
            for item in enriched_items
        }
        return [
            enriched_lookup.get(
                (
                    normalize_contract_address(item.get("contractAddress")),
                    str(item.get("tokenId") or "").strip(),
                ),
                item,
            )
            for item in blockscout_items
        ]

    nft_ids = []

    for contract in contract_addresses:
        balance_items = fetch_balances(wallet_address, contract)

        for item in balance_items:
            token_id = str(item.get("tokenId") or "").strip()
            balance = str(item.get("balance") or "1").strip()

            if not token_id:
                continue

            nft_ids.append(
                {
                    "tokenId": token_id,
                    "contractAddress": contract,
                    "balance": balance,
                }
            )

    return fetch_nft_details(nft_ids)


def fetch_chicken_by_token(token_id: str, contract_addresses: list):
    token_id = str(token_id).strip()
    if not token_id:
        return None

    candidates = [
        {
            "tokenId": token_id,
            "contractAddress": contract,
            "balance": "1",
        }
        for contract in contract_addresses
        if contract
    ]

    try:
        items = fetch_nft_details(candidates)
    except Exception:
        return None

    for item in items:
        if str(item.get("tokenId") or "").strip() == token_id:
            return item

    return None
