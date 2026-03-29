import requests

BASE_URL = "https://skynet-api.roninchain.com/ronin/explorer/v2"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def fetch_balances(wallet_address: str, contract_address: str, limit: int = 25):
    offset = 0
    all_items = []

    while True:
        url = (
            f"{BASE_URL}/accounts/{wallet_address}/balances/{contract_address}"
            f"?limit={limit}&offset={offset}"
        )
        print("URL:", url)

        response = requests.get(url, headers=HEADERS, timeout=(5, 5))
        response.raise_for_status()
        data = response.json()

        items = data.get("result", {}).get("items", [])
        if not items:
            break

        all_items.extend(items)

        if len(items) < limit:
            break

        offset += limit

    return all_items

def fetch_nft_details(nft_ids: list, batch_size: int = 50):
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

    url = f"{BASE_URL}/collections/nfts"
    all_items = []

    for i in range(0, len(clean_ids), batch_size):
        batch = clean_ids[i:i + batch_size]
        payload = {"nftIds": batch}

        print("NFT DETAILS URL:", url)
        print("NFT DETAILS BATCH:", i // batch_size + 1, "SIZE:", len(batch))

        response = requests.post(url, headers=HEADERS, json=payload, timeout=(5, 5))
        response.raise_for_status()
        data = response.json()

        items = data.get("result", {}).get("items", [])
        all_items.extend(items)

    return all_items


def fetch_all_owned_chickens(wallet_address: str, contract_addresses: list):
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
