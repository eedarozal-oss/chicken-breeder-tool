from __future__ import annotations

from typing import Any, Dict, Optional

from services.ronin_api import fetch_balances


CHICKEN_SAGA_RESOURCE_CONTRACT = "0xac78515ef7d0bb935bc86d4f8ce314038946b52f".lower()

BREEDING_ITEM_NAME_TO_TOKEN_ID: Dict[str, str] = {
    "Soulknot": "63",
    "Gregor's Gift": "65",
    "Mendel's Memento": "66",
    "Quentin's Talon": "67",
    "Dragon's Whip": "68",
    "Chibidei's Curse": "69",
    "All-seeing Seed": "70",
    "Chim Lac's Curio": "71",
    "Suave Scissors": "72",
    "Simurgh's Sovereign": "73",
    "St. Elmo's Fire": "74",
    "Cocktail's Obsidian Beak": "76",
    "Pos2 Pellet": "77",
    "Fetzzz Feet": "78",
    "Vananderen's Vitality": "79",
    "Pinong's Bird": "80",
    "Ouchie's Ornament": "81",
    "Lockedin State": "82",
}

ITEM_NAME_ALIASES: Dict[str, str] = {
    "Cocktail's Beak": "Cocktail's Obsidian Beak",
}

TOKEN_ID_TO_BREEDING_ITEM_NAME: Dict[str, str] = {
    token_id: name for name, token_id in BREEDING_ITEM_NAME_TO_TOKEN_ID.items()
}

BREEDING_ITEM_TOKEN_IDS = set(TOKEN_ID_TO_BREEDING_ITEM_NAME.keys())


def normalize_item_name(item_name: Optional[str]) -> str:
    raw = str(item_name or "").strip()
    if not raw:
        return ""
    return ITEM_NAME_ALIASES.get(raw, raw)


def get_breeding_item_token_id(item_name: Optional[str]) -> Optional[str]:
    canonical_name = normalize_item_name(item_name)
    return BREEDING_ITEM_NAME_TO_TOKEN_ID.get(canonical_name)


def get_breeding_item_name_by_token_id(token_id: Optional[str]) -> str:
    return TOKEN_ID_TO_BREEDING_ITEM_NAME.get(str(token_id or "").strip(), "")


def build_resource_image_url(token_id: Optional[str]) -> str:
    token_id = str(token_id or "").strip()
    if not token_id:
        return ""
    return f"https://sabong-saga-resources.s3.ap-southeast-1.amazonaws.com/{token_id}.png"


def get_breeding_item_image_url(item_name: Optional[str]) -> str:
    token_id = get_breeding_item_token_id(item_name)
    if not token_id:
        return ""
    return build_resource_image_url(token_id)


def fetch_wallet_item_inventory(
    wallet_address: str,
    token_address: str = CHICKEN_SAGA_RESOURCE_CONTRACT,
) -> list[Dict[str, Any]]:
    wallet_address = str(wallet_address or "").strip().lower()
    token_address = str(token_address or "").strip().lower()

    if not wallet_address:
        return []

    balance_rows = fetch_balances(wallet_address, token_address)

    filtered_rows = []
    for row in balance_rows or []:
        token_id = str(row.get("tokenId") or "").strip()
        if not token_id:
            continue
        if token_id not in BREEDING_ITEM_TOKEN_IDS:
            continue
        filtered_rows.append(row)

    return filtered_rows


def build_wallet_inventory_lookup(
    wallet_address: str,
    token_address: str = CHICKEN_SAGA_RESOURCE_CONTRACT,
) -> Dict[str, Dict[str, Any]]:
    rows = fetch_wallet_item_inventory(wallet_address, token_address=token_address)

    lookup: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        token_id = str(row.get("tokenId") or "").strip()
        if not token_id:
            continue

        balance_raw = row.get("balance")
        try:
            balance = int(str(balance_raw or "0"))
        except ValueError:
            balance = 0

        canonical_name = get_breeding_item_name_by_token_id(token_id)
        image = build_resource_image_url(token_id)

        lookup[token_id] = {
            "token_id": token_id,
            "name": canonical_name,
            "balance": balance,
            "image": image,
            "token_address": token_address,
            "is_locked": False,
        }

    return lookup


def build_wallet_inventory_name_lookup(
    wallet_address: str,
    token_address: str = CHICKEN_SAGA_RESOURCE_CONTRACT,
) -> Dict[str, Dict[str, Any]]:
    token_lookup = build_wallet_inventory_lookup(wallet_address, token_address=token_address)
    by_name: Dict[str, Dict[str, Any]] = {}

    for item in token_lookup.values():
        canonical_name = normalize_item_name(item.get("name"))
        if not canonical_name:
            continue
        by_name[canonical_name] = item

    return by_name
