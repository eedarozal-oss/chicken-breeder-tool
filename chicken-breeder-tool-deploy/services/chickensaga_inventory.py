import requests
from datetime import datetime, timezone

from services.ip_calculator import calculate_ip
from services.metadata_parser import parse_generation_number


CHICKENSAGA_INVENTORY_URL = "https://app.chickensaga.com/api/proxy/ronin-gql"


def _first_attribute(attributes, name):
    value = (attributes or {}).get(name)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _string_value(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _bool_text(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_seconds_remaining(value):
    remaining = _safe_int(value)
    if remaining is None:
        return None

    if remaining <= 0:
        return None

    days = remaining // 86400
    hours = (remaining % 86400) // 3600
    minutes = (remaining % 3600) // 60

    if days > 0:
        return f"{days}d {hours}h {minutes:02d}m"
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _seconds_until_timestamp(value):
    target_unix = _safe_int(value)
    if not target_unix:
        return 0
    return max(0, target_unix - int(datetime.now(timezone.utc).timestamp()))


def derive_chickensaga_state(raw_state, breeding_time):
    raw_state_text = str(raw_state or "").strip()
    normalized = raw_state_text.lower()

    if normalized == "dead":
        return "Dead"

    if normalized == "breeding":
        return "Breeding" if _seconds_until_timestamp(breeding_time) > 0 else "Normal"

    return raw_state_text or "Normal"


def fetch_chickensaga_inventory(wallet_address, timeout=(2, 8)):
    wallet_address = str(wallet_address or "").strip().lower()
    if not wallet_address:
        return []

    response = requests.get(
        CHICKENSAGA_INVENTORY_URL,
        params={"address": wallet_address},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()

    tokens = data.get("tokens") if isinstance(data, dict) else None
    if not isinstance(tokens, list):
        raise ValueError("ChickenSaga inventory response did not include a token list.")

    return tokens


def infer_chickensaga_contract_address(chicken_type, contract_addresses=None):
    contract_addresses = list(contract_addresses or [])
    if not contract_addresses:
        return None

    chicken_type = str(chicken_type or "").strip().lower()
    if chicken_type == "genesis":
        return contract_addresses[0]

    if len(contract_addresses) > 1:
        return contract_addresses[1]

    return contract_addresses[0]


def chickensaga_token_to_record(wallet_address, token, contract_addresses=None):
    attributes = token.get("attributes") or {}
    chicken_type = _string_value(_first_attribute(attributes, "Type"))
    raw_state = _string_value(_first_attribute(attributes, "State"))
    breeding_time = _first_attribute(attributes, "Breeding Time")
    breeding = _bool_text(_first_attribute(attributes, "Breeding"))

    if not raw_state:
        raw_state = "Breeding" if breeding else "Normal"

    state = derive_chickensaga_state(raw_state, breeding_time)

    is_egg = str(chicken_type or "").strip().lower() == "egg"
    is_dead = state.strip().lower() == "dead"

    ip_attributes = {
        "Innate Attack": _first_attribute(attributes, "Innate Attack"),
        "Innate Defense": _first_attribute(attributes, "Innate Defense"),
        "Innate Speed": _first_attribute(attributes, "Innate Speed"),
        "Innate Health": _first_attribute(attributes, "Innate Health"),
        "Innate Ferocity": _first_attribute(attributes, "Innate Ferocity"),
        "Innate Cockrage": _first_attribute(attributes, "Innate Cockrage"),
        "Innate Evasion": _first_attribute(attributes, "Innate Evasion"),
    }

    breeding_time_remaining = _seconds_until_timestamp(breeding_time)
    generation_text = _string_value(_first_attribute(attributes, "Generation"))

    return {
        "wallet_address": wallet_address,
        "contract_address": infer_chickensaga_contract_address(chicken_type, contract_addresses),
        "token_id": _string_value(token.get("tokenId")),
        "name": f"Chicken #{token.get('tokenId')}" if token.get("tokenId") else None,
        "nickname": None,
        "image": _string_value(token.get("image")),
        "token_uri": None,
        "raw_state": raw_state,
        "state": state,
        "is_dead": is_dead,
        "is_egg": is_egg,
        "breeding_time": breeding_time,
        "breeding_time_remaining": _format_seconds_remaining(breeding_time_remaining),
        "breed_count": _first_attribute(attributes, "Breed Count"),
        "type": chicken_type,
        "gender": _string_value(_first_attribute(attributes, "Gender")),
        "level": _first_attribute(attributes, "Level"),
        "generation_text": generation_text,
        "generation_num": parse_generation_number(generation_text),
        "parent_1": _string_value(_first_attribute(attributes, "Parent 1")),
        "parent_2": _string_value(_first_attribute(attributes, "Parent 2")),
        "instinct": _string_value(_first_attribute(attributes, "Instinct")),
        "beak": _string_value(_first_attribute(attributes, "Beak")),
        "comb": _string_value(_first_attribute(attributes, "Comb")),
        "eyes": _string_value(_first_attribute(attributes, "Eyes")),
        "feet": _string_value(_first_attribute(attributes, "Feet")),
        "wings": _string_value(_first_attribute(attributes, "Wings")),
        "tail": _string_value(_first_attribute(attributes, "Tail")),
        "body": _string_value(_first_attribute(attributes, "Body")),
        "innate_attack": _first_attribute(attributes, "Innate Attack"),
        "innate_defense": _first_attribute(attributes, "Innate Defense"),
        "innate_speed": _first_attribute(attributes, "Innate Speed"),
        "innate_health": _first_attribute(attributes, "Innate Health"),
        "innate_ferocity": _first_attribute(attributes, "Innate Ferocity"),
        "innate_cockrage": _first_attribute(attributes, "Innate Cockrage"),
        "innate_evasion": _first_attribute(attributes, "Innate Evasion"),
        "ip": calculate_ip(ip_attributes, is_egg=is_egg, is_dead=is_dead),
    }


def build_chickensaga_wallet_records(wallet_address, tokens, contract_addresses=None):
    records = []
    for token in tokens or []:
        if not isinstance(token, dict):
            continue
        record = chickensaga_token_to_record(wallet_address, token, contract_addresses)
        if record.get("token_id"):
            records.append(record)
    return records


def _normalized_token_map(records):
    result = {}
    for record in records or []:
        token_id = str(record.get("token_id") or "").strip()
        if token_id:
            result[token_id] = record
    return result


def _normalized_field(value):
    return str(value or "").strip().lower()


def should_use_chickensaga_wallet_records(ronin_records, chickensaga_records):
    ronin_map = _normalized_token_map(ronin_records)
    chickensaga_map = _normalized_token_map(chickensaga_records)

    if not chickensaga_map:
        return False, "empty_chickensaga_inventory"

    if set(ronin_map) != set(chickensaga_map):
        return True, "token_set_mismatch"

    for token_id, chickensaga_record in chickensaga_map.items():
        ronin_record = ronin_map.get(token_id) or {}
        for field in ("type", "state", "breed_count", "breeding_time"):
            if _normalized_field(ronin_record.get(field)) != _normalized_field(chickensaga_record.get(field)):
                return True, f"{field}_mismatch"

    return False, "ronin_inventory_current"
