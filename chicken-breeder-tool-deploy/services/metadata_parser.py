from datetime import datetime, timezone
from services.ip_calculator import calculate_ip


def attributes_to_dict(attributes_list):
    result = {}
    for item in attributes_list or []:
        trait_type = item.get("trait_type")
        value = item.get("value")
        if trait_type:
            result[trait_type] = value
    return result


def parse_generation_number(generation_text):
    if not generation_text:
        return None

    generation_text = str(generation_text).strip()
    if generation_text.lower().startswith("gen "):
        num = generation_text[4:].strip()
        if num.isdigit():
            return int(num)

    return None


def get_remaining_seconds(target_unix):
    if not target_unix:
        return 0

    now_unix = int(datetime.now(timezone.utc).timestamp())
    remaining = int(target_unix) - now_unix
    return max(0, remaining)


def format_time_remaining(target_unix):
    remaining = get_remaining_seconds(target_unix)

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


def derive_state(raw_state, breeding_time):
    raw_state_text = str(raw_state or "").strip().lower()

    if raw_state_text == "dead":
        return "Dead"

    if get_remaining_seconds(breeding_time) > 0:
        return "Breeding"

    return "Normal"


def parse_chicken_record(wallet_address: str, item: dict):
    metadata = item.get("metadata", {}) or {}
    attributes = attributes_to_dict(metadata.get("attributes", []))

    raw_state = attributes.get("State")
    chicken_type = attributes.get("Type")
    generation_text = attributes.get("Generation")
    breeding_time = attributes.get("Breeding Time")

    derived_state = derive_state(raw_state, breeding_time)

    is_dead = derived_state == "Dead"
    is_egg = str(chicken_type or "").strip().lower() == "egg"

    record = {
        "wallet_address": wallet_address,
        "contract_address": item.get("contractAddress"),
        "token_id": item.get("tokenId"),
        "name": metadata.get("name"),
        "nickname": metadata.get("nickname"),
        "image": metadata.get("image"),
        "token_uri": item.get("tokenURI"),

        "raw_state": raw_state,
        "state": derived_state,
        "is_dead": is_dead,
        "is_egg": is_egg,
        "breeding_time": breeding_time,
        "breeding_time_remaining": format_time_remaining(breeding_time),
        "breed_count": attributes.get("Breed Count"),
        "type": chicken_type,
        "gender": attributes.get("Gender"),
        "level": attributes.get("Level"),
        "generation_text": generation_text,
        "generation_num": parse_generation_number(generation_text),

        "parent_1": attributes.get("Parent 1"),
        "parent_2": attributes.get("Parent 2"),

        "instinct": attributes.get("Instinct"),

        "beak": attributes.get("Beak"),
        "comb": attributes.get("Comb"),
        "eyes": attributes.get("Eyes"),
        "feet": attributes.get("Feet"),
        "wings": attributes.get("Wings"),
        "tail": attributes.get("Tail"),
        "body": attributes.get("Body"),

        "innate_attack": attributes.get("Innate Attack"),
        "innate_defense": attributes.get("Innate Defense"),
        "innate_speed": attributes.get("Innate Speed"),
        "innate_health": attributes.get("Innate Health"),
        "innate_ferocity": attributes.get("Innate Ferocity"),
        "innate_cockrage": attributes.get("Innate Cockrage"),
        "innate_evasion": attributes.get("Innate Evasion"),
    }

    record["ip"] = calculate_ip(attributes, is_egg=is_egg, is_dead=is_dead)

    return record
