from services.database import get_chicken_by_token, upsert_chicken
from services.metadata_parser import parse_chicken_record
from services.ronin_api import fetch_chicken_by_token


ROOT_MAX_ID = 11110


def safe_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_family_root(token_id):
    token_num = safe_int(token_id)
    return token_num is not None and token_num <= ROOT_MAX_ID


def should_check_root_alive_state(token_id):
    token_num = safe_int(token_id)
    return token_num is not None and 2222 < token_num < ROOT_MAX_ID


def is_breedable_chicken(chicken):
    if not chicken:
        return False

    if chicken.get("is_egg"):
        return False

    if str(chicken.get("state") or "").strip().lower() != "normal":
        return False

    return True


def is_dead_chicken(chicken):
    if not chicken:
        return False

    if chicken.get("is_dead"):
        return True

    return str(chicken.get("state") or "").strip().lower() == "dead"


def build_chicken_lookup(chickens):
    lookup = {}
    for chicken in chickens or []:
        token_id = str(chicken.get("token_id") or "").strip()
        if token_id:
            lookup[token_id] = chicken
    return lookup


def build_owned_token_set(chickens):
    owned = set()
    for chicken in chickens or []:
        token_id = str(chicken.get("token_id") or "").strip()
        if token_id:
            owned.add(token_id)
    return owned


def get_or_fetch_chicken_record(token_id, contract_addresses=None):
    token_id = str(token_id or "").strip()
    if not token_id:
        return None

    chicken = get_chicken_by_token(token_id)
    if chicken:
        return chicken

    if not contract_addresses:
        return None

    raw_item = fetch_chicken_by_token(token_id, contract_addresses)
    if not raw_item:
        return None

    parsed = parse_chicken_record(wallet_address=None, item=raw_item)
    upsert_chicken(parsed)

    chicken = get_chicken_by_token(token_id)
    return chicken or parsed


def filter_alive_roots(roots, contract_addresses=None):
    unique_roots = sorted({str(root) for root in (roots or set())}, key=lambda x: int(x))

    alive_roots = []
    dead_roots = []
    had_lookup_failure = False

    for root_id in unique_roots:
        if not should_check_root_alive_state(root_id):
            alive_roots.append(root_id)
            continue

        try:
            chicken = get_or_fetch_chicken_record(root_id, contract_addresses=contract_addresses)
        except Exception:
            had_lookup_failure = True
            chicken = None

        if chicken is None:
            had_lookup_failure = True
            alive_roots.append(root_id)
            continue

        if is_dead_chicken(chicken):
            dead_roots.append(root_id)
        else:
            alive_roots.append(root_id)

    return {
        "alive_roots": alive_roots,
        "dead_roots": dead_roots,
        "had_lookup_failure": had_lookup_failure,
    }


def resolve_family_roots_for_token(token_id, chicken_lookup, cache=None, visited=None):
    """
    Returns:
    {
        "roots": set[str],
        "is_complete": bool
    }
    """
    token_id = str(token_id or "").strip()

    if not token_id:
        return {
            "roots": set(),
            "is_complete": False,
        }

    if cache is None:
        cache = {}

    if visited is None:
        visited = set()

    if token_id in cache:
        return {
            "roots": set(cache[token_id]["roots"]),
            "is_complete": cache[token_id]["is_complete"],
        }

    if token_id in visited:
        return {
            "roots": set(),
            "is_complete": False,
        }

    if is_family_root(token_id):
        result = {
            "roots": {token_id},
            "is_complete": True,
        }
        cache[token_id] = {
            "roots": set(result["roots"]),
            "is_complete": result["is_complete"],
        }
        return result

    chicken = chicken_lookup.get(token_id)
    if not chicken:
        return {
            "roots": set(),
            "is_complete": False,
        }

    current_visited = set(visited)
    current_visited.add(token_id)

    parent_1 = str(chicken.get("parent_1") or "").strip()
    parent_2 = str(chicken.get("parent_2") or "").strip()

    combined_roots = set()
    is_complete = True
    parent_found = False

    for parent_id in [parent_1, parent_2]:
        if not parent_id:
            continue

        parent_found = True

        parent_result = resolve_family_roots_for_token(
            parent_id,
            chicken_lookup=chicken_lookup,
            cache=cache,
            visited=current_visited,
        )

        combined_roots.update(parent_result["roots"])

        if not parent_result["is_complete"]:
            is_complete = False

    if not parent_found:
        is_complete = False

    result = {
        "roots": combined_roots,
        "is_complete": is_complete,
    }

    cache[token_id] = {
        "roots": set(result["roots"]),
        "is_complete": result["is_complete"],
    }

    return result


def build_family_root_summary(token_id, roots, owned_token_ids, is_complete, contract_addresses=None):
    filtered = filter_alive_roots(roots, contract_addresses=contract_addresses)

    alive_roots = filtered["alive_roots"]
    owned_roots = [root for root in alive_roots if root in owned_token_ids]

    total_root_count = len(alive_roots)
    owned_root_count = len(owned_roots)

    ownership_percent = 0.0
    if total_root_count > 0:
        ownership_percent = round((owned_root_count / total_root_count) * 100, 2)

    final_complete = bool(is_complete) and not filtered.get("had_lookup_failure")

    return {
        "token_id": str(token_id),
        "owned_root_count": owned_root_count,
        "total_root_count": total_root_count,
        "ownership_percent": ownership_percent,
        "is_complete": 1 if final_complete else 0,
        "roots": alive_roots,
        "dead_roots": filtered["dead_roots"],
    }


def resolve_family_roots_for_chicken(chicken, chicken_lookup, owned_token_ids, cache=None, contract_addresses=None):
    token_id = str(chicken.get("token_id") or "").strip()

    if not is_breedable_chicken(chicken):
        return {
            "token_id": token_id,
            "owned_root_count": 0,
            "total_root_count": 0,
            "ownership_percent": 0.0,
            "is_complete": 0,
            "roots": [],
            "dead_roots": [],
        }

    result = resolve_family_roots_for_token(
        token_id=token_id,
        chicken_lookup=chicken_lookup,
        cache=cache or {},
        visited=set(),
    )

    return build_family_root_summary(
        token_id=token_id,
        roots=result["roots"],
        owned_token_ids=owned_token_ids,
        is_complete=result["is_complete"],
        contract_addresses=contract_addresses,
    )


def resolve_family_roots_for_all(chickens, contract_addresses=None):
    """
    chickens: list of chicken dict rows from DB

    Returns:
    [
        {
            "token_id": "16893",
            "owned_root_count": 2,
            "total_root_count": 2,
            "ownership_percent": 100.0,
            "is_complete": 1,
            "roots": ["1916", "9301"]
        },
        ...
    ]
    """
    chicken_lookup = build_chicken_lookup(chickens)
    owned_token_ids = build_owned_token_set(chickens)
    cache = {}

    results = []

    sorted_chickens = sorted(
        chickens,
        key=lambda row: safe_int(row.get("token_id")) or 0
    )

    for chicken in sorted_chickens:
        summary = resolve_family_roots_for_chicken(
            chicken=chicken,
            chicken_lookup=chicken_lookup,
            owned_token_ids=owned_token_ids,
            cache=cache,
            contract_addresses=contract_addresses,
        )
        results.append(summary)

    return results
