import requests
from datetime import datetime, timedelta, timezone
from services.database import (
    get_chicken_by_token,
    upsert_chicken,
    get_family_root_items,
    upsert_family_root_item,
    upsert_family_root_summary,
    insert_family_root_items,
)
from services.metadata_parser import parse_chicken_record
from services.ronin_api import fetch_chicken_by_token, fetch_nft_details

ROOT_MAX_ID = 11110

def fetch_chicken_from_game_api(token_id):
    token_id = str(token_id or "").strip()
    if not token_id:
        return None

    url = f"https://chicken-api-ivory.vercel.app/api/{token_id}"

    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()
        data = response.json()
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    return data

def normalize_game_api_chicken(data, token_id):
    if not isinstance(data, dict):
        return None

    state = ""
    is_dead = None

    for attr in data.get("attributes", []) or []:
        trait_type = str(attr.get("trait_type") or "").strip().lower()
        value = attr.get("value")

        if trait_type == "state":
            state = str(value or "").strip().lower()
        elif trait_type == "breeding":
            # optional, not needed for dead check
            pass

    if is_dead is None:
        is_dead = state == "dead"

    return {
        "token_id": str(token_id),
        "state": state,
        "is_dead": bool(is_dead),
    }



def is_root_check_stale(last_checked_at, max_age_hours=24):
    if not last_checked_at:
        return True

    try:
        checked_at = datetime.fromisoformat(last_checked_at)
    except Exception:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    return checked_at <= cutoff

def fetch_root_records_in_batch(root_ids, contract_addresses=None):
    root_ids = [str(root_id).strip() for root_id in (root_ids or []) if str(root_id).strip()]
    if not root_ids or not contract_addresses:
        return {}

    result_lookup = {}
    missing_root_ids = []

    for root_id in root_ids:
        chicken = get_chicken_by_token(root_id)
        if chicken:
            result_lookup[root_id] = chicken
        else:
            missing_root_ids.append(root_id)

    if not missing_root_ids:
        return result_lookup

    candidates = []
    for root_id in missing_root_ids:
        for contract in contract_addresses or []:
            if not contract:
                continue
            candidates.append({
                "tokenId": root_id,
                "contractAddress": contract,
                "balance": "1",
            })

    if not candidates:
        return result_lookup

    try:
        items = fetch_nft_details(candidates)
    except Exception:
        return result_lookup

    raw_lookup = {}
    for item in items or []:
        token_id = str(item.get("tokenId") or "").strip()
        if token_id and token_id not in raw_lookup:
            raw_lookup[token_id] = item

    for root_id in missing_root_ids:
        raw_item = raw_lookup.get(root_id)
        if not raw_item:
            continue

        parsed = parse_chicken_record(wallet_address=None, item=raw_item)
        upsert_chicken(parsed)

        chicken = get_chicken_by_token(root_id)
        result_lookup[root_id] = chicken or parsed

    return result_lookup

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

def build_initial_root_status_map(roots, owned_token_ids):
    owned_token_ids = {str(token_id) for token_id in (owned_token_ids or set())}
    now_utc = datetime.now(timezone.utc).isoformat()
    result = {}

    for root_id in roots or []:
        root_id = str(root_id).strip()
        if not root_id:
            continue

        if root_id in owned_token_ids:
            result[root_id] = {
                "root_check_status": "skipped",
                "is_dead_root": False,
                "last_checked_at": now_utc,
            }
        elif not should_check_root_alive_state(root_id):
            result[root_id] = {
                "root_check_status": "skipped",
                "is_dead_root": False,
                "last_checked_at": now_utc,
            }
        else:
            result[root_id] = {
                "root_check_status": "pending",
                "is_dead_root": False,
                "last_checked_at": None,
            }

    return result

def refresh_pending_and_stale_root_items(wallet_address, token_id, root_items, contract_addresses=None, max_age_hours=24):
    now_utc = datetime.now(timezone.utc).isoformat()
    roots_to_check = []

    for item in root_items or []:
        root_id = str(item.get("root_token_id") or "").strip()
        status = str(item.get("root_check_status") or "").strip().lower()
        is_owned_root = int(item.get("is_owned_root") or 0)

        if not root_id:
            continue

        if is_owned_root:
            continue

        if status == "dead_checked":
            continue

        if status == "pending":
            roots_to_check.append(root_id)
            continue

        if status == "alive_checked" and is_root_check_stale(item.get("last_checked_at"), max_age_hours=max_age_hours):
            roots_to_check.append(root_id)

    if not roots_to_check:
        return

    batch_lookup = fetch_root_records_in_batch(
        roots_to_check,
        contract_addresses=contract_addresses,
    )

    for root_id in roots_to_check:
        chicken = batch_lookup.get(root_id)

        if chicken is None:
            game_api_data = fetch_chicken_from_game_api(root_id)
            chicken = normalize_game_api_chicken(game_api_data, root_id)

        if chicken is None:
            upsert_family_root_item(
                wallet_address=wallet_address,
                token_id=token_id,
                root_token_id=root_id,
                root_check_status="pending",
                is_dead_root=0,
                last_checked_at=None,
            )
            continue

        if is_dead_chicken(chicken):
            upsert_family_root_item(
                wallet_address=wallet_address,
                token_id=token_id,
                root_token_id=root_id,
                root_check_status="dead_checked",
                is_dead_root=1,
                last_checked_at=now_utc,
            )
        else:
            upsert_family_root_item(
                wallet_address=wallet_address,
                token_id=token_id,
                root_check_status="alive_checked",
                root_token_id=root_id,
                is_dead_root=0,
                last_checked_at=now_utc,
            )

def should_check_root_alive_state(token_id):
    token_num = safe_int(token_id)
    return token_num is not None and 2222 < token_num < ROOT_MAX_ID

def build_family_root_summary_from_items(token_id, root_items, owned_token_ids):
    alive_roots = []
    dead_roots = []
    pending_root_check_count = 0
    root_check_target_count = 0

    owned_token_ids = {str(token_id) for token_id in (owned_token_ids or set())}

    for item in root_items or []:
        root_id = str(item.get("root_token_id") or "").strip()
        status = str(item.get("root_check_status") or "").strip().lower()
        is_dead_root = int(item.get("is_dead_root") or 0)

        if not root_id:
            continue

        if status in {"alive_checked", "pending"}:
            root_check_target_count += 1

        if status == "dead_checked" or is_dead_root:
            dead_roots.append(root_id)
            continue

        if status == "pending":
            pending_root_check_count += 1

        alive_roots.append(root_id)

    owned_roots = [root for root in alive_roots if root in owned_token_ids]

    total_root_count = len(alive_roots)
    owned_root_count = len(owned_roots)

    ownership_percent = 0.0
    if total_root_count > 0:
        ownership_percent = round((owned_root_count / total_root_count) * 100, 2)

    return {
        "token_id": str(token_id),
        "owned_root_count": owned_root_count,
        "total_root_count": total_root_count,
        "ownership_percent": ownership_percent,
        "is_complete": 1 if pending_root_check_count == 0 else 0,
        "roots": alive_roots,
        "dead_roots": dead_roots,
        "root_check_target_count": root_check_target_count,
        "pending_root_check_count": pending_root_check_count,
    }

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


def filter_alive_roots(roots, owned_token_ids=None, contract_addresses=None):
    unique_roots = sorted({str(root) for root in (roots or set())}, key=lambda x: int(x))
    owned_token_ids = {str(token_id) for token_id in (owned_token_ids or set())}

    alive_roots = []
    dead_roots = []
    had_lookup_failure = False
    roots_to_check = []
    pending_root_check_count = 0

    for root_id in unique_roots:
        if root_id in owned_token_ids:
            alive_roots.append(root_id)
            continue

        if not should_check_root_alive_state(root_id):
            alive_roots.append(root_id)
            continue

        roots_to_check.append(root_id)

    batch_lookup = fetch_root_records_in_batch(
        roots_to_check,
        contract_addresses=contract_addresses,
    )

    for root_id in roots_to_check:
        chicken = batch_lookup.get(root_id)

        if chicken is None:
            had_lookup_failure = True
            pending_root_check_count += 1
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
        "root_check_target_count": len(roots_to_check),
        "pending_root_check_count": pending_root_check_count,
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
    filtered = filter_alive_roots(
        roots,
        owned_token_ids=owned_token_ids,
        contract_addresses=contract_addresses,
    )

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
        "root_check_target_count": filtered.get("root_check_target_count", 0),
        "pending_root_check_count": filtered.get("pending_root_check_count", 0),
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

def complete_ninuno_via_lineage_with_resume(wallet_address, token_id: str, owned_token_ids, depth: int = 3, max_tokens: int = 300, contract_addresses=None):
    token_id = str(token_id).strip()

    existing_root_items = get_family_root_items(wallet_address, token_id)

    if existing_root_items:
        refresh_pending_and_stale_root_items(
            wallet_address=wallet_address,
            token_id=token_id,
            root_items=existing_root_items,
            contract_addresses=contract_addresses,
            max_age_hours=24,
        )
        refreshed_items = get_family_root_items(wallet_address, token_id)
        return build_family_root_summary_from_items(
            token_id=token_id,
            root_items=refreshed_items,
            owned_token_ids=owned_token_ids,
        )

    from services.lineage_api import complete_ninuno_via_lineage

    summary = complete_ninuno_via_lineage(
        token_id=token_id,
        owned_token_ids=owned_token_ids,
        depth=depth,
        max_tokens=max_tokens,
        contract_addresses=contract_addresses,
    )

    root_status_map = build_initial_root_status_map(summary.get("roots", []), owned_token_ids)

    from services.database import insert_family_root_items
    insert_family_root_items(
        wallet_address=wallet_address,
        token_id=token_id,
        roots=summary.get("roots", []),
        owned_root_ids=owned_token_ids,
        root_status_map=root_status_map,
    )

    stored_items = get_family_root_items(wallet_address, token_id)

    refresh_pending_and_stale_root_items(
        wallet_address=wallet_address,
        token_id=token_id,
        root_items=stored_items,
        contract_addresses=contract_addresses,
        max_age_hours=24,
    )

    final_items = get_family_root_items(wallet_address, token_id)
    return build_family_root_summary_from_items(
        token_id=token_id,
        root_items=final_items,
        owned_token_ids=owned_token_ids,
    )

def initialize_simple_family_roots_for_wallet(chickens, wallet_address, contract_addresses=None):
    chickens = chickens or []
    owned_token_ids = build_owned_token_set(chickens)
    now_utc = datetime.now(timezone.utc).isoformat()

    simple_candidates = []
    roots_to_check = set()

    for chicken in chickens:
        if not is_breedable_chicken(chicken):
            continue

        token_id = str(chicken.get("token_id") or "").strip()
        if not token_id:
            continue

        roots = []

        # Case 1: chicken itself is already a root
        if is_family_root(token_id):
            roots = [token_id]

        # Case 2: both parents are roots
        else:
            parent_1 = str(chicken.get("parent_1") or "").strip()
            parent_2 = str(chicken.get("parent_2") or "").strip()

            if not parent_1 or not parent_2:
                continue

            if not is_family_root(parent_1) or not is_family_root(parent_2):
                continue

            seen = set()
            for root_id in [parent_1, parent_2]:
                if root_id and root_id not in seen:
                    seen.add(root_id)
                    roots.append(root_id)

        if not roots:
            continue

        for root_id in roots:
            if root_id not in owned_token_ids and should_check_root_alive_state(root_id):
                roots_to_check.add(root_id)

        simple_candidates.append({
            "token_id": token_id,
            "roots": roots,
        })

    batch_lookup = fetch_root_records_in_batch(
        sorted(roots_to_check, key=lambda x: safe_int(x) or 0),
        contract_addresses=contract_addresses,
    )

    for entry in simple_candidates:
        token_id = entry["token_id"]
        roots = entry["roots"]

        root_status_map = {}

        for root_id in roots:
            if root_id in owned_token_ids:
                root_status_map[root_id] = {
                    "root_check_status": "skipped",
                    "is_dead_root": False,
                    "last_checked_at": now_utc,
                }
                continue

            if not should_check_root_alive_state(root_id):
                root_status_map[root_id] = {
                    "root_check_status": "skipped",
                    "is_dead_root": False,
                    "last_checked_at": now_utc,
                }
                continue

            root_chicken = batch_lookup.get(root_id)

            if root_chicken is None:
                root_status_map[root_id] = {
                    "root_check_status": "pending",
                    "is_dead_root": False,
                    "last_checked_at": None,
                }
            elif is_dead_chicken(root_chicken):
                root_status_map[root_id] = {
                    "root_check_status": "dead_checked",
                    "is_dead_root": True,
                    "last_checked_at": now_utc,
                }
            else:
                root_status_map[root_id] = {
                    "root_check_status": "alive_checked",
                    "is_dead_root": False,
                    "last_checked_at": now_utc,
                }

        insert_family_root_items(
            wallet_address=wallet_address,
            token_id=token_id,
            roots=roots,
            owned_root_ids=owned_token_ids,
            root_status_map=root_status_map,
        )

        stored_items = get_family_root_items(wallet_address, token_id)
        summary = build_family_root_summary_from_items(
            token_id=token_id,
            root_items=stored_items,
            owned_token_ids=owned_token_ids,
        )
        upsert_family_root_summary(wallet_address, summary)
        
