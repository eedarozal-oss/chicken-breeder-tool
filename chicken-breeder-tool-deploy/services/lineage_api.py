import requests
from collections import deque
from services.family_roots import build_family_root_summary

LINEAGE_API_BASE = "https://breeding.sabongsaga-services.com/lineages"
ROOT_MAX_ID = 11110


def fetch_lineage_tree(token_id: str, depth: int = 6):
    token_id = str(token_id).strip()
    if not token_id:
        return None

    response = requests.get(
        LINEAGE_API_BASE,
        params={"tokenId": token_id, "depth": depth},
        timeout=60,
    )
    response.raise_for_status()

    data = response.json()
    tree = data.get("data", {}).get("tree")
    return tree if isinstance(tree, dict) else None


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _find_first_token_nodes(obj):
    """
    Find direct descendant lineage nodes.
    Stops descending once a dict with tokenId is found.
    """
    found = []

    def walk(value):
        if isinstance(value, dict):
            if "tokenId" in value:
                found.append(value)
                return
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(obj)
    return found


def extract_roots_and_unresolved_from_tree(tree, root_max_id=ROOT_MAX_ID):
    """
    Returns:
    {
        "roots": set[str],
        "unresolved": set[str]
    }

    unresolved = non-root leaf tokens where this returned tree no longer expands
    into child tokenId nodes.
    """
    if not isinstance(tree, dict) or "tokenId" not in tree:
        return {"roots": set(), "unresolved": set()}

    roots = set()
    unresolved = set()

    def walk(node):
        token_num = _safe_int(node.get("tokenId"))
        if token_num is None:
            return

        token_id = str(token_num)

        if token_num <= root_max_id:
            roots.add(token_id)
            return

        child_nodes = []
        for key, value in node.items():
            if key == "tokenId":
                continue
            child_nodes.extend(_find_first_token_nodes(value))

        seen = set()
        deduped_children = []
        for child in child_nodes:
            child_id = str(child.get("tokenId") or "").strip()
            if child_id and child_id not in seen and child_id != token_id:
                seen.add(child_id)
                deduped_children.append(child)

        if not deduped_children:
            unresolved.add(token_id)
            return

        for child in deduped_children:
            walk(child)

    walk(tree)
    return {"roots": roots, "unresolved": unresolved}


def complete_ninuno_via_lineage(token_id: str, owned_token_ids, depth: int = 6, max_tokens: int = 60, contract_addresses=None):
    """
    Better Ninuno completion strategy:
    - fetch lineage tree for the selected chicken
    - collect resolved roots
    - find unresolved non-root leaf tokens
    - fetch NEW lineage trees for those unresolved tokens
    - merge discovered roots
    - repeat until queue is empty or limit is hit
    - exclude dead Ninuno roots from numerator and denominator
    """
    token_id = str(token_id).strip()

    if not token_id:
        return {
            "token_id": "",
            "owned_root_count": 0,
            "total_root_count": 0,
            "ownership_percent": 0.0,
            "is_complete": 0,
            "roots": [],
            "dead_roots": [],
        }

    token_num = _safe_int(token_id)
    if token_num is not None and token_num <= ROOT_MAX_ID:
        return build_family_root_summary(
            token_id=token_id,
            roots={token_id},
            owned_token_ids=owned_token_ids,
            is_complete=True,
            contract_addresses=contract_addresses,
        )

    roots_found = set()
    pending = deque([token_id])
    processed = set()
    had_fetch_failure = False

    while pending and len(processed) < max_tokens:
        current = str(pending.popleft()).strip()
        if not current or current in processed:
            continue

        processed.add(current)

        current_num = _safe_int(current)
        if current_num is not None and current_num <= ROOT_MAX_ID:
            roots_found.add(current)
            continue

        tree = fetch_lineage_tree(current, depth=depth)
        if not tree:
            had_fetch_failure = True
            continue

        extracted = extract_roots_and_unresolved_from_tree(tree)
        roots_found.update(extracted["roots"])

        for unresolved_token in extracted["unresolved"]:
            unresolved_num = _safe_int(unresolved_token)
            if unresolved_num is not None and unresolved_num <= ROOT_MAX_ID:
                roots_found.add(unresolved_token)
            elif unresolved_token not in processed:
                pending.append(unresolved_token)

    is_complete = bool(roots_found) and not pending and not had_fetch_failure

    return build_family_root_summary(
        token_id=token_id,
        roots=roots_found,
        owned_token_ids=owned_token_ids,
        is_complete=is_complete,
        contract_addresses=contract_addresses,
    )
