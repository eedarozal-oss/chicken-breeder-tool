from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from services.wallet_item_inventory import (
    CHICKEN_SAGA_RESOURCE_CONTRACT,
    build_resource_image_url,
    build_wallet_inventory_lookup,
    get_breeding_item_image_url,
    get_breeding_item_token_id,
    normalize_item_name,
)


def _extract_item_name(item_obj: Optional[Dict[str, Any]]) -> str:
    if not isinstance(item_obj, dict):
        return ""
    return normalize_item_name(item_obj.get("name"))


def collect_planner_required_items(queue_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = defaultdict(int)

    for row in queue_rows or []:
        left_item_name = _extract_item_name(row.get("left_item"))
        right_item_name = _extract_item_name(row.get("right_item"))

        if left_item_name:
            counts[left_item_name] += 1
        if right_item_name:
            counts[right_item_name] += 1

    results: List[Dict[str, Any]] = []

    for item_name in sorted(counts.keys()):
        token_id = get_breeding_item_token_id(item_name)
        results.append({
            "name": item_name,
            "token_id": token_id or "",
            "required_count": counts[item_name],
            "image": get_breeding_item_image_url(item_name) or build_resource_image_url(token_id),
        })

    return results


def build_planner_item_requirements_summary(
    queue_rows: List[Dict[str, Any]],
    wallet_inventory_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    required_items = collect_planner_required_items(queue_rows)

    summary_rows: List[Dict[str, Any]] = []
    total_required = 0
    total_missing = 0
    all_available = True
    has_unknown = False

    wallet_inventory_lookup = wallet_inventory_lookup or {}

    for item in required_items:
        token_id = str(item.get("token_id") or "").strip()
        required_count = int(item.get("required_count") or 0)
        inventory_row = wallet_inventory_lookup.get(token_id)

        if token_id:
            available_count = int((inventory_row or {}).get("balance") or 0)
            status = "ready" if available_count >= required_count else "missing"
        else:
            available_count = 0
            status = "unknown"
            has_unknown = True

        missing_count = max(0, required_count - available_count)

        if missing_count > 0:
            all_available = False
            total_missing += missing_count

        summary_rows.append({
            "name": item["name"],
            "token_id": token_id,
            "image": item["image"],
            "required_count": required_count,
            "available_count": available_count,
            "missing_count": missing_count,
            "status": status,
        })

        total_required += required_count

    missing_items = [row for row in summary_rows if row["missing_count"] > 0]
    ready_items = [row for row in summary_rows if row["status"] == "ready"]

    if has_unknown:
        overall_status = "unknown"
    elif all_available:
        overall_status = "ready"
    else:
        overall_status = "missing"

    return {
        "overall_status": overall_status,
        "all_available": all_available and not has_unknown,
        "has_unknown": has_unknown,
        "total_item_types": len(summary_rows),
        "total_required_count": total_required,
        "total_missing_count": total_missing,
        "items": summary_rows,
        "missing_items": missing_items,
        "ready_items": ready_items,
    }


def build_wallet_planner_item_requirements_summary(
    wallet_address: str,
    queue_rows: List[Dict[str, Any]],
    token_address: str = CHICKEN_SAGA_RESOURCE_CONTRACT,
) -> Dict[str, Any]:
    inventory_lookup = build_wallet_inventory_lookup(
        wallet_address=wallet_address,
        token_address=token_address,
    )

    summary = build_planner_item_requirements_summary(
        queue_rows=queue_rows,
        wallet_inventory_lookup=inventory_lookup,
    )

    summary["wallet_address"] = wallet_address
    summary["token_address"] = token_address
    return summary


def build_per_pair_item_status(
    planner_row: Dict[str, Any],
    wallet_inventory_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    wallet_inventory_lookup = wallet_inventory_lookup or {}

    left_item_name = _extract_item_name(planner_row.get("left_item"))
    right_item_name = _extract_item_name(planner_row.get("right_item"))

    item_rows: List[Dict[str, Any]] = []

    for side, item_name in (("left", left_item_name), ("right", right_item_name)):
        if not item_name:
            continue

        token_id = get_breeding_item_token_id(item_name)
        inventory_row = wallet_inventory_lookup.get(str(token_id or "").strip(), {})
        available_count = int(inventory_row.get("balance") or 0)

        item_rows.append({
            "side": side,
            "name": item_name,
            "token_id": str(token_id or ""),
            "image": get_breeding_item_image_url(item_name),
            "required_count": 1,
            "available_count": available_count,
            "missing_count": max(0, 1 - available_count),
            "status": "ready" if available_count >= 1 else ("unknown" if not token_id else "missing"),
        })

    has_unknown = any(row["status"] == "unknown" for row in item_rows)
    has_missing = any(row["missing_count"] > 0 for row in item_rows)

    if has_unknown:
        overall_status = "unknown"
    elif has_missing:
        overall_status = "missing"
    else:
        overall_status = "ready"

    return {
        "pair_key": str(planner_row.get("pair_key") or "").strip(),
        "mode": str(planner_row.get("mode") or "").strip(),
        "left_token_id": str((planner_row.get("left") or {}).get("token_id") or "").strip(),
        "right_token_id": str((planner_row.get("right") or {}).get("token_id") or "").strip(),
        "items": item_rows,
        "overall_status": overall_status,
    }
