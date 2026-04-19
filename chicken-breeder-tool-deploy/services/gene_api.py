from datetime import datetime, timezone
import requests

GENE_API_TEMPLATE = "https://chicken-api-ivory.vercel.app/api/genes/{token_id}"

GENE_SLOT_MAP = {
    "Feet": "feet",
    "Tail": "tail",
    "Body": "body",
    "Wings": "wings",
    "Eyes": "eyes",
    "Beak": "beak",
    "Comb": "comb",
}


def fetch_gene_profile(token_id: str):
    token_id = str(token_id or "").strip()
    if not token_id:
        return None

    response = requests.get(
        GENE_API_TEMPLATE.format(token_id=token_id),
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    if not payload.get("success"):
        return None

    decoded_gene = payload.get("decodedGene") or {}
    if not isinstance(decoded_gene, dict):
        return None

    return decoded_gene


def flatten_gene_profile(decoded_gene: dict):
    if not decoded_gene:
        return {}

    result = {}

    for api_slot, field_prefix in GENE_SLOT_MAP.items():
        slot_gene = decoded_gene.get(api_slot) or {}
        if not isinstance(slot_gene, dict):
            continue

        result[f"{field_prefix}_h1"] = slot_gene.get("h1")
        result[f"{field_prefix}_h2"] = slot_gene.get("h2")
        result[f"{field_prefix}_h3"] = slot_gene.get("h3")

    result["gene_profile_loaded"] = 1
    result["gene_last_updated"] = datetime.now(timezone.utc).isoformat()

    return result


def fetch_and_flatten_gene_profile(token_id: str):
    decoded_gene = fetch_gene_profile(token_id)
    if not decoded_gene:
        return None

    return flatten_gene_profile(decoded_gene)
