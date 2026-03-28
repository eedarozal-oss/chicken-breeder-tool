from services.gene_api import fetch_and_flatten_gene_profile
from services.gene_classifier import classify_gene_profile


def should_fetch_gene_profile(chicken):
    return (
        chicken
        and not chicken.get("is_egg")
        and str(chicken.get("state") or "").strip().lower() == "normal"
        and not chicken.get("gene_profile_loaded")
    )


def enrich_chicken_record(chicken):
    working = dict(chicken or {})

    if should_fetch_gene_profile(working):
        try:
            gene_fields = fetch_and_flatten_gene_profile(working.get("token_id"))
            if gene_fields:
                working.update(gene_fields)
        except Exception:
            pass

    classification_fields = classify_gene_profile(working)
    working.update(classification_fields)

    return working


def enrich_chicken_records(chickens):
    return [enrich_chicken_record(chicken) for chicken in (chickens or [])]
