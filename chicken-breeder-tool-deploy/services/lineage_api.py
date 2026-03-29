import time
import requests
from collections import deque
from services.family_roots import build_family_root_summary

LINEAGE_API_BASE = "https://breeding.sabongsaga-services.com/lineages"
ROOT_MAX_ID = 11110

_LINEAGE_CACHE = {}
_LINEAGE_CACHE_TTL = 300  # 5 minutes


def _cache_get(key):
    row = _LINEAGE_CACHE.get(key)
    if not row:
        return None

    expires_at, value = row
    if time.time() > expires_at:
        _LINEAGE_CACHE.pop(key, None)
        return None

    return value


def _cache_set(key, value, ttl=_LINEAGE_CACHE_TTL):
    _LINEAGE_CACHE[key] = (time.time() + ttl, value)


def fetch_lineage_tree(token_id: str, depth: int = 6, max_retries: int = 4, base_delay: float = 1.5):
    token_id = str(token_id).strip()
    if not token_id:
        return None

    cache_key = f"{token_id}:{depth}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    last_error = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(
                LINEAGE_API_BASE,
                params={"tokenId": token_id, "depth": depth},
                timeout=60,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else (base_delay * (2 ** attempt))
                time.sleep(delay)
                continue

            if response.status_code in {500, 502, 503, 504}:
                if attempt < max_retries:
                    time.sleep(base_delay * (2 ** attempt))
                    continue

            response.raise_for_status()

            data = response.json()
            tree = data.get("data", {}).get("tree")
            tree = tree if isinstance(tree, dict) else None
            _cache_set(cache_key, tree)
            return tree

        except requests.RequestException as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(base_delay * (2 ** attempt))
                continue
            break

    raise last_error
