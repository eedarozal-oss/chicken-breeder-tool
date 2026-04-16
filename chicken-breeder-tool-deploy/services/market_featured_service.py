from services.market_candidate_refresh import refresh_market_candidate_cache
from services.market_featured_feed import get_featured_market_rows


def get_featured_market_feed(mode, target_count=8, batch_size=20):
    refresh_market_candidate_cache()

    return get_featured_market_rows(
        mode=mode,
        target_count=target_count,
        batch_size=batch_size,
    )
