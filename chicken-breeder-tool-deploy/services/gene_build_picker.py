from services.builds_config import BUILD_PRIORITY
from services.gene_breeding import get_gene_build_target_info


def get_best_available_gene_build_info(chicken, build_options=None):
    resolved_builds = [
        str(build_key or "").strip().lower()
        for build_key in (build_options or BUILD_PRIORITY)
        if str(build_key or "").strip()
    ]
    build_rank = {build_key: index for index, build_key in enumerate(resolved_builds)}

    best_info = {
        "build_key": "",
        "build_label": "",
        "build_count_display": "",
        "source": "",
        "display_source": "",
        "sort_source_rank": 99,
        "sort_match_count": 0,
        "sort_match_total": 0,
    }

    for build_key in resolved_builds:
        info = get_gene_build_target_info(chicken, build_key)
        if not info.get("eligible"):
            continue

        current = {
            "build_key": build_key,
            "build_label": build_key.title(),
            "build_count_display": info.get("display_match") or "",
            "source": info.get("source") or "",
            "display_source": info.get("display_source") or "",
            "sort_source_rank": info.get("sort_source_rank", 99),
            "sort_match_count": info.get("sort_match_count", 0),
            "sort_match_total": info.get("sort_match_total", 0),
        }

        current_rank = (
            current["sort_source_rank"],
            -(current["sort_match_count"] or 0),
            -(current["sort_match_total"] or 0),
            build_rank.get(build_key, 999),
        )
        best_rank = (
            best_info["sort_source_rank"],
            -(best_info["sort_match_count"] or 0),
            -(best_info["sort_match_total"] or 0),
            build_rank.get(best_info["build_key"], 999),
        )

        if not best_info["build_key"] or current_rank < best_rank:
            best_info = current

    return best_info
