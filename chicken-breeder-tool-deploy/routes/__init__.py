from routes.core import register_core_routes
from routes.analysis import register_analysis_routes
from routes.best_pairs import register_best_pair_routes
from routes.match import register_match_routes
from routes.planner import register_planner_routes

__all__ = [
    "register_core_routes",
    "register_analysis_routes",
    "register_best_pair_routes",
    "register_match_routes",
    "register_planner_routes",
]
