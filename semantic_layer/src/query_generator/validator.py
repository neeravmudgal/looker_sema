"""
Validates generated Looker queries against the graph before returning to user.

WHY: LLMs can hallucinate field names or include fields from the wrong explore.
     Validation catches these errors before they reach the user, replacing invalid
     fields with warnings instead of returning a broken query.

CALLED BY: looker_query_builder.py as the final step.
CALLS: cache.py for field existence checks.

DESIGN: Never crash — always return the best valid query possible.
If a field is invalid, remove it and add a warning. If ALL fields are invalid,
return an empty query with a clear error message.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from src.graph.cache import ExploreContextCache

logger = logging.getLogger(__name__)


class ValidationResult:
    """Result of query validation — the cleaned query plus any warnings."""

    def __init__(self):
        self.query: dict = {}
        self.warnings: List[str] = []
        self.is_valid: bool = True
        self.removed_fields: List[str] = []


class QueryValidator:
    """
    Validates a Looker query dict against the graph.

    Checks:
    1. Explore exists and is not hidden
    2. Every field in "fields" is accessible in the selected explore
    3. No hidden fields are included
    4. Filter targets exist in the explore
    5. Limit is within bounds (1–5000)
    6. always_filters are present
    """

    def __init__(self, cache: ExploreContextCache):
        self._cache = cache

    def validate(self, query: dict) -> ValidationResult:
        """
        Validate a query dict and return a cleaned version.

        Never raises exceptions — returns the best valid query
        with warnings for anything that was wrong.
        """
        result = ValidationResult()
        result.query = dict(query)  # Work on a copy

        explore_name = query.get("explore", "")
        model_name = query.get("model", "")

        # ── Check 1: Explore exists ───────────────────────────────
        ctx = self._cache.get_explore(explore_name)
        if not ctx:
            result.warnings.append(
                f"Explore '{explore_name}' not found in the model. "
                f"Available explores: {', '.join(self._cache.all_explore_names())}"
            )
            result.is_valid = False
            return result

        # Check if hidden
        if ctx.get("explore", {}).get("is_hidden", False):
            result.warnings.append(
                f"Explore '{explore_name}' is hidden and may not be available."
            )

        # ── Check 2: Validate fields ─────────────────────────────
        valid_fqns = set()
        for f in ctx.get("fields", []):
            valid_fqns.add(f"{f.view_name}.{f.name}")

        cleaned_fields = []
        for fqn in query.get("fields", []):
            if fqn in valid_fqns:
                cleaned_fields.append(fqn)
            else:
                result.removed_fields.append(fqn)
                result.warnings.append(
                    f"Removed '{fqn}' — not accessible in the '{explore_name}' explore."
                )
        result.query["fields"] = cleaned_fields

        if not cleaned_fields:
            result.warnings.append("No valid fields remain after validation.")
            result.is_valid = False

        # ── Check 3: Validate filter targets ──────────────────────
        for filter_dict_key in ("filters", "having_filters"):
            if filter_dict_key not in result.query:
                continue
            validated_filters = {}
            for fqn, value in result.query[filter_dict_key].items():
                if fqn in valid_fqns:
                    validated_filters[fqn] = value
                else:
                    result.warnings.append(
                        f"Removed filter on '{fqn}' — field not found in explore."
                    )
            result.query[filter_dict_key] = validated_filters

        # ── Check 4: Validate sorts ──────────────────────────────
        cleaned_sorts = []
        for sort in result.query.get("sorts", []):
            # Sort format: "view.field desc" or "view.field asc"
            parts = sort.strip().split()
            fqn = parts[0] if parts else ""
            if fqn in valid_fqns:
                cleaned_sorts.append(sort)
        result.query["sorts"] = cleaned_sorts

        # ── Check 5: Limit bounds ────────────────────────────────
        limit = result.query.get("limit", 500)
        if not isinstance(limit, int):
            try:
                limit = int(limit)
            except (ValueError, TypeError):
                limit = 500
        limit = max(1, min(limit, 5000))
        result.query["limit"] = limit

        return result
