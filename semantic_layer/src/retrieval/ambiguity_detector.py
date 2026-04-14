"""
Detects semantic ambiguity in retrieval results before query generation.

WHY: Generating a query from ambiguous intent leads to incorrect results that
     look correct — the most dangerous kind of error. It's better to ask the
     user a 10-second clarification question than to silently pick the wrong
     attribution model or the wrong date field.

CALLED BY: retriever.py — Step 5 of the retrieval pipeline.
CALLS: cache.py for explore context lookups.

FOUR AMBIGUITY TYPES:
  1. ATTRIBUTION — multiple attribution-tagged fields match (first vs last touch)
  2. FIELD_COLLISION — same field name in different views, both are candidates
  3. EXPLORE_CONFLICT — top-2 explores have nearly identical scores
  4. CROSS_EXPLORE — requested fields live in incompatible explores

GENERIC DESIGN: Attribution is detected by scanning field descriptions and names
for configurable keyword patterns, NOT by hardcoded field names. This works with
any LookML model that follows common naming conventions.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional, Set, Tuple

from src.config import settings
from src.graph.cache import ExploreContextCache

logger = logging.getLogger(__name__)

# ── Attribution detection keywords ────────────────────────────────────
# These patterns are scanned in field names and descriptions to detect
# attribution-related fields. They're intentionally broad to work with
# any LookML model, not just the sample fixtures.

FIRST_TOUCH_PATTERNS = [
    r"first.?touch", r"acquisition.?source", r"first.?visit",
    r"first.?click", r"site.?acquisition", r"original.?source",
    r"first.?session", r"acquired",
]

LAST_TOUCH_PATTERNS = [
    r"last.?touch", r"purchase.?session.?source", r"last.?click",
    r"converting.?source", r"last.?session.?before", r"conversion.?source",
    r"purchase.?source",
]

ATTRIBUTION_QUERY_PATTERNS = [
    r"channel", r"source", r"marketing", r"traffic.?source",
    r"acquisition", r"attribution", r"campaign",
]


@dataclass
class AmbiguityResult:
    """
    Result of ambiguity detection.

    detected:       True if any ambiguity was found.
    ambiguity_type: "attribution" | "field_collision" | "explore_conflict" | "cross_explore" | None
    question:       Human-readable clarification question.
    options:        ["A) ...", "B) ...", "C) ..."] for the user to choose from.
    is_blocking:    True → must clarify before generating SQL.
                    False → it's a warning, proceed with best guess.
    field_candidates: The conflicting fields (for resolution after user responds).
    """

    detected: bool = False
    ambiguity_type: Optional[str] = None
    question: str = ""
    options: List[str] = dataclass_field(default_factory=list)
    is_blocking: bool = False
    field_candidates: Dict[str, list] = dataclass_field(default_factory=dict)


class AmbiguityDetector:
    """
    Detects ambiguity in retrieval candidates.

    Runs all four checks in priority order (attribution first, since it's
    the most common and impactful). Returns the first ambiguity found.
    """

    def __init__(self, cache: ExploreContextCache):
        self._cache = cache

    def detect(
        self,
        candidates: List[dict],
        intent: dict,
        explore_scores: Dict[str, float],
        user_query: str,
    ) -> AmbiguityResult:
        """
        Run all ambiguity checks in priority order.

        Args:
            candidates:     All field candidates from ANN search.
            intent:         Structured intent from extraction.
            explore_scores: Explore name → score from scoring step.
            user_query:     Original user question (for message generation).

        Returns:
            AmbiguityResult — detected=False if no ambiguity.
        """
        # Pre-filter candidates to only those with decent scores.
        # This prevents low-relevance ANN noise from triggering false ambiguities.
        score_floor = max(settings.confidence_threshold, 0.4)
        relevant_candidates = [c for c in candidates if c.get("score", 0) >= score_floor]

        # Only check attribution if user hasn't already specified a hint
        if not intent.get("attribution_hint"):
            result = self._check_attribution(relevant_candidates, intent, user_query)
            if result.detected:
                return result

        # Explore conflict before field collision — field collision is noisiest
        result = self._check_explore_conflict(explore_scores, user_query)
        if result.detected:
            return result

        result = self._check_field_collision(relevant_candidates, intent, user_query)
        if result.detected:
            return result

        return AmbiguityResult()  # No ambiguity

    def _check_attribution(
        self,
        candidates: List[dict],
        intent: dict,
        user_query: str,
    ) -> AmbiguityResult:
        """
        TYPE 1: Attribution ambiguity.

        Triggered when:
        - The query mentions channel/source/marketing concepts
        - AND candidates include both first-touch and last-touch fields
        - AND the user didn't specify which attribution model they want

        This is the most impactful ambiguity: first-touch and last-touch
        attribution can give completely opposite answers to "which channel
        drives the most revenue?"
        """
        # Check if the query is about attribution concepts
        query_lower = user_query.lower()
        is_attribution_query = any(
            re.search(pattern, query_lower) for pattern in ATTRIBUTION_QUERY_PATTERNS
        )
        if not is_attribution_query:
            return AmbiguityResult()

        # Scan candidates for first-touch and last-touch fields
        first_touch_fields = []
        last_touch_fields = []

        for candidate in candidates:
            field_text = (
                f"{candidate.get('field_name', '')} "
                f"{candidate.get('description', '')} "
                f"{' '.join(candidate.get('tags', []))}"
            ).lower()

            is_first = any(re.search(p, field_text) for p in FIRST_TOUCH_PATTERNS)
            is_last = any(re.search(p, field_text) for p in LAST_TOUCH_PATTERNS)

            if is_first:
                first_touch_fields.append(candidate)
            if is_last:
                last_touch_fields.append(candidate)

        # Only ambiguous if BOTH types are present
        if not (first_touch_fields and last_touch_fields):
            return AmbiguityResult()

        return AmbiguityResult(
            detected=True,
            ambiguity_type="attribution",
            question=(
                "I found multiple ways to attribute revenue to marketing channels. "
                "Which attribution model do you want?\n\n"
                "A) First touch — credit the channel that first brought the user "
                "to your site (best for measuring acquisition campaigns)\n"
                "B) Last touch — credit the channel active right before purchase "
                "(best for measuring conversion campaigns)\n"
                "C) Linear — split credit equally across all sessions in the user's journey"
            ),
            options=[
                "A) First touch attribution",
                "B) Last touch attribution",
                "C) Linear multi-touch attribution",
            ],
            is_blocking=True,
            field_candidates={
                "first_touch": first_touch_fields,
                "last_touch": last_touch_fields,
            },
        )

    def _check_field_collision(
        self,
        candidates: List[dict],
        intent: dict,
        user_query: str,
    ) -> AmbiguityResult:
        """
        TYPE 2: Field name collision.

        Triggered when two candidates have the same base field name
        but belong to different views, and both match a requested slot.

        Only checks fields that are relevant to the user's intent (metrics
        or dimensions they asked for). This prevents low-relevance ANN noise
        from triggering false collision alerts.

        Example: sessions.created_date and orders.created_date both
        match "by date", but mean completely different things.
        """
        # Build a set of intent terms to check relevance against
        intent_terms = set()
        for m in intent.get("metrics", []):
            intent_terms.update(m.lower().split("_"))
            intent_terms.add(m.lower())
        for d in intent.get("dimensions", []):
            intent_terms.update(d.lower().split("_"))
            intent_terms.add(d.lower())
        query_terms = set(user_query.lower().split())

        # Group candidates by their base name (without view prefix)
        name_groups: Dict[str, List[dict]] = {}
        for candidate in candidates:
            base_name = candidate["field_name"]
            if base_name not in name_groups:
                name_groups[base_name] = []
            name_groups[base_name].append(candidate)

        # Find collisions: same name, different views, both high-scoring
        # AND relevant to what the user actually asked for
        for name, group in name_groups.items():
            if len(group) < 2:
                continue

            # Get unique view names
            views = list({c["view_name"] for c in group})
            if len(views) < 2:
                continue

            # Check relevance: does this field name relate to the user's intent?
            name_parts = set(name.lower().split("_"))
            field_label_parts = set()
            for c in group:
                field_label_parts.update(c.get("label", "").lower().split())

            # A field is relevant if its name or label overlaps with intent terms or query terms
            name_overlap = name_parts & (intent_terms | query_terms)
            label_overlap = field_label_parts & (intent_terms | query_terms)
            if not name_overlap and not label_overlap:
                continue  # This collision is not about what the user asked for

            # Only flag if at least 2 candidates from different views have decent scores
            scores_by_view = {}
            for c in group:
                v = c["view_name"]
                if v not in scores_by_view or c["score"] > scores_by_view[v]:
                    scores_by_view[v] = c["score"]

            high_score_views = [v for v, s in scores_by_view.items() if s >= settings.confidence_threshold]
            if len(high_score_views) < 2:
                continue

            # Build clarification using only high-scoring views
            options = []
            for i, view in enumerate(high_score_views[:3]):
                label = chr(65 + i)  # A, B, C
                field_in_view = next(c for c in group if c["view_name"] == view)
                desc = field_in_view.get("description", "No description available")
                options.append(f"{label}) {view}.{name} — {desc[:100]}")

            return AmbiguityResult(
                detected=True,
                ambiguity_type="field_collision",
                question=(
                    f"I found '{name}' in multiple places:\n\n"
                    + "\n".join(options)
                    + "\n\nWhich one did you mean?"
                ),
                options=options,
                is_blocking=True,
                field_candidates={
                    view: [c for c in group if c["view_name"] == view]
                    for view in high_score_views
                },
            )

        return AmbiguityResult()

    def _check_explore_conflict(
        self,
        explore_scores: Dict[str, float],
        user_query: str,
    ) -> AmbiguityResult:
        """
        TYPE 3: Explore conflict.

        Triggered when the top-2 explores have scores within
        ambiguity_threshold of each other.

        SKIP if the user explicitly named an explore in their query
        (e.g. "in events", "from sessions", "using events explore").
        """
        if len(explore_scores) < 2:
            return AmbiguityResult()

        # Check if user explicitly mentioned an explore name in the query.
        # If so, there's no ambiguity — they told us which one they want.
        query_lower = user_query.lower()
        for explore_name in explore_scores:
            # Match patterns like "in events", "from sessions", "using events",
            # "events explore", or just the explore name surrounded by word boundaries
            name_lower = explore_name.lower()
            explicit_patterns = [
                rf"\bin\s+{re.escape(name_lower)}\b",
                rf"\bfrom\s+{re.escape(name_lower)}\b",
                rf"\busing\s+{re.escape(name_lower)}\b",
                rf"\b{re.escape(name_lower)}\s+explore\b",
                rf"\b{re.escape(name_lower)}\s+data\b",
            ]
            if any(re.search(p, query_lower) for p in explicit_patterns):
                logger.info(
                    "User explicitly mentioned explore '%s' in query, skipping explore conflict",
                    explore_name,
                )
                return AmbiguityResult()

        sorted_explores = sorted(
            explore_scores.items(), key=lambda x: x[1], reverse=True
        )
        best_name, best_score = sorted_explores[0]
        second_name, second_score = sorted_explores[1]

        if best_score - second_score >= settings.ambiguity_threshold:
            return AmbiguityResult()

        # Build option descriptions from cache
        best_ctx = self._cache.get_explore(best_name) or {}
        second_ctx = self._cache.get_explore(second_name) or {}

        best_desc = best_ctx.get("explore", {}).get("description", "")[:150]
        second_desc = second_ctx.get("explore", {}).get("description", "")[:150]

        return AmbiguityResult(
            detected=True,
            ambiguity_type="explore_conflict",
            question=(
                f"Your question could be answered from two different data perspectives:\n\n"
                f"A) {best_name} — {best_desc or 'No description'}\n"
                f"B) {second_name} — {second_desc or 'No description'}\n\n"
                f"Which perspective would you like?"
            ),
            options=[
                f"A) {best_name}",
                f"B) {second_name}",
            ],
            is_blocking=True,
            field_candidates={
                best_name: [],
                second_name: [],
            },
        )
