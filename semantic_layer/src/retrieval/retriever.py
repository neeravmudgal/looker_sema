"""
Core retrieval pipeline — ANN search + graph traversal + explore scoring.

WHY: This is where natural language intent becomes concrete field selections.
     The pipeline combines vector similarity (find semantically relevant fields)
     with graph structure (verify those fields are reachable from a single explore)
     to produce a confident, correct result — or detect ambiguity.

CALLED BY: turn_handler.py
CALLS: embedder (vector search), cache (field-to-explore mapping),
       ambiguity_detector (check for conflicts)

THE 5-STEP PIPELINE:
  1. Embed the structured intent string (not raw query — better matching)
  2. ANN search Neo4j vector index for candidate fields + explores
  3. Use the in-memory cache to map each field to its explore(s)
  4. Score each explore by coverage, penalize by join count
  5. Run ambiguity detection before returning
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional

from neo4j import Driver

from src.config import settings
from src.parser.models import LookMLField, LookMLJoin, LookMLExplore
from src.graph.cache import ExploreContextCache
from src.graph import graph_queries as Q
from src.embeddings.embedder import Embedder
from src.retrieval.ambiguity_detector import AmbiguityDetector, AmbiguityResult

logger = logging.getLogger(__name__)


@dataclass
class RetrievalResult:
    """
    Complete result from the retrieval pipeline.

    If needs_clarification is True, the caller should present the
    clarification_question and options to the user instead of generating SQL.
    """

    explore_name: Optional[str] = None
    model_name: Optional[str] = None
    selected_fields: List[LookMLField] = dataclass_field(default_factory=list)
    joins_needed: List[dict] = dataclass_field(default_factory=list)
    confidence_score: float = 0.0
    needs_clarification: bool = False
    clarification_question: str = ""
    clarification_options: List[str] = dataclass_field(default_factory=list)
    warnings: List[str] = dataclass_field(default_factory=list)
    alternatives: List[dict] = dataclass_field(default_factory=list)
    ambiguity_type: Optional[str] = None
    # Store raw candidates so ambiguity resolution can pick from them
    _all_candidates: List[dict] = dataclass_field(default_factory=list)
    _explore_scores: Dict[str, float] = dataclass_field(default_factory=dict)


class Retriever:
    """
    Orchestrates the full retrieval pipeline.

    Usage:
        retriever = Retriever(driver, cache, embedder)
        result = retriever.retrieve(intent, user_query)
    """

    def __init__(
        self,
        driver: Driver,
        cache: ExploreContextCache,
        embedder: Embedder,
    ):
        self._driver = driver
        self._cache = cache
        self._embedder = embedder
        self._ambiguity_detector = AmbiguityDetector(cache)

    def retrieve(self, intent: dict, user_query: str) -> RetrievalResult:
        """
        Run the full 5-step retrieval pipeline.

        Args:
            intent: Structured intent from IntentExtractor.
            user_query: Original user query (for ambiguity messages).

        Returns:
            RetrievalResult with selected explore, fields, and confidence.
        """
        result = RetrievalResult()

        # ── Step 1: Build and embed the intent string ─────────────
        # We embed the structured intent rather than the raw query because
        # "revenue by country" matches field embeddings better than
        # "Show me total revenue broken down by user country last quarter"
        intent_text = self._build_intent_string(intent)
        logger.info("Embedding intent: '%s'", intent_text)

        try:
            query_embedding = self._embedder.embed_query(intent_text)
        except Exception as exc:
            logger.error("Failed to embed query: %s", exc)
            result.warnings.append(f"Embedding failed: {exc}")
            return result

        # ── Step 2: ANN search for candidate fields ───────────────
        candidates = self._ann_search_fields(query_embedding)
        if not candidates:
            logger.info("No field candidates found via ANN search")
            return result

        result._all_candidates = candidates

        # ── Step 3: Map each candidate to its explore(s) via cache ─
        # Group candidates by explore, counting how many requested
        # fields each explore can serve
        explore_field_map = self._group_by_explore(candidates, intent)

        if not explore_field_map:
            logger.info("No explores found that match the requested fields")
            return result

        # ── Step 4: Score each explore ────────────────────────────
        explore_scores = self._score_explores(explore_field_map, intent)
        result._explore_scores = explore_scores

        if not explore_scores:
            return result

        # Sort explores by score
        sorted_explores = sorted(explore_scores.items(), key=lambda x: x[1], reverse=True)
        best_explore_name, best_score = sorted_explores[0]

        # Check confidence threshold
        if best_score < settings.confidence_threshold:
            result.confidence_score = best_score
            # Return top suggestions even though confidence is low
            result.warnings.append(
                f"Low confidence ({best_score:.2f}). "
                f"Closest matches: {', '.join(e[0] for e in sorted_explores[:3])}"
            )
            # Still populate the result so caller can decide
            self._populate_result(result, best_explore_name, explore_field_map, best_score)
            return result

        # ── Step 5: Ambiguity detection ───────────────────────────
        ambiguity = self._ambiguity_detector.detect(
            candidates=candidates,
            intent=intent,
            explore_scores=explore_scores,
            user_query=user_query,
        )

        if ambiguity.detected:
            result.needs_clarification = ambiguity.is_blocking
            result.ambiguity_type = ambiguity.ambiguity_type
            result.clarification_question = ambiguity.question
            result.clarification_options = ambiguity.options
            # Store the conflicting field candidates so resolution can use them
            result._ambiguity_field_candidates = ambiguity.field_candidates
            if not ambiguity.is_blocking:
                # Non-blocking: add as warning and still populate result
                result.warnings.append(ambiguity.question)

        # Populate the result with the best explore's fields
        self._populate_result(result, best_explore_name, explore_field_map, best_score)

        # Track alternatives if top-2 are close
        if len(sorted_explores) >= 2:
            second_name, second_score = sorted_explores[1]
            if best_score - second_score < settings.ambiguity_threshold:
                result.alternatives.append({
                    "explore": second_name,
                    "score": second_score,
                })

        # ── Fanout warning ────────────────────────────────────────
        self._check_fanout_warnings(result)

        # ── PDT warning ───────────────────────────────────────────
        self._check_pdt_warnings(result)

        return result

    def _build_intent_string(self, intent: dict) -> str:
        """
        Build a search-optimized string from structured intent.

        This string bridges the gap between natural language queries and the
        verbose field embedding text format. We include contextual keywords
        that help the embedding model match against field descriptions.
        """
        parts = []

        metrics = intent.get("metrics", [])
        if metrics:
            # Include both the metric name and "measure" keyword to match field embeddings
            metric_text = " ".join(metrics)
            parts.append(f"measure {metric_text}")

        dimensions = intent.get("dimensions", [])
        if dimensions:
            dim_text = " ".join(dimensions)
            parts.append(f"dimension {dim_text} grouped by {dim_text}")

        time_range = intent.get("time_range", {}) or {}
        period = time_range.get("period", "")
        if period:
            parts.append(f"time {period.replace('_', ' ')}")

        grain = time_range.get("grain", "")
        if grain:
            parts.append(f"granularity {grain}")

        filters = intent.get("filters", [])
        if filters:
            filter_parts = []
            for f in filters:
                if isinstance(f, dict):
                    filter_parts.append(f.get("field", str(f)))
                else:
                    filter_parts.append(str(f))
            parts.append("filtered by " + " ".join(filter_parts))

        attribution = intent.get("attribution_hint")
        if attribution:
            parts.append(f"attribution {attribution.replace('_', ' ')}")

        return " ".join(parts) if parts else "general query"

    def _ann_search_fields(self, embedding: List[float]) -> List[dict]:
        """
        Query the Neo4j vector index for candidate fields.

        Returns a list of dicts with field info + similarity score.
        """
        candidates = []
        try:
            with self._driver.session() as session:
                result = session.run(
                    Q.ANN_SEARCH_FIELDS,
                    k=settings.top_k_fields,
                    embedding=embedding,
                )
                for record in result:
                    candidates.append({
                        "field_name": record["field_name"],
                        "view_name": record["view_name"],
                        "explore_name": record["explore_name"],
                        "field_type": record["field_type"],
                        "data_type": record.get("data_type", ""),
                        "label": record.get("label", ""),
                        "description": record.get("description", ""),
                        "tags": record.get("tags", []) or [],
                        "sql": record.get("sql", ""),
                        "model_name": record.get("model_name", ""),
                        "score": record["score"],
                    })
        except Exception as exc:
            logger.error("ANN search failed: %s", exc)

        logger.info("ANN search returned %d candidates", len(candidates))
        return candidates

    def _group_by_explore(
        self,
        candidates: List[dict],
        intent: dict,
    ) -> Dict[str, List[dict]]:
        """
        Group candidate fields by explore, using the cache's reverse index.

        For each candidate field, we look up which explores contain it.
        Result: explore_name → [candidate_dicts]

        IMPORTANT: Skips hidden explores — they should never appear in results.
        """
        explore_field_map: Dict[str, List[dict]] = {}

        # Build set of hidden explores to skip
        hidden_explores = set()
        for name in self._cache.all_explore_names():
            ctx = self._cache.get_explore(name)
            if ctx and ctx.get("explore", {}).get("is_hidden", False):
                hidden_explores.add(name)

        # Also get all valid (non-hidden) explore names
        valid_explores = set(self._cache.all_explore_names()) - hidden_explores

        for candidate in candidates:
            field_id = f"{candidate['view_name']}.{candidate['field_name']}"
            explores = self._cache.get_explores_for_field(field_id)

            for explore_name in explores:
                # Skip hidden explores
                if explore_name in hidden_explores:
                    continue
                # Skip explores not in our valid set
                if explore_name not in valid_explores:
                    continue

                if explore_name not in explore_field_map:
                    explore_field_map[explore_name] = []
                # Avoid duplicates within same explore
                existing = {
                    f"{c['view_name']}.{c['field_name']}"
                    for c in explore_field_map[explore_name]
                }
                if field_id not in existing:
                    explore_field_map[explore_name].append(candidate)

        return explore_field_map

    def _score_explores(
        self,
        explore_field_map: Dict[str, List[dict]],
        intent: dict,
    ) -> Dict[str, float]:
        """
        Score each explore by how well it covers the requested fields.

        Scoring formula:
          best_score  = highest similarity score among matched fields
          avg_score   = average similarity of top-N fields (N = requested count)
            (Only average the most relevant fields, not ANN noise)
          blended     = 0.6 * best_score + 0.4 * avg_top_score
          coverage_bonus = small bonus when the explore covers multiple requested slots
          join_penalty = 0.005 per join (very light — joins are normal in LookML)
          final       = blended + coverage_bonus - join_penalty

        Key change: coverage is now an additive BONUS (0-0.1), not a multiplicative
        factor. The old formula (blended * coverage) halved scores when only 1 of 2
        requested fields was found, which is too aggressive — finding 1 field well
        is a strong signal even if the second isn't in the ANN top-k.
        """
        scores: Dict[str, float] = {}

        requested_count = max(
            len(intent.get("metrics", [])) + len(intent.get("dimensions", [])),
            1,
        )

        for explore_name, matched_fields in explore_field_map.items():
            if not matched_fields:
                continue

            field_scores = sorted([f["score"] for f in matched_fields], reverse=True)
            best_score = field_scores[0]

            # Average only the top-N scores where N = requested field count
            # This avoids diluting the score with low-relevance ANN tail results
            top_n = min(requested_count, len(field_scores))
            avg_top_score = sum(field_scores[:top_n]) / top_n

            # Blend: weight the best match and top-N average
            blended = 0.6 * best_score + 0.4 * avg_top_score

            # Coverage bonus: small additive reward for covering more requested slots
            # Capped at 0.1 so it nudges but doesn't dominate
            coverage_ratio = min(len(matched_fields) / requested_count, 1.0)
            coverage_bonus = 0.1 * coverage_ratio

            # Very light join penalty — joins are normal, not a red flag
            ctx = self._cache.get_explore(explore_name)
            join_count = len(ctx.get("joins", [])) if ctx else 0
            join_penalty = 0.005 * join_count

            final_score = blended + coverage_bonus - join_penalty
            scores[explore_name] = max(final_score, 0.0)

            logger.debug(
                "Explore '%s': best=%.3f avg_top=%.3f coverage=%.2f joins=%d → score=%.3f",
                explore_name, best_score, avg_top_score, coverage_ratio, join_count, final_score,
            )

        return scores

    def _populate_result(
        self,
        result: RetrievalResult,
        explore_name: str,
        explore_field_map: Dict[str, List[dict]],
        score: float,
    ) -> None:
        """Fill the RetrievalResult with the selected explore's data."""
        ctx = self._cache.get_explore(explore_name)
        if not ctx:
            return

        result.explore_name = explore_name
        result.model_name = ctx.get("model_name", "")
        result.confidence_score = score
        result.joins_needed = ctx.get("joins", [])

        # Convert matched candidates to LookMLField objects
        matched = explore_field_map.get(explore_name, [])
        for m in matched:
            field = LookMLField(
                name=m["field_name"],
                view_name=m["view_name"],
                field_type=m.get("field_type", ""),
                data_type=m.get("data_type", ""),
                sql=m.get("sql", ""),
                label=m.get("label", ""),
                description=m.get("description", ""),
                tags=m.get("tags", []),
                explore_name=explore_name,
                model_name=result.model_name,
            )
            result.selected_fields.append(field)

    def _check_fanout_warnings(self, result: RetrievalResult) -> None:
        """
        Warn if a SUM/COUNT measure is used alongside a one-to-many join.

        The risk: when a one_to_many join is present and any field from that
        joined view is selected, rows from OTHER views get duplicated.
        So a measure from the base view or another many_to_one view will
        be double-counted.

        We also warn about measures FROM the one_to_many view itself,
        since they are aggregated across the fanout.
        """
        measure_fields = [f for f in result.selected_fields if f.field_type == "measure"]
        if not measure_fields:
            return

        # Identify one_to_many joined views
        otm_views = set()
        for join in result.joins_needed:
            if join.get("relationship") == "one_to_many":
                otm_views.add(join.get("view_name"))

        if not otm_views:
            return

        # Check if any field from a one_to_many view is selected (dimension or measure)
        selected_views = {f.view_name for f in result.selected_fields}
        active_otm = otm_views & selected_views

        if not active_otm:
            # No fields selected from the one_to_many view, so Looker won't include
            # that join — no fanout risk
            return

        for mf in measure_fields:
            # Warn about measures that could be inflated by the fanout
            # This includes measures from the one_to_many view AND measures
            # from other views that will see duplicated rows
            otm_view_str = ", ".join(sorted(active_otm))
            if mf.view_name in active_otm:
                result.warnings.append(
                    f"Warning: '{mf.label}' is aggregated across a one-to-many join "
                    f"with '{otm_view_str}'. This may cause double-counting."
                )
            else:
                result.warnings.append(
                    f"Warning: '{mf.label}' may be inflated because fields from "
                    f"'{otm_view_str}' (one-to-many) are also selected, causing row duplication."
                )

    def _check_pdt_warnings(self, result: RetrievalResult) -> None:
        """Add a note if any selected field comes from a derived table."""
        ctx = self._cache.get_explore(result.explore_name) if result.explore_name else None
        if not ctx:
            return

        pdt_views = set()
        for join in ctx.get("joins", []):
            if join.get("is_pdt"):
                pdt_views.add(join["view_name"])

        for field in result.selected_fields:
            if field.view_name in pdt_views:
                result.warnings.append(
                    f"Note: '{field.view_name}' is a derived table that refreshes "
                    f"periodically. Data may not reflect the most recent changes."
                )
                break  # One warning per PDT view is enough
