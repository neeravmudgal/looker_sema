"""
Core retrieval pipeline — ANN search + graph traversal + explore scoring.

WHY: This is where natural language intent becomes concrete field selections.
     The pipeline combines vector similarity (find semantically relevant fields)
     with graph structure (verify those fields are reachable from a single explore)
     to produce a confident, correct result — or detect ambiguity.

CALLED BY: turn_handler.py
CALLS: embedder (vector search), cache (field-to-explore mapping),
       ambiguity_detector (check for conflicts)

THE PIPELINE:
  1. Embed the structured intent string (not raw query — better matching)
  2. ANN search Neo4j vector index for candidate fields (deduplicated)
  3. Use the in-memory cache to map each field to its explore(s)
  4. Score each explore by coverage, penalize by join count
  5. Run ambiguity detection before returning
"""

from __future__ import annotations

import logging
import re
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
        """Run the full retrieval pipeline."""
        result = RetrievalResult()

        # ── Step 1: Build and embed the intent string ─────────────
        intent_text = self._build_intent_string(intent)
        logger.info("Embedding intent: '%s'", intent_text)

        try:
            query_embedding = self._embedder.embed_query(intent_text)
        except Exception as exc:
            logger.error("Failed to embed query: %s", exc)
            result.warnings.append(f"Embedding failed: {exc}")
            return result

        # ── Step 2: Hybrid search — vector ANN + fulltext keyword ─
        # Vector finds semantically similar fields.
        # Fulltext finds exact keyword matches in name/label/description.
        # Merge both, deduplicate, keep highest score per field.
        vector_candidates = self._ann_search_fields(query_embedding)
        fulltext_candidates = self._fulltext_search_fields(intent, user_query)
        candidates = self._merge_candidates(vector_candidates, fulltext_candidates)

        if not candidates:
            logger.info("No field candidates found")
            return result

        result._all_candidates = candidates

        # ── Step 3: Map each candidate to its explore(s) via cache ─
        explore_field_map = self._group_by_explore(candidates, intent)

        if not explore_field_map:
            logger.info("No explores found that match the requested fields")
            return result

        # ── Step 4: Score each explore ────────────────────────────
        explore_scores = self._score_explores(explore_field_map, intent)

        # Boost if user explicitly named an explore ("in events", "from sessions")
        explore_scores = self._apply_explicit_explore_boost(explore_scores, user_query)

        result._explore_scores = explore_scores

        if not explore_scores:
            return result

        sorted_explores = sorted(explore_scores.items(), key=lambda x: x[1], reverse=True)
        best_explore_name, best_score = sorted_explores[0]

        if best_score < settings.confidence_threshold:
            result.confidence_score = best_score
            result.warnings.append(
                f"Low confidence ({best_score:.2f}). "
                f"Closest matches: {', '.join(e[0] for e in sorted_explores[:3])}"
            )
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
            result._ambiguity_field_candidates = ambiguity.field_candidates
            if not ambiguity.is_blocking:
                result.warnings.append(ambiguity.question)

        self._populate_result(result, best_explore_name, explore_field_map, best_score)

        if len(sorted_explores) >= 2:
            second_name, second_score = sorted_explores[1]
            if best_score - second_score < settings.ambiguity_threshold:
                result.alternatives.append({
                    "explore": second_name,
                    "score": second_score,
                })

        self._check_fanout_warnings(result)
        self._check_pdt_warnings(result)

        return result

    def _build_intent_string(self, intent: dict) -> str:
        """Build a search-optimized string from structured intent."""
        parts = []

        metrics = intent.get("metrics", [])
        if metrics:
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
        Query the Neo4j vector index for candidate fields, deduplicated.

        Fetches extra results since the same field exists per explore
        with identical embeddings. Deduplicates by (name, view_name).
        """
        raw_k = settings.top_k_fields * 3
        raw_candidates = []
        try:
            with self._driver.session() as session:
                result = session.run(
                    Q.ANN_SEARCH_FIELDS,
                    k=raw_k,
                    embedding=embedding,
                )
                for record in result:
                    raw_candidates.append({
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

        # Deduplicate: keep the highest-scoring copy of each (name, view_name)
        seen = {}
        for c in raw_candidates:
            key = (c["field_name"], c["view_name"])
            if key not in seen or c["score"] > seen[key]["score"]:
                seen[key] = c

        candidates = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
        candidates = candidates[:settings.top_k_fields]

        logger.info(
            "ANN search: %d raw → %d unique → %d top-k candidates",
            len(raw_candidates), len(seen), len(candidates),
        )
        return candidates

    def _fulltext_search_fields(self, intent: dict, user_query: str) -> List[dict]:
        """
        Search the fulltext index for exact keyword matches.

        Complements vector search: when user says "revenue", fulltext
        finds fields literally named "revenue" or with "revenue" in
        their label/description. Vector search might rank "net_profit"
        higher if its description mentions "revenue".
        """
        # Build search terms from intent metrics + dimensions
        terms = []
        for m in intent.get("metrics", []):
            terms.extend(m.replace("_", " ").split())
        for d in intent.get("dimensions", []):
            terms.extend(d.replace("_", " ").split())

        if not terms:
            return []

        # Lucene query: OR across all terms
        query = " OR ".join(terms)

        candidates = []
        try:
            with self._driver.session() as session:
                result = session.run(
                    Q.FULLTEXT_SEARCH_FIELDS,
                    query=query,
                    k=settings.top_k_fields,
                )
                for record in result:
                    # Normalize fulltext score to 0-1 range (Lucene scores can be >1)
                    raw_score = record["score"]
                    normalized_score = min(raw_score / 10.0, 1.0)
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
                        "score": normalized_score,
                    })
        except Exception as exc:
            # Fulltext index may not exist yet — not fatal
            logger.warning("Fulltext search failed (may not be indexed yet): %s", exc)

        logger.info("Fulltext search for '%s': %d candidates", query, len(candidates))
        return candidates

    def _merge_candidates(
        self,
        vector_candidates: List[dict],
        fulltext_candidates: List[dict],
    ) -> List[dict]:
        """
        Merge vector and fulltext results, keeping the best score per field.

        If a field appears in both, take the higher score.
        Deduplicate by (field_name, view_name) across both sources.
        """
        seen = {}
        for c in vector_candidates:
            key = (c["field_name"], c["view_name"])
            if key not in seen or c["score"] > seen[key]["score"]:
                seen[key] = c

        for c in fulltext_candidates:
            key = (c["field_name"], c["view_name"])
            if key not in seen or c["score"] > seen[key]["score"]:
                seen[key] = c

        merged = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
        merged = merged[:settings.top_k_fields]

        logger.info(
            "Merged candidates: %d vector + %d fulltext → %d unique → %d top-k",
            len(vector_candidates), len(fulltext_candidates), len(seen), len(merged),
        )
        return merged

    def _group_by_explore(
        self,
        candidates: List[dict],
        intent: dict,
    ) -> Dict[str, List[dict]]:
        """Group candidate fields by explore, using the cache's reverse index."""
        explore_field_map: Dict[str, List[dict]] = {}

        hidden_explores = set()
        for name in self._cache.all_explore_names():
            ctx = self._cache.get_explore(name)
            if ctx and ctx.get("explore", {}).get("is_hidden", False):
                hidden_explores.add(name)

        valid_explores = set(self._cache.all_explore_names()) - hidden_explores

        for candidate in candidates:
            field_id = f"{candidate['view_name']}.{candidate['field_name']}"
            explores = self._cache.get_explores_for_field(field_id)

            for explore_name in explores:
                if explore_name in hidden_explores or explore_name not in valid_explores:
                    continue

                if explore_name not in explore_field_map:
                    explore_field_map[explore_name] = []
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
        """Score each explore by how well it covers the requested fields."""
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

            top_n = min(requested_count, len(field_scores))
            avg_top_score = sum(field_scores[:top_n]) / top_n

            blended = 0.6 * best_score + 0.4 * avg_top_score

            coverage_ratio = min(len(matched_fields) / requested_count, 1.0)
            coverage_bonus = 0.1 * coverage_ratio

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

    def _apply_explicit_explore_boost(
        self,
        scores: Dict[str, float],
        user_query: str,
    ) -> Dict[str, float]:
        """Boost an explore's score if the user explicitly named it in the query."""
        query_lower = user_query.lower()

        for explore_name in scores:
            name_lower = explore_name.lower()
            patterns = [
                rf"\bin\s+{re.escape(name_lower)}\b",
                rf"\bfrom\s+{re.escape(name_lower)}\b",
                rf"\busing\s+{re.escape(name_lower)}\b",
                rf"\b{re.escape(name_lower)}\s+explore\b",
                rf"\b{re.escape(name_lower)}\s+data\b",
            ]
            if any(re.search(p, query_lower) for p in patterns):
                old_score = scores[explore_name]
                scores[explore_name] = min(old_score + 0.2, 1.0)
                logger.info(
                    "Boosted explore '%s' (%.3f → %.3f) — user explicitly named it",
                    explore_name, old_score, scores[explore_name],
                )
                break

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
        """Warn if measures are used alongside a one-to-many join."""
        measure_fields = [f for f in result.selected_fields if f.field_type == "measure"]
        if not measure_fields:
            return

        otm_views = set()
        for join in result.joins_needed:
            if join.get("relationship") == "one_to_many":
                otm_views.add(join.get("view_name"))

        if not otm_views:
            return

        selected_views = {f.view_name for f in result.selected_fields}
        active_otm = otm_views & selected_views

        if not active_otm:
            return

        otm_view_str = ", ".join(sorted(active_otm))
        for mf in measure_fields:
            if mf.view_name in active_otm:
                result.warnings.append(
                    f"Warning: '{mf.label}' is aggregated across a one-to-many join "
                    f"with '{otm_view_str}'. This may cause double-counting."
                )
            else:
                result.warnings.append(
                    f"Warning: '{mf.label}' may be inflated because fields from "
                    f"'{otm_view_str}' (one-to-many) are also selected."
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
                break
