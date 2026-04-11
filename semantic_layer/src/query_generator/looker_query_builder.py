"""
Assembles Looker Explore query JSON using LLM generation.

DESIGN:
  The retrieval pipeline picks the EXPLORE (which data perspective).
  The context assembler gives the LLM ALL fields in that explore.
  The LLM picks the FIELDS and builds the full query.
  We validate the LLM output against the explore's field catalog.
  If the LLM fails, we fall back to direct assembly from retrieval results.

WHY LLM FOR QUERY GENERATION:
  The LLM understands nuance that retrieval can't capture:
  - "top 5 channels" → needs limit=5 + sort desc
  - "revenue trend by month" → needs a month grain field + sort by date asc
  - "users who spent > $500" → needs a having_filter on a measure
  These decisions require understanding intent, not just field matching.

CALLED BY: turn_handler.py
CALLS: LLM provider for query generation, cache for validation
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from string import Template
from typing import Dict, List, Optional

from src.graph.cache import ExploreContextCache
from src.llm.provider import LLMProvider
from src.retrieval.retriever import RetrievalResult

logger = logging.getLogger(__name__)

_PROMPT_DIR = Path(__file__).resolve().parent.parent / "llm" / "prompts"


def _load_prompt(name: str) -> str:
    path = _PROMPT_DIR / name
    return path.read_text(encoding="utf-8") if path.exists() else ""


# ── Time filter mapping ───────────────────────────────────────────────
LOOKER_TIME_FILTERS = {
    "last_quarter": "last 1 quarter",
    "this_quarter": "this quarter",
    "last_month": "last 1 month",
    "this_month": "this month",
    "last_week": "last 1 week",
    "this_week": "this week",
    "last_year": "last 1 year",
    "this_year": "this year",
    "ytd": "this year",
    "year_to_date": "this year",
    "last_30_days": "30 days",
    "last_7_days": "7 days",
    "last_90_days": "90 days",
    "today": "today",
    "yesterday": "yesterday",
}


class LookerQueryBuilder:
    """
    Builds Looker queries by giving the LLM full explore context.

    Flow:
    1. LLM gets: all fields in explore + retrieval hints + user intent
    2. LLM generates: complete query JSON with fields, filters, sorts
    3. We validate: every field the LLM chose actually exists in the explore
    4. Fallback: if LLM fails, build directly from retrieval results
    """

    def __init__(self, cache: ExploreContextCache, llm: LLMProvider):
        self._cache = cache
        self._llm = llm
        self._prompt_template = _load_prompt("query_generation.txt")

    def build(
        self,
        result: RetrievalResult,
        intent: dict,
        context: dict,
        user_query: str,
    ) -> dict:
        """
        Build a Looker query. LLM-first, with direct assembly fallback.
        """
        warnings: List[str] = []

        # ── Try 1: LLM generates the query ───────────────────────
        query = self._generate_via_llm(context, user_query)

        if query:
            # Normalize keys (LLM might say "view" instead of "explore")
            query = self._normalize_keys(query, result, context)
            # Validate fields against the explore
            query, validation_warnings = self._validate_fields(query, result.explore_name)
            warnings.extend(validation_warnings)

            # If LLM query has fields, use it
            if query.get("fields"):
                query["_warnings"] = warnings
                query["_confidence"] = result.confidence_score
                return query
            else:
                logger.warning("LLM query had no valid fields after validation, falling back")
                warnings.append("LLM query fields were invalid, used direct assembly.")

        # ── Try 2: Direct assembly from retrieval results ─────────
        logger.info("Using direct assembly fallback")
        query = self._assemble_directly(result, intent, context)
        query["_warnings"] = warnings
        query["_confidence"] = result.confidence_score
        return query

    def _generate_via_llm(self, context: dict, user_query: str) -> Optional[dict]:
        """
        Send the full explore context to the LLM and get a query back.

        The prompt includes:
        - All fields in the explore (the full menu)
        - Retrieval hints (★ marked fields)
        - The user's intent
        - Time filter value (pre-resolved)
        - Always filters
        """
        if not self._prompt_template:
            return None

        try:
            prompt = Template(self._prompt_template).safe_substitute(
                explore_name=context.get("explore_name", ""),
                model_name=context.get("model_name", ""),
                base_view=context.get("base_view", ""),
                sql_table=context.get("sql_table", ""),
                joins_formatted=context.get("joins_formatted", ""),
                fields_formatted=context.get("fields_formatted", ""),
                always_filters=context.get("always_filters", "none"),
                user_query=user_query,
                intent_json=context.get("intent_json", "{}"),
                time_filter_value=context.get("time_filter_value", ""),
            )

            # Add the retrieval hint if available
            retrieval_hint = context.get("retrieval_hint", "")
            if retrieval_hint:
                prompt += f"\n\n## Retrieval Hint\n{retrieval_hint}"

            system = (
                "You are a LookML query expert. Generate a Looker Explore query JSON. "
                "Use ONLY fields from the Available Fields list — use their exact "
                "view_name.field_name format. Return ONLY valid JSON."
            )

            response = self._llm.complete_json(system, prompt)
            if response and isinstance(response, dict):
                logger.info("LLM generated query with %d fields", len(response.get("fields", [])))
                return response

        except Exception as exc:
            logger.warning("LLM query generation failed: %s", exc)

        return None

    def _normalize_keys(self, query: dict, result: RetrievalResult, context: dict) -> dict:
        """
        Fix common LLM output inconsistencies:
        - "view" → "explore"
        - Wrong model name
        - having_filters as list → dict
        """
        # Fix explore key
        if "explore" not in query or not query.get("explore"):
            query["explore"] = query.pop("view", "") or result.explore_name or ""
        query.pop("view", None)  # Remove "view" if it still exists alongside "explore"

        # Ensure explore is valid
        valid_explores = set(self._cache.all_explore_names())
        if query["explore"] not in valid_explores:
            query["explore"] = result.explore_name or ""

        # Fix model
        expected_model = context.get("model_name", "") or result.model_name or ""
        if expected_model:
            query["model"] = expected_model

        # Ensure required keys
        query.setdefault("fields", [])
        query.setdefault("filters", {})
        query.setdefault("having_filters", {})
        query.setdefault("sorts", [])
        query.setdefault("limit", 500)

        # Normalize having_filters if it's a list
        hf = query.get("having_filters")
        if isinstance(hf, list):
            new_hf = {}
            for item in hf:
                if isinstance(item, dict):
                    dim = item.get("dimension_name", item.get("field", ""))
                    expr = item.get("expression", item.get("value", ""))
                    if dim and expr:
                        new_hf[dim] = expr
            query["having_filters"] = new_hf

        return query

    def _validate_fields(
        self, query: dict, explore_name: str
    ) -> tuple:
        """
        Validate every field in the LLM's query against the explore's catalog.

        Returns (cleaned_query, warnings_list).
        Fields not found in the explore are removed with a warning.
        """
        warnings = []

        if not explore_name:
            return query, warnings

        ctx = self._cache.get_explore(explore_name)
        if not ctx:
            warnings.append(f"Explore '{explore_name}' not found in cache.")
            return query, warnings

        # Build the set of all valid FQNs in this explore
        valid_fqns = set()
        for f in ctx.get("fields", []):
            if not f.is_hidden and f.field_type != "dimension_group":
                valid_fqns.add(f"{f.view_name}.{f.name}")

        # Validate query fields
        valid_query_fields = []
        for fqn in query.get("fields", []):
            if fqn in valid_fqns:
                valid_query_fields.append(fqn)
            else:
                logger.warning("Removed LLM field '%s' — not in explore '%s'", fqn, explore_name)
                warnings.append(f"Removed '{fqn}' — not found in explore.")
        query["fields"] = valid_query_fields

        # Validate filter targets
        for key in ("filters", "having_filters"):
            valid_filters = {}
            for fqn, val in query.get(key, {}).items():
                if fqn in valid_fqns:
                    valid_filters[fqn] = val
                else:
                    logger.warning("Removed filter on '%s'", fqn)
            query[key] = valid_filters

        # Validate sorts
        valid_sorts = []
        for sort_expr in query.get("sorts", []):
            fqn = sort_expr.strip().split()[0]
            if fqn in valid_fqns:
                valid_sorts.append(sort_expr)
        query["sorts"] = valid_sorts

        # Enforce limit
        limit = query.get("limit", 500)
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = 500
        query["limit"] = max(1, min(limit, 5000))

        return query, warnings

    def _assemble_directly(
        self, result: RetrievalResult, intent: dict, context: dict
    ) -> dict:
        """
        Fallback: build query directly from retrieval results (no LLM).

        Uses the retriever's selected fields, adds time filters and sorting.
        Always produces a valid query (fields are from the cache).
        """
        query = {
            "explore": result.explore_name or "",
            "model": result.model_name or "",
            "fields": [],
            "filters": {},
            "having_filters": {},
            "sorts": [],
            "limit": 500,
            "explanation": "",
        }

        # Add retrieved fields (guaranteed valid — came from the cache)
        for field in result.selected_fields:
            if field.field_type == "dimension_group":
                continue
            fqn = f"{field.view_name}.{field.name}"
            if fqn not in query["fields"]:
                query["fields"].append(fqn)

        # Add time filter
        time_range = intent.get("time_range", {})
        period = time_range.get("period", "")
        if period:
            normalized = period.lower().strip().replace(" ", "_").replace("-", "_")
            time_value = LOOKER_TIME_FILTERS.get(normalized, period)
            if time_value:
                date_field = self._find_date_field(result)
                if date_field:
                    query["filters"][date_field] = time_value

        # Sort by first measure descending
        measures = [f for f in result.selected_fields if f.field_type == "measure"]
        if measures:
            fqn = f"{measures[0].view_name}.{measures[0].name}"
            query["sorts"].append(f"{fqn} desc")

        # Inject always_filters
        af_str = context.get("always_filters", "none")
        if af_str and af_str != "none":
            try:
                af = json.loads(af_str)
                if isinstance(af, dict):
                    for k, v in af.items():
                        query["filters"].setdefault(k, v)
            except (json.JSONDecodeError, TypeError):
                pass

        # Fallback explanation
        field_names = query["fields"][:5]
        query["explanation"] = (
            f"This query uses the **{result.explore_name}** explore. "
            f"Fields: {', '.join(field_names)}."
        )

        return query

    def _find_date_field(self, result: RetrievalResult) -> str:
        """Find a date field for time filtering."""
        for f in result.selected_fields:
            if f.data_type in ("date", "time"):
                return f"{f.view_name}.{f.name}"

        ctx = self._cache.get_explore(result.explore_name) if result.explore_name else None
        if not ctx:
            return ""

        base_view = ctx.get("base_view", "")
        for f in ctx.get("fields", []):
            if f.view_name == base_view and f.name.endswith("_date") and f.data_type == "date":
                return f"{f.view_name}.{f.name}"

        for f in ctx.get("fields", []):
            if f.data_type == "date":
                return f"{f.view_name}.{f.name}"

        return ""
