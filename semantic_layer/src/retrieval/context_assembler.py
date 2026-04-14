"""
Assembles the full context passed to the LLM for query generation.

WHY: The LLM needs to see ALL fields available in the selected explore so
     it can intelligently choose which ones answer the user's question.
     Sending only the ANN-matched fields is too narrow — the LLM might need
     a date field for filtering that wasn't in the ANN top-k.

DESIGN:
  Retrieval picks the EXPLORE (which data perspective to use).
  The LLM picks the FIELDS (which specific columns/measures to include).
  This module gives the LLM everything it needs to make that choice:
    - All fields in the explore, grouped by view, with exact FQN names
    - Join info (so the LLM understands the data relationships)
    - The user's intent (what they're trying to measure/slice)
    - Time filter value (pre-resolved to Looker syntax)

CALLED BY: turn_handler.py — after retrieval, before query generation.
CALLS: cache.py for explore metadata.
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

from src.graph.cache import ExploreContextCache
from src.retrieval.retriever import RetrievalResult

logger = logging.getLogger(__name__)

# ── Time filter mapping ───────────────────────────────────────────────
LOOKER_TIME_MAP = {
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


class ContextAssembler:
    """
    Builds the context dict that gets injected into the LLM query generation prompt.

    The key design choice: we send ALL fields in the selected explore,
    not just the ANN matches. The LLM is smart enough to pick the right
    fields — we just need to give it the full menu.
    """

    def __init__(self, cache: ExploreContextCache):
        self._cache = cache

    def assemble(self, result: RetrievalResult, intent: dict) -> dict:
        """
        Assemble the full context for the LLM.

        Returns a dict matching the $variables in query_generation.txt:
        {
            "explore_name", "model_name", "base_view",
            "joins_formatted", "fields_formatted", "always_filters",
            "intent_json", "time_filter_value", "explore_description",
            "retrieval_hint"
        }
        """
        if not result.explore_name:
            return self._empty_context()

        ctx = self._cache.get_explore(result.explore_name)
        if not ctx:
            return self._empty_context()

        explore_info = ctx.get("explore", {})
        all_fields = ctx.get("fields", [])
        all_joins = ctx.get("joins", [])

        # ── Format ALL joins in the explore ───────────────────────
        joins_formatted = self._format_joins(all_joins)

        # ── Format ALL fields in the explore, grouped by view ─────
        # This is the full "menu" the LLM picks from.
        # We also mark which fields the retriever thinks are most relevant.
        retrieval_field_ids = {
            f"{f.view_name}.{f.name}" for f in result.selected_fields
        }
        fields_formatted = self._format_all_fields(all_fields, retrieval_field_ids)

        # ── Retrieval hint: which fields the retriever recommends ─
        # This guides the LLM toward the most relevant fields without
        # restricting it to only those.
        retrieval_hint = self._format_retrieval_hint(result.selected_fields)

        # ── Always filters ────────────────────────────────────────
        always_filters = ctx.get("always_filter", {})
        always_filters_str = json.dumps(always_filters) if always_filters else "none"

        # ── Time filter resolution ────────────────────────────────
        time_range = intent.get("time_range", {})
        time_filter_value = self._resolve_time_filter(time_range)

        return {
            "explore_name": result.explore_name,
            "model_name": result.model_name or "",
            "base_view": ctx.get("base_view", ""),
            "sql_table": "",
            "joins_formatted": joins_formatted,
            "fields_formatted": fields_formatted,
            "always_filters": always_filters_str,
            "intent_json": json.dumps(intent, indent=2),
            "time_filter_value": time_filter_value,
            "explore_description": explore_info.get("description", ""),
            "retrieval_hint": retrieval_hint,
        }

    def _format_all_fields(
        self, fields: list, highlighted_ids: set
    ) -> str:
        """
        Format ALL fields in the explore, grouped by view.

        Fields that the retriever flagged as relevant are marked with ★
        so the LLM knows which ones are most likely needed.
        Each field shows its exact FQN (view_name.field_name) — the LLM
        must use this exact format in the query.
        """
        if not fields:
            return "No fields available."

        # Group by view
        views: Dict[str, list] = {}
        for f in fields:
            if f.is_hidden:
                continue
            if f.field_type == "dimension_group":
                continue  # Skip parent groups — expanded fields are listed
            views.setdefault(f.view_name, []).append(f)

        lines = []
        for view_name in sorted(views.keys()):
            view_fields = views[view_name]
            lines.append(f"\n  [{view_name}]")

            # Show dimensions first, then measures
            dims = sorted([f for f in view_fields if f.field_type == "dimension"], key=lambda x: x.name)
            measures = sorted([f for f in view_fields if f.field_type == "measure"], key=lambda x: x.name)
            others = sorted([f for f in view_fields if f.field_type not in ("dimension", "measure")], key=lambda x: x.name)

            for f in dims + measures + others:
                fqn = f"{f.view_name}.{f.name}"
                star = " ★" if fqn in highlighted_ids else ""
                desc = f" — {f.description[:80]}" if f.description else ""
                lines.append(f"    {fqn} ({f.field_type}, {f.data_type}){star}{desc}")

        return "\n".join(lines)

    def _format_retrieval_hint(self, selected_fields: list) -> str:
        """
        Format the retriever's top picks as a hint for the LLM.

        This tells the LLM: "Based on semantic and keyword search, these
        fields are most relevant to the user's question. Start with these,
        but you can use ANY field from the Available Fields list."
        """
        if not selected_fields:
            return "No specific field recommendations."

        lines = [
            "Retrieval recommends these fields (most relevant to the question).",
            "Start with these, but use ANY field from the Available Fields list if better.",
        ]
        # Group by type: measures first, then dimensions
        measures = [f for f in selected_fields if f.field_type == "measure"]
        dims = [f for f in selected_fields if f.field_type == "dimension"]

        if measures:
            lines.append("\nMeasures:")
            for f in measures[:8]:
                fqn = f"{f.view_name}.{f.name}"
                desc = f" — {f.description[:60]}" if f.description else ""
                lines.append(f"  ★ {fqn}{desc}")

        if dims:
            lines.append("\nDimensions:")
            for f in dims[:8]:
                fqn = f"{f.view_name}.{f.name}"
                desc = f" — {f.description[:60]}" if f.description else ""
                lines.append(f"  ★ {fqn}{desc}")

        return "\n".join(lines)

    def _format_joins(self, joins: List[dict]) -> str:
        """Format all joins in the explore."""
        if not joins:
            return "No joins (base view only)."

        lines = []
        for j in joins:
            pdt = " [derived table]" if j.get("is_pdt") else ""
            lines.append(
                f"  - {j['view_name']} ({j.get('relationship', 'many_to_one')}){pdt}"
            )
        return "\n".join(lines)

    def _resolve_time_filter(self, time_range: dict) -> str:
        """Convert extracted time range to Looker date filter syntax."""
        period = time_range.get("period", "")
        if not period:
            return ""
        normalized = period.lower().strip().replace(" ", "_").replace("-", "_")
        return LOOKER_TIME_MAP.get(normalized, period)

    def _empty_context(self) -> dict:
        return {
            "explore_name": "", "model_name": "", "base_view": "",
            "sql_table": "", "joins_formatted": "No joins.",
            "fields_formatted": "No fields found.",
            "always_filters": "none", "intent_json": "{}",
            "time_filter_value": "", "explore_description": "",
            "retrieval_hint": "",
        }
