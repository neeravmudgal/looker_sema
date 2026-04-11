"""
Extracts structured intent from natural language user queries.

WHY: Raw user queries like "Show me revenue by country last quarter" contain
     implicit structure: a metric (revenue), a dimension (country), and a
     time filter (last quarter). We need to extract this structure so the
     retriever can search for the right fields, not just do a fuzzy text match.

CALLED BY: turn_handler.py — first step in processing any new user query.
CALLS: llm/provider.py for the LLM call, llm/prompts/intent_extraction.txt

GENERIC DESIGN: The extraction prompt includes available explore names and
tags from the actual loaded model, not hardcoded values.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from string import Template
from typing import Dict, List, Optional

from src.llm.provider import LLMProvider
from src.llm.response_parser import parse_intent, parse_json_safe

logger = logging.getLogger(__name__)

# Load prompt template once at module level
_PROMPT_DIR = Path(__file__).resolve().parent.parent / "llm" / "prompts"


def _load_prompt(name: str) -> str:
    """Load a prompt template from the prompts directory."""
    path = _PROMPT_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    logger.warning("Prompt file not found: %s, using fallback", path)
    return ""


class IntentExtractor:
    """
    Converts a raw natural language query into a structured intent dict.

    The intent dict has this shape:
    {
        "metrics": ["revenue"],          # What to measure
        "dimensions": ["country"],       # What to group by
        "filters": [],                   # Explicit filter conditions
        "time_range": {
            "period": "last_quarter",    # Relative time period
            "grain": "quarter"           # Time granularity
        },
        "intent_type": "aggregation",    # aggregation|trend|comparison|ranking
        "attribution_hint": null         # first_touch|last_touch|linear|null
    }
    """

    def __init__(self, llm: LLMProvider):
        self._llm = llm
        self._prompt_template = _load_prompt("intent_extraction.txt")

    def extract(
        self,
        user_query: str,
        explore_names: List[str],
        available_tags: List[str],
    ) -> dict:
        """
        Extract structured intent from a user query.

        Args:
            user_query:     The raw natural language question.
            explore_names:  List of explore names from the loaded model (for context).
            available_tags: List of unique tags from all fields (for context).

        Returns:
            Structured intent dict. On failure, returns a minimal fallback intent
            that treats the entire query as a single metric search term.
        """
        # ── Build the prompt ──────────────────────────────────────
        if self._prompt_template:
            prompt = Template(self._prompt_template).safe_substitute(
                explore_names=", ".join(explore_names),
                available_tags=", ".join(available_tags) if available_tags else "none",
                user_query=user_query,
            )
        else:
            # Fallback if prompt file is missing
            prompt = self._build_fallback_prompt(user_query, explore_names)

        system = (
            "You are a data analyst assistant. Extract structured intent from "
            "natural language queries about data. Return ONLY valid JSON."
        )

        # ── First attempt ─────────────────────────────────────────
        try:
            result = self._llm.complete_json(system, prompt)
            if result:
                return self._validate_intent(result)
        except Exception as exc:
            logger.warning("Intent extraction first attempt failed: %s", exc)

        # ── Retry with stricter prompt ────────────────────────────
        try:
            strict_prompt = (
                prompt + "\n\nYour previous response was not valid JSON. "
                "Return ONLY a JSON object with keys: metrics, dimensions, "
                "filters, time_range, intent_type, attribution_hint."
            )
            result = self._llm.complete_json(system, strict_prompt)
            if result:
                return self._validate_intent(result)
        except Exception as exc:
            logger.warning("Intent extraction retry failed: %s", exc)

        # ── Fallback: treat entire query as a metric search term ──
        logger.warning(
            "Intent extraction failed for query '%s', using fallback", user_query
        )
        return self._fallback_intent(user_query)

    def _validate_intent(self, raw: dict) -> dict:
        """Ensure all required keys exist with correct types."""
        return {
            "metrics": raw.get("metrics", []) or [],
            "dimensions": raw.get("dimensions", []) or [],
            "filters": raw.get("filters", []) or [],
            "time_range": raw.get("time_range") or {"period": "", "grain": ""},
            "intent_type": raw.get("intent_type", "aggregation") or "aggregation",
            "attribution_hint": raw.get("attribution_hint") or None,
        }

    def _fallback_intent(self, query: str) -> dict:
        """Minimal intent when LLM extraction fails."""
        return {
            "metrics": [query],
            "dimensions": [],
            "filters": [],
            "time_range": {"period": "", "grain": ""},
            "intent_type": "aggregation",
            "attribution_hint": None,
        }

    def _build_fallback_prompt(
        self, query: str, explore_names: List[str]
    ) -> str:
        """Build a basic prompt if the template file is missing."""
        return (
            f"Extract the structured intent from this data analytics question:\n\n"
            f'"{query}"\n\n'
            f"Available data explores: {', '.join(explore_names)}\n\n"
            f"Return JSON with keys: metrics (list), dimensions (list), "
            f"filters (list), time_range (dict with period and grain), "
            f"intent_type (string: aggregation|trend|comparison|ranking), "
            f"attribution_hint (string: first_touch|last_touch|linear|null)\n\n"
            f"Attribution hint signals:\n"
            f"  'acquired', 'first visit', 'brought to site' → first_touch\n"
            f"  'converted', 'last click', 'before purchase' → last_touch\n"
            f"  'all touchpoints', 'equally' → linear\n"
            f"  No signal → null"
        )
