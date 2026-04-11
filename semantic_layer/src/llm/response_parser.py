"""
LLM response parser and validator for the GraphRAG Semantic Layer.

WHAT: Functions that take raw LLM text output and return validated Python
      dicts. Each parser knows the expected schema for its use-case
      (intent extraction, query generation, etc.) and raises clear errors
      when the response does not match.

WHY:  LLMs are not compilers. Even with JSON mode enabled, responses may:
      - Be wrapped in markdown code fences (```json ... ```)
      - Contain trailing commas or comments (rare but possible)
      - Be missing required keys
      - Have keys with wrong types (e.g. a string where a list is expected)

      By centralising parsing here, every call-site gets consistent error
      handling. The alternative — scattering json.loads() and key checks
      across the codebase — leads to cryptic KeyError tracebacks that are
      hard to debug.

WHO CALLS THIS:
    - IntentExtractor  → parse_intent()
    - QueryGenerator   → parse_query()
    - Any module that needs safe JSON extraction → parse_json_safe()
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Safe JSON extraction (used by the other parsers and available standalone)
# ---------------------------------------------------------------------------

def parse_json_safe(raw: str) -> Optional[dict]:
    """
    Best-effort extraction of a JSON object from LLM output.

    LLMs frequently return JSON wrapped in explanatory text or markdown
    fences. This function tries multiple strategies to find the JSON:

    1. Direct parse (the happy path — raw string is pure JSON).
    2. Strip markdown code fences (```json ... ```).
    3. Find the outermost { ... } boundaries and parse that substring.

    Parameters
    ----------
    raw : str
        The raw text response from the LLM.

    Returns
    -------
    dict or None
        The parsed dict, or None if no valid JSON object can be found.
        We return None instead of raising so callers can provide a
        fallback or a friendlier error message.
    """
    if not raw or not raw.strip():
        return None

    text = raw.strip()

    # --- Strategy 1: direct parse ---
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    # --- Strategy 2: strip markdown code fences ---
    # Matches ```json, ```JSON, or just ``` at the start.
    fence_pattern = re.compile(
        r"^```(?:json|JSON)?\s*\n(.*?)\n\s*```\s*$",
        re.DOTALL,
    )
    fence_match = fence_pattern.search(text)
    if fence_match:
        try:
            parsed = json.loads(fence_match.group(1))
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    # --- Strategy 3: find outermost { ... } ---
    # This handles cases like "Here is the JSON:\n{...}\nHope that helps!"
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        candidate = text[first_brace : last_brace + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    # All strategies failed.
    logger.warning(
        "Could not extract JSON from LLM response. "
        "First 300 chars: %s",
        text[:300],
    )
    return None


# ---------------------------------------------------------------------------
# Intent parsing
# ---------------------------------------------------------------------------

# These are the keys we expect in every intent-extraction response.
# If any are missing, the LLM prompt probably needs adjustment — but
# we should not silently continue with incomplete data.
_INTENT_REQUIRED_KEYS = {
    "metrics",
    "dimensions",
    "filters",
    "intent_type",
}

# Valid values for intent_type. We check this to catch hallucinated types
# before they propagate downstream and cause confusing errors.
_VALID_INTENT_TYPES = {
    "aggregation",
    "trend",
    "comparison",
    "ranking",
}


def parse_intent(raw_response: str) -> dict:
    """
    Parse and validate an intent-extraction response from the LLM.

    Expected schema (all fields required unless noted):
    {
        "metrics":           ["metric_name", ...],
        "dimensions":        ["dimension_name", ...],
        "filters":           [{"field": "...", "operator": "...", "value": "..."}, ...],
        "time_range":        {"period": "...", "grain": "..."} | null,
        "intent_type":       "aggregation" | "trend" | "comparison" | "ranking",
        "attribution_hint":  "first_touch" | "last_touch" | "linear" | null
    }

    Parameters
    ----------
    raw_response : str
        The raw text from the LLM (may contain code fences or extra text).

    Returns
    -------
    dict
        The validated intent dict.

    Raises
    ------
    ValueError
        If the response cannot be parsed or required keys are missing.
    """
    parsed = parse_json_safe(raw_response)
    if parsed is None:
        raise ValueError(
            "Failed to parse intent-extraction response as JSON. "
            "This usually means the LLM ignored the JSON-output instruction. "
            f"Raw response (first 300 chars): {raw_response[:300]}"
        )

    # --- Check required keys ---
    missing = _INTENT_REQUIRED_KEYS - set(parsed.keys())
    if missing:
        raise ValueError(
            f"Intent response is missing required keys: {sorted(missing)}. "
            f"Keys present: {sorted(parsed.keys())}. "
            "The intent-extraction prompt may need adjustment."
        )

    # --- Validate intent_type ---
    intent_type = parsed.get("intent_type")
    if intent_type not in _VALID_INTENT_TYPES:
        logger.warning(
            "Unexpected intent_type '%s'. Expected one of %s. "
            "Defaulting to 'aggregation'.",
            intent_type,
            sorted(_VALID_INTENT_TYPES),
        )
        parsed["intent_type"] = "aggregation"

    # --- Validate list fields ---
    for key in ("metrics", "dimensions", "filters"):
        if not isinstance(parsed.get(key), list):
            raise ValueError(
                f"Intent field '{key}' should be a list, "
                f"got {type(parsed.get(key)).__name__}: {parsed.get(key)!r}"
            )

    # --- Provide defaults for optional keys ---
    # We normalise here so downstream code never has to check "is this key
    # present?". Every consumer can assume the full schema exists.
    parsed.setdefault("time_range", None)
    parsed.setdefault("attribution_hint", None)

    return parsed


# ---------------------------------------------------------------------------
# Query parsing
# ---------------------------------------------------------------------------

# The query-generation LLM returns a Looker-compatible query spec.
_QUERY_REQUIRED_KEYS = {
    "model",
    "view",
    "fields",
}


def parse_query(raw_response: str) -> dict:
    """
    Parse and validate a query-generation response from the LLM.

    Expected schema (required keys):
    {
        "model":           "model_name",
        "view":            "explore_name",
        "fields":          ["view.field_name", ...],
        "filters":         {"view.field": "value", ...},
        "sorts":           ["view.field desc", ...],
        "limit":           500,
        "having_filters":  [{"dimension_name": "...", "expression": "..."}]
    }

    Parameters
    ----------
    raw_response : str
        The raw text from the LLM.

    Returns
    -------
    dict
        The validated query dict.

    Raises
    ------
    ValueError
        If parsing fails or required keys are missing.
    """
    parsed = parse_json_safe(raw_response)
    if parsed is None:
        raise ValueError(
            "Failed to parse query-generation response as JSON. "
            f"Raw response (first 300 chars): {raw_response[:300]}"
        )

    # --- Check required keys ---
    missing = _QUERY_REQUIRED_KEYS - set(parsed.keys())
    if missing:
        raise ValueError(
            f"Query response is missing required keys: {sorted(missing)}. "
            f"Keys present: {sorted(parsed.keys())}. "
            "The query-generation prompt may need adjustment."
        )

    # --- Validate fields is a non-empty list ---
    fields = parsed.get("fields")
    if not isinstance(fields, list) or len(fields) == 0:
        raise ValueError(
            f"Query 'fields' must be a non-empty list, "
            f"got: {fields!r}"
        )

    # --- Validate field name format ---
    # Looker fields must be prefixed with the view name: "view_name.field_name".
    # We warn (not error) on unprefixed fields because the query engine can
    # sometimes fix them.
    for field in fields:
        if isinstance(field, str) and "." not in field:
            logger.warning(
                "Field '%s' is missing a view prefix (expected 'view.field'). "
                "The query engine may need to fix this.",
                field,
            )

    # --- Provide defaults for optional keys ---
    parsed.setdefault("filters", {})
    parsed.setdefault("sorts", [])
    parsed.setdefault("limit", 500)
    parsed.setdefault("having_filters", [])

    return parsed
