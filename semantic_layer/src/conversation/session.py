"""
Multi-turn conversation state machine.

WHY: A single user question can take 2-3 turns to resolve:
     Turn 1: User asks "revenue by channel" → system detects ambiguity
     Turn 2: System asks "which attribution model?" → user picks "first touch"
     Turn 3: System generates query using first-touch field

     We need to store the partial retrieval result between turns so we don't
     re-run the full pipeline after the user answers a clarification.

CALLED BY: turn_handler.py manages the session.
CALLS: Nothing external — pure state management.

STATE MACHINE:
    IDLE                        → normal query processing
    WAITING_FOR_CLARIFICATION   → ambiguity detected, waiting for user choice
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from src.retrieval.retriever import RetrievalResult

logger = logging.getLogger(__name__)


@dataclass
class ConversationTurn:
    """One message in the conversation (either user or assistant)."""

    role: str              # "user" | "assistant"
    content: str           # The message text
    turn_type: str         # "query" | "clarification" | "answer" | "error" | "no_match"
    generated_query: Optional[dict] = None
    explanation: Optional[str] = None
    warnings: List[str] = dataclass_field(default_factory=list)
    confidence: Optional[float] = None
    explore_used: Optional[str] = None
    fields_used: Optional[List[str]] = None
    stages: Optional[list] = None          # List of StageInfo dicts for timing display
    total_duration_ms: float = 0.0
    timestamp: datetime = dataclass_field(default_factory=datetime.now)


class ConversationSession:
    """
    Manages the state of one conversation between a user and the system.

    Key behaviors:
    - Stores conversation history (all turns)
    - Implements a state machine: IDLE ↔ WAITING_FOR_CLARIFICATION
    - Holds partial retrieval results during clarification so we don't
      re-run the full pipeline after the user responds
    - Exports conversation as JSON for debugging/logging
    """

    def __init__(self):
        self.session_id: str = str(uuid4())
        self.turns: List[ConversationTurn] = []
        self.state: str = "IDLE"
        self.pending_retrieval: Optional[RetrievalResult] = None
        self.pending_intent: Optional[dict] = None
        self.pending_user_query: Optional[str] = None
        self._token_count: Dict[str, int] = {"input": 0, "output": 0}

    def add_turn(self, turn: ConversationTurn) -> None:
        """Add a new turn to the conversation history."""
        self.turns.append(turn)

    def get_history(self) -> List[ConversationTurn]:
        """Return all turns in chronological order."""
        return list(self.turns)

    def set_pending(
        self,
        retrieval: RetrievalResult,
        intent: dict,
        user_query: str,
    ) -> None:
        """
        Store a partial retrieval result while waiting for clarification.

        This is called when ambiguity is detected. The retrieval result
        contains all the candidate fields — we just need the user's
        choice to pick the right ones.
        """
        self.state = "WAITING_FOR_CLARIFICATION"
        self.pending_retrieval = retrieval
        self.pending_intent = intent
        self.pending_user_query = user_query
        logger.debug("Session %s: set pending (ambiguity: %s)", self.session_id, retrieval.ambiguity_type)

    def resolve_pending(self, user_choice: str) -> tuple:
        """
        Apply the user's clarification choice to the pending retrieval.

        Parses the user's choice (A/B/C or matching text) and filters
        the pending candidates accordingly.

        Returns:
            (retrieval_result, intent, original_user_query) tuple
        """
        if not self.pending_retrieval or not self.pending_intent:
            raise ValueError("No pending clarification to resolve")

        result = self.pending_retrieval
        intent = self.pending_intent
        user_query = self.pending_user_query or ""

        # Parse choice: accept A/B/C, "a", "first", option text, etc.
        choice_normalized = user_choice.strip().lower()

        if result.ambiguity_type == "attribution":
            intent = self._resolve_attribution_choice(choice_normalized, intent, result)
        elif result.ambiguity_type == "field_collision":
            result = self._resolve_field_collision_choice(choice_normalized, result)
        elif result.ambiguity_type == "explore_conflict":
            result = self._resolve_explore_conflict_choice(choice_normalized, result)

        # Clear ambiguity flags
        result.needs_clarification = False
        result.ambiguity_type = None
        result.clarification_question = ""
        result.clarification_options = []

        # Reset state
        self.clear_pending()

        return result, intent, user_query

    def _resolve_attribution_choice(
        self, choice: str, intent: dict, result: RetrievalResult
    ) -> dict:
        """Map user's A/B/C choice to attribution_hint in the intent."""
        if choice.startswith("a") or "first" in choice:
            intent["attribution_hint"] = "first_touch"
        elif choice.startswith("b") or "last" in choice:
            intent["attribution_hint"] = "last_touch"
        elif choice.startswith("c") or "linear" in choice or "multi" in choice:
            intent["attribution_hint"] = "linear"
        else:
            # Default to first touch if we can't parse the choice
            intent["attribution_hint"] = "first_touch"
            logger.warning("Could not parse attribution choice '%s', defaulting to first_touch", choice)

        return intent

    def _resolve_field_collision_choice(
        self, choice: str, result: RetrievalResult
    ) -> RetrievalResult:
        """Keep only the fields from the user's chosen view."""
        candidates = result.field_candidates if hasattr(result, 'field_candidates') else {}

        # Determine which option the user chose
        view_names = list(candidates.keys()) if candidates else []
        chosen_idx = 0
        if choice.startswith("b") and len(view_names) > 1:
            chosen_idx = 1
        elif choice.startswith("c") and len(view_names) > 2:
            chosen_idx = 2

        # Filter selected_fields to only include the chosen view's version
        if view_names and chosen_idx < len(view_names):
            chosen_view = view_names[chosen_idx]
            result.selected_fields = [
                f for f in result.selected_fields
                if f.view_name == chosen_view
                or f.name not in {f2.name for f2 in result.selected_fields if f2.view_name != chosen_view}
            ]

        return result

    def _resolve_explore_conflict_choice(
        self, choice: str, result: RetrievalResult
    ) -> RetrievalResult:
        """Switch to the user's chosen explore."""
        if choice.startswith("b") and result.alternatives:
            # Switch to the alternative explore
            alt = result.alternatives[0]
            result.explore_name = alt.get("explore", result.explore_name)
            result.confidence_score = alt.get("score", result.confidence_score)

        return result

    def clear_pending(self) -> None:
        """Reset all pending state."""
        self.state = "IDLE"
        self.pending_retrieval = None
        self.pending_intent = None
        self.pending_user_query = None

    def add_tokens(self, input_tokens: int, output_tokens: int) -> None:
        """Track token usage across the session."""
        self._token_count["input"] += input_tokens
        self._token_count["output"] += output_tokens

    @property
    def token_count(self) -> Dict[str, int]:
        return dict(self._token_count)

    @property
    def turn_count(self) -> int:
        return len(self.turns)

    def export_json(self) -> str:
        """Export the full conversation as JSON for debugging/logging."""
        data = {
            "session_id": self.session_id,
            "state": self.state,
            "turn_count": self.turn_count,
            "token_count": self.token_count,
            "turns": [
                {
                    "role": t.role,
                    "content": t.content,
                    "turn_type": t.turn_type,
                    "query": t.generated_query,
                    "explanation": t.explanation,
                    "warnings": t.warnings,
                    "confidence": t.confidence,
                    "explore_used": t.explore_used,
                    "timestamp": t.timestamp.isoformat(),
                }
                for t in self.turns
            ],
        }
        return json.dumps(data, indent=2, default=str)
