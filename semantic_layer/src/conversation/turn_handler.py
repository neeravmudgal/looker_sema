"""
Main orchestration layer — routes each user message through the correct pipeline.

WHY: This is the single entry point that ties all services together. Instead of
     the Streamlit UI knowing about retrievers, embedders, and LLMs, it just calls
     turn_handler.handle_turn(message) and gets back a TurnResponse to render.

CALLED BY: streamlit_app.py — on every user message.
CALLS: IntentExtractor, Retriever, ContextAssembler, LookerQueryBuilder,
       QueryValidator, LLMProvider, ConversationSession.

ROUTING LOGIC:
  If session is WAITING_FOR_CLARIFICATION → resolve the ambiguity → generate query
  If new query → extract intent → retrieve → check ambiguity → generate query

ERROR HANDLING: The entire routing is wrapped in try/except.
Never propagate exceptions to Streamlit — return a friendly error TurnResponse.
"""

from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass, field as dataclass_field
from pathlib import Path
from string import Template
from typing import Callable, Dict, List, Optional

from neo4j import Driver

from src.config import settings
from src.graph.cache import ExploreContextCache
from src.embeddings.embedder import Embedder
from src.llm.provider import LLMProvider
from src.retrieval.intent_extractor import IntentExtractor
from src.retrieval.retriever import Retriever, RetrievalResult
from src.retrieval.context_assembler import ContextAssembler
from src.query_generator.looker_query_builder import LookerQueryBuilder
from src.conversation.session import ConversationSession, ConversationTurn

logger = logging.getLogger(__name__)

_PROMPT_DIR = Path(__file__).resolve().parent.parent / "llm" / "prompts"


def _load_prompt(name: str) -> str:
    path = _PROMPT_DIR / name
    return path.read_text(encoding="utf-8") if path.exists() else ""


@dataclass
class StageInfo:
    """Timing and status for one pipeline stage."""
    name: str
    status: str = "pending"    # "pending" | "running" | "done" | "skipped" | "error"
    duration_ms: float = 0.0
    detail: str = ""


@dataclass
class TurnResponse:
    """
    Everything the UI needs to render one assistant response.

    turn_type determines how the UI renders it:
      "answer"         → show query + explanation
      "clarification"  → show question + option buttons
      "no_match"       → show suggestions
      "error"          → show error message + expandable detail

    stages tracks timing for every pipeline step so the UI can show
    which stages ran and how long each took.
    """

    message: str = ""
    turn_type: str = "error"  # "clarification" | "answer" | "error" | "no_match"
    query: Optional[dict] = None
    explanation: Optional[str] = None
    warnings: List[str] = dataclass_field(default_factory=list)
    confidence: Optional[float] = None
    clarification_options: List[str] = dataclass_field(default_factory=list)
    explore_used: Optional[str] = None
    fields_used: Optional[List[str]] = None
    token_count: Optional[Dict[str, int]] = None
    error_detail: Optional[str] = None
    stages: List[StageInfo] = dataclass_field(default_factory=list)
    total_duration_ms: float = 0.0


# Type alias for the status callback the UI passes in
StatusCallback = Optional[Callable[[str], None]]


class TurnHandler:
    """
    Routes each user message to the correct action and returns a TurnResponse.

    Usage:
        handler = TurnHandler(driver, cache, embedder, llm)
        response = handler.handle_turn("show me revenue by country", session)

    Pass a status_callback to get real-time stage updates in the UI:
        def on_status(msg): st.write(msg)
        response = handler.handle_turn(msg, session, status_callback=on_status)
    """

    def __init__(
        self,
        driver: Driver,
        cache: ExploreContextCache,
        embedder: Embedder,
        llm: LLMProvider,
    ):
        self._driver = driver
        self._cache = cache
        self._embedder = embedder
        self._llm = llm
        self._intent_extractor = IntentExtractor(llm)
        self._retriever = Retriever(driver, cache, embedder)
        self._context_assembler = ContextAssembler(cache)
        self._query_builder = LookerQueryBuilder(cache, llm)

    def handle_turn(
        self,
        user_message: str,
        session: ConversationSession,
        status_callback: StatusCallback = None,
    ) -> TurnResponse:
        """
        Main entry point. Process a user message and return a response.

        Args:
            user_message: The user's text input.
            session: The current conversation session.
            status_callback: Optional callable that receives stage status strings
                           for real-time UI updates (e.g. "Extracting intent...")

        This method NEVER raises exceptions — all errors are caught and
        returned as friendly error messages in TurnResponse.
        """
        turn_start = time.time()

        # Record the user's message
        session.add_turn(ConversationTurn(
            role="user",
            content=user_message,
            turn_type="query",
        ))

        def _notify(msg: str):
            if status_callback:
                status_callback(msg)

        try:
            if session.state == "WAITING_FOR_CLARIFICATION":
                _notify("Resolving your choice...")
                response = self._handle_clarification_response(user_message, session, _notify)
            else:
                response = self._handle_new_query(user_message, session, _notify)

            # Record the assistant's response (including stage timings)
            session.add_turn(ConversationTurn(
                role="assistant",
                content=response.message,
                turn_type=response.turn_type,
                generated_query=response.query,
                explanation=response.explanation,
                warnings=response.warnings,
                confidence=response.confidence,
                explore_used=response.explore_used,
                fields_used=response.fields_used,
                stages=[
                    {"name": s.name, "status": s.status,
                     "duration_ms": s.duration_ms, "detail": s.detail}
                    for s in response.stages
                ],
                total_duration_ms=response.total_duration_ms,
            ))

            response.token_count = self._llm.get_token_summary()
            response.total_duration_ms = (time.time() - turn_start) * 1000

            return response

        except Exception as exc:
            error_detail = traceback.format_exc()
            logger.error("Turn handling failed: %s\n%s", exc, error_detail)

            error_response = TurnResponse(
                message=(
                    "I ran into an unexpected error processing your question. "
                    "Please try rephrasing or check the system status in the sidebar."
                ),
                turn_type="error",
                error_detail=error_detail,
                total_duration_ms=(time.time() - turn_start) * 1000,
            )

            session.add_turn(ConversationTurn(
                role="assistant",
                content=error_response.message,
                turn_type="error",
            ))

            return error_response

    def _handle_new_query(
        self,
        user_message: str,
        session: ConversationSession,
        notify: Callable[[str], None],
    ) -> TurnResponse:
        """
        Process a brand new query (not a clarification response).

        Each stage is timed and reported via the notify callback.
        """
        stages: List[StageInfo] = []

        # ── Stage 1: Extract intent ───────────────────────────────
        notify("Stage 1/4 — Extracting intent (LLM call)...")
        t0 = time.time()

        explore_names = self._cache.all_explore_names()
        available_tags = self._cache.get_all_tags()
        intent = self._intent_extractor.extract(user_message, explore_names, available_tags)

        stage1_ms = (time.time() - t0) * 1000
        stages.append(StageInfo(
            name="Intent Extraction",
            status="done",
            duration_ms=stage1_ms,
            detail=f"Metrics: {intent.get('metrics', [])}, Dims: {intent.get('dimensions', [])}, "
                   f"Attribution: {intent.get('attribution_hint', 'none')}",
        ))
        logger.info("Intent extracted in %.0fms: %s", stage1_ms, intent)

        # ── Stage 2: Retrieval (embed + ANN + graph) ─────────────
        notify("Stage 2/4 — Searching knowledge graph (embed + ANN + scoring)...")
        t0 = time.time()

        result = self._retriever.retrieve(intent, user_message)

        stage2_ms = (time.time() - t0) * 1000
        field_count = len(result.selected_fields)
        stages.append(StageInfo(
            name="Retrieval (Embed → ANN → Graph → Score)",
            status="done",
            duration_ms=stage2_ms,
            detail=f"Found {field_count} fields, "
                   f"Explore: {result.explore_name or 'none'}, "
                   f"Confidence: {result.confidence_score:.2f}",
        ))

        # ── Stage 3: Ambiguity check ─────────────────────────────
        notify("Stage 3/4 — Checking for ambiguities...")
        t0 = time.time()

        # Ambiguity is already checked inside retriever, just record timing
        stage3_ms = (time.time() - t0) * 1000
        if result.needs_clarification:
            stages.append(StageInfo(
                name="Ambiguity Detection",
                status="done",
                duration_ms=stage3_ms,
                detail=f"Ambiguity found: {result.ambiguity_type}",
            ))
            # Return clarification — don't proceed to query gen
            session.set_pending(result, intent, user_message)
            return TurnResponse(
                message=result.clarification_question,
                turn_type="clarification",
                clarification_options=result.clarification_options,
                warnings=result.warnings,
                stages=stages,
            )

        stages.append(StageInfo(
            name="Ambiguity Detection",
            status="done",
            duration_ms=stage3_ms,
            detail="No ambiguity detected" if field_count > 0 else "No fields found",
        ))

        # No match at all
        if not result.selected_fields:
            return TurnResponse(
                **self._build_no_match_data(result, user_message),
                stages=stages,
            )

        # ── Stage 4: Build query + explanation ────────────────────
        notify("Stage 4/4 — Building query + generating explanation...")
        t0 = time.time()

        context = self._context_assembler.assemble(result, intent)
        query = self._query_builder.build(result, intent, context, user_message)

        stage4_ms = (time.time() - t0) * 1000

        # Extract internal keys
        all_warnings = list(result.warnings)
        if query.get("_warnings"):
            all_warnings.extend(query.pop("_warnings"))
        query.pop("_confidence", None)

        explanation = query.pop("explanation", "") or self._build_fallback_explanation(result)

        stages.append(StageInfo(
            name="Query Build + Explanation",
            status="done",
            duration_ms=stage4_ms,
            detail=f"Fields: {len(query.get('fields', []))}, "
                   f"Filters: {len(query.get('filters', {}))}, "
                   f"Explore: {query.get('explore', '?')}",
        ))

        final_query = query
        fields_used = final_query.get("fields", [])

        return TurnResponse(
            message=explanation,
            turn_type="answer",
            query=final_query,
            explanation=explanation,
            warnings=all_warnings,
            confidence=result.confidence_score,
            explore_used=result.explore_name,
            fields_used=fields_used,
            stages=stages,
        )

    def _handle_clarification_response(
        self,
        user_message: str,
        session: ConversationSession,
        notify: Callable[[str], None],
    ) -> TurnResponse:
        """
        Process a user's response to a clarification question.

        Key insight: for attribution ambiguity, the user's choice tells us
        which specific field to use. We modify the intent with the hint,
        then re-run retrieval so the scoring picks the right explore that
        contains that field. We do NOT blindly re-run — the attribution_hint
        guides the retriever to prefer fields matching the chosen model.
        """
        stages: List[StageInfo] = []
        t0 = time.time()

        try:
            result, intent, original_query = session.resolve_pending(user_message)
        except ValueError as exc:
            logger.warning("Failed to resolve pending: %s", exc)
            session.clear_pending()
            return self._handle_new_query(user_message, session, notify)

        stages.append(StageInfo(
            name="Clarification Resolved",
            status="done",
            duration_ms=(time.time() - t0) * 1000,
            detail=f"Choice: {user_message[:50]}, Attribution: {intent.get('attribution_hint', 'none')}",
        ))

        # Re-run retrieval with the attribution hint so it picks the correct field
        notify("Re-running retrieval with your choice...")
        t0 = time.time()
        result = self._retriever.retrieve(intent, original_query)
        stages.append(StageInfo(
            name="Re-retrieval with hint",
            status="done",
            duration_ms=(time.time() - t0) * 1000,
            detail=f"Explore: {result.explore_name}, Fields: {len(result.selected_fields)}, "
                   f"Confidence: {result.confidence_score:.2f}",
        ))

        if not result.selected_fields:
            return TurnResponse(
                **self._build_no_match_data(result, original_query),
                stages=stages,
            )

        # Generate query
        notify("Building query + explanation...")
        t0 = time.time()
        context = self._context_assembler.assemble(result, intent)
        query = self._query_builder.build(result, intent, context, original_query)

        all_warnings = list(result.warnings)
        if query.get("_warnings"):
            all_warnings.extend(query.pop("_warnings"))
        query.pop("_confidence", None)
        explanation = query.pop("explanation", "") or self._build_fallback_explanation(result)
        final_query = query

        stages.append(StageInfo(
            name="Query Build + Explanation",
            status="done",
            duration_ms=(time.time() - t0) * 1000,
            detail=f"Fields: {len(final_query.get('fields', []))}, Explore: {final_query.get('explore', '?')}",
        ))

        fields_used = final_query.get("fields", [])

        return TurnResponse(
            message=explanation,
            turn_type="answer",
            query=final_query,
            explanation=explanation,
            warnings=all_warnings,
            confidence=result.confidence_score,
            explore_used=result.explore_name,
            fields_used=fields_used,
            stages=stages,
        )

    def _build_fallback_explanation(self, result: RetrievalResult) -> str:
        """Simple explanation when LLM doesn't provide one in the query JSON."""
        field_names = [f"{f.view_name}.{f.name}" for f in result.selected_fields]
        return (
            f"I used the **{result.explore_name}** explore to answer your question. "
            f"Fields: {', '.join(field_names[:5])}. "
            f"Confidence: {result.confidence_score:.0%}."
        )

    def _build_no_match_data(self, result: RetrievalResult, user_query: str) -> dict:
        """Build data dict for a 'no match' response."""
        suggestions = []
        for candidate in result._all_candidates[:3]:
            fqn = f"{candidate['view_name']}.{candidate['field_name']}"
            score = candidate.get('score', 0)
            desc = candidate.get('description', '')[:60]
            suggestions.append(f"  - **{fqn}** ({score:.0%}) — {desc}")

        if suggestions:
            msg = (
                "I couldn't find a confident match for your question. "
                "Here are the closest fields I found:\n\n"
                + "\n".join(suggestions)
                + "\n\nTry rephrasing with one of these field names, "
                "or add more context like 'by country' or 'last month'."
            )
        else:
            msg = (
                "I couldn't find any fields matching your question. "
                "Try browsing available explores in the sidebar to see "
                "what data is available."
            )

        return {
            "message": msg,
            "turn_type": "no_match",
            "confidence": result.confidence_score if result.confidence_score > 0 else None,
        }
