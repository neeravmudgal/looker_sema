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

import json
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
class LoopIteration:
    """
    One iteration of the multi-hop query generation loop.

    Tracks exactly what was sent to and received from the LLM at each step,
    plus any vector search results appended for Type 3 context requests.
    All data is exposed to the UI for full transparency.
    """
    iteration_number: int
    response_type: str  # "query" | "clarification" | "context_request"
    system_prompt: str = ""
    user_prompt: str = ""
    raw_response: str = ""
    parsed_response: dict = dataclass_field(default_factory=dict)
    additional_context: Optional[str] = None
    duration_ms: float = 0.0
    vector_search_query: Optional[str] = None
    vector_search_results: Optional[List[dict]] = None


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
    prompt_log: List[dict] = dataclass_field(default_factory=list)
    loop_iterations: List[dict] = dataclass_field(default_factory=list)


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

        # Clear prompt log so we capture only this turn's LLM calls
        self._llm.clear_prompt_log()

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

            # Record the assistant's response (including stage timings and full context)
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
                prompt_log=response.prompt_log,
                loop_iterations=response.loop_iterations if response.loop_iterations else None,
            ))

            response.token_count = self._llm.get_token_summary()
            response.prompt_log = self._llm.get_prompt_log()
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
        The notify callback receives structured dicts with stage info
        so the UI can show real-time elapsed time and data.
        """
        stages: List[StageInfo] = []

        def _stage_notify(stage_num: int, total: int, name: str, status: str = "running", **data):
            """Send structured stage update to the UI callback."""
            notify(json.dumps({
                "stage_num": stage_num,
                "total_stages": total,
                "name": name,
                "status": status,
                "timestamp": time.time(),
                **data,
            }))

        # ── Stage 1: Extract intent ───────────────────────────────
        _stage_notify(1, 4, "Intent Extraction", status="running")
        t0 = time.time()

        explore_names = self._cache.all_explore_names()
        available_tags = self._cache.get_all_tags()
        intent = self._intent_extractor.extract(user_message, explore_names, available_tags)

        stage1_ms = (time.time() - t0) * 1000
        intent_detail = (
            f"Metrics: {intent.get('metrics', [])}, "
            f"Dims: {intent.get('dimensions', [])}, "
            f"Type: {intent.get('intent_type', '?')}, "
            f"Attribution: {intent.get('attribution_hint', 'none')}"
        )
        stages.append(StageInfo(
            name="Intent Extraction",
            status="done",
            duration_ms=stage1_ms,
            detail=intent_detail,
        ))
        _stage_notify(1, 4, "Intent Extraction", status="done",
                      duration_ms=stage1_ms, detail=intent_detail,
                      data={"intent": intent})
        logger.info("Intent extracted in %.0fms: %s", stage1_ms, intent)

        # ── Stage 2: Retrieval (embed + ANN + graph) ─────────────
        _stage_notify(2, 4, "Retrieval", status="running")
        t0 = time.time()

        result = self._retriever.retrieve(intent, user_message)

        stage2_ms = (time.time() - t0) * 1000
        field_count = len(result.selected_fields)
        top_fields = [f"{f.view_name}.{f.name}" for f in result.selected_fields[:10]]
        explore_scores_summary = {k: round(v, 3) for k, v in sorted(
            result._explore_scores.items(), key=lambda x: x[1], reverse=True
        )}
        retrieval_detail = (
            f"Found {field_count} fields, "
            f"Explore: {result.explore_name or 'none'}, "
            f"Confidence: {result.confidence_score:.2f}"
        )
        stages.append(StageInfo(
            name="Retrieval (Embed → ANN → Graph → Score)",
            status="done",
            duration_ms=stage2_ms,
            detail=retrieval_detail,
        ))
        _stage_notify(2, 4, "Retrieval", status="done",
                      duration_ms=stage2_ms, detail=retrieval_detail,
                      data={
                          "explore_scores": explore_scores_summary,
                          "top_fields": top_fields,
                          "all_candidates_count": len(result._all_candidates),
                      })

        # ── Stage 3: Ambiguity check (programmatic, conditional) ──
        _stage_notify(3, 4, "Ambiguity Detection", status="running")
        t0 = time.time()

        if not settings.use_llm_ambiguity:
            # Legacy programmatic ambiguity detection
            stage3_ms = (time.time() - t0) * 1000
            if result.needs_clarification:
                amb_detail = f"Ambiguity found: {result.ambiguity_type}"
                stages.append(StageInfo(
                    name="Ambiguity Detection",
                    status="done",
                    duration_ms=stage3_ms,
                    detail=amb_detail,
                ))
                _stage_notify(3, 4, "Ambiguity Detection", status="done",
                              duration_ms=stage3_ms, detail=amb_detail,
                              data={"type": result.ambiguity_type,
                                    "options": result.clarification_options})
                session.set_pending(result, intent, user_message)
                return TurnResponse(
                    message=result.clarification_question,
                    turn_type="clarification",
                    clarification_options=result.clarification_options,
                    warnings=result.warnings,
                    stages=stages,
                )
            amb_detail = "No ambiguity detected" if field_count > 0 else "No fields found"
        else:
            # LLM-driven ambiguity — skip programmatic check, let the LLM decide
            stage3_ms = (time.time() - t0) * 1000
            amb_detail = "Skipped (LLM-driven ambiguity enabled)"

        stages.append(StageInfo(
            name="Ambiguity Detection",
            status="done",
            duration_ms=stage3_ms,
            detail=amb_detail,
        ))
        _stage_notify(3, 4, "Ambiguity Detection", status="done",
                      duration_ms=stage3_ms, detail=amb_detail)

        # No match at all
        if not result.selected_fields:
            return TurnResponse(
                **self._build_no_match_data(result, user_message),
                stages=stages,
            )

        # ── Stage 4: Multi-hop query generation loop ─────────────
        _stage_notify(4, 4, "Query Generation", status="running")
        t0 = time.time()

        context = self._context_assembler.assemble(result, intent)

        if settings.use_llm_ambiguity:
            # Multi-hop loop: LLM decides if it needs clarification or more context
            loop_response, iterations = self._run_generation_loop(
                result, intent, context, user_message, notify,
            )

            stage4_ms = (time.time() - t0) * 1000
            loop_outcome = loop_response.get("_loop_outcome", "query")

            # Convert iterations to serializable dicts for the UI
            iterations_data = []
            for it in iterations:
                iterations_data.append({
                    "iteration_number": it.iteration_number,
                    "response_type": it.response_type,
                    "system_prompt": it.system_prompt,
                    "user_prompt": it.user_prompt,
                    "raw_response": it.raw_response,
                    "parsed_response": it.parsed_response,
                    "additional_context": it.additional_context,
                    "duration_ms": it.duration_ms,
                    "vector_search_query": it.vector_search_query,
                    "vector_search_results": it.vector_search_results,
                })

            if loop_outcome == "clarification":
                # Type 2: LLM wants to ask the user a question
                question = loop_response.get("question", "Could you clarify your question?")
                options = loop_response.get("options", [])
                reason = loop_response.get("reason", "")

                stages.append(StageInfo(
                    name="Query Generation (Multi-Hop)",
                    status="done",
                    duration_ms=stage4_ms,
                    detail=f"LLM needs clarification ({len(iterations)} iterations). Reason: {reason[:80]}",
                ))

                # Store state for when user responds
                session.set_pending_clarification(
                    result=result,
                    intent=intent,
                    user_query=user_message,
                    loop_context=context,
                    iterations=iterations_data,
                )

                return TurnResponse(
                    message=question,
                    turn_type="clarification",
                    clarification_options=options,
                    warnings=list(result.warnings),
                    stages=stages,
                    loop_iterations=iterations_data,
                )

            # Type 1 or forced query: extract the query
            query = loop_response
            query.pop("_loop_outcome", None)
            query.pop("response_type", None)

            # Validate and normalize
            query = self._query_builder._normalize_keys(query, result, context)
            query, validation_warnings = self._query_builder._validate_fields(query, result.explore_name)

            all_warnings = list(result.warnings) + validation_warnings

            if not query.get("fields"):
                # LLM query failed validation — use direct assembly fallback
                logger.warning("Multi-hop query had no valid fields, falling back to direct assembly")
                all_warnings.append("LLM multi-hop query fields were invalid, used direct assembly.")
                query = self._query_builder._assemble_directly(result, intent, context)

            explanation = query.pop("explanation", "") or self._build_fallback_explanation(result)

            query_detail = (
                f"Fields: {len(query.get('fields', []))}, "
                f"Iterations: {len(iterations)}, "
                f"Explore: {query.get('explore', '?')}"
            )
            stages.append(StageInfo(
                name="Query Generation (Multi-Hop)",
                status="done",
                duration_ms=stage4_ms,
                detail=query_detail,
            ))
            _stage_notify(4, 4, "Query Generation", status="done",
                          duration_ms=stage4_ms, detail=query_detail,
                          data={"query_fields": query.get("fields", []),
                                "iterations": len(iterations)})

            query.pop("_warnings", None)
            query.pop("_confidence", None)

            return TurnResponse(
                message=explanation,
                turn_type="answer",
                query=query,
                explanation=explanation,
                warnings=all_warnings,
                confidence=result.confidence_score,
                explore_used=result.explore_name,
                fields_used=query.get("fields", []),
                stages=stages,
                loop_iterations=iterations_data,
            )

        else:
            # Legacy single-call path
            query = self._query_builder.build(result, intent, context, user_message)
            stage4_ms = (time.time() - t0) * 1000

            all_warnings = list(result.warnings)
            if query.get("_warnings"):
                all_warnings.extend(query.pop("_warnings"))
            query.pop("_confidence", None)

            explanation = query.pop("explanation", "") or self._build_fallback_explanation(result)

            query_detail = (
                f"Fields: {len(query.get('fields', []))}, "
                f"Filters: {len(query.get('filters', {}))}, "
                f"Explore: {query.get('explore', '?')}"
            )
            stages.append(StageInfo(
                name="Query Generation",
                status="done",
                duration_ms=stage4_ms,
                detail=query_detail,
            ))
            _stage_notify(4, 4, "Query Generation", status="done",
                          duration_ms=stage4_ms, detail=query_detail,
                          data={"query_fields": query.get("fields", [])})

            return TurnResponse(
                message=explanation,
                turn_type="answer",
                query=query,
                explanation=explanation,
                warnings=all_warnings,
                confidence=result.confidence_score,
                explore_used=result.explore_name,
                fields_used=query.get("fields", []),
                stages=stages,
            )

    def _run_generation_loop(
        self,
        result: RetrievalResult,
        intent: dict,
        context: dict,
        user_query: str,
        notify: Callable[[str], None],
    ) -> tuple:
        """
        Multi-hop query generation loop.

        Calls the LLM iteratively until it returns a final query (Type 1),
        asks for user clarification (Type 2), or exhausts the iteration limit.

        For Type 3 (context_request) responses, the loop performs a vector
        search for the requested concept, appends results to the context,
        and calls the LLM again.

        Args:
            result: RetrievalResult from the retrieval stage.
            intent: Structured intent dict.
            context: Assembled context dict from ContextAssembler.
            user_query: Original user question.
            notify: Callback for UI progress updates.

        Returns:
            Tuple of (response_dict, List[LoopIteration]).
            response_dict has a "_loop_outcome" key:
              - "query": response_dict is the Looker query
              - "clarification": response_dict has question/options/reason
              - "forced_query": max iterations hit, forced a query
        """
        max_iters = settings.max_loop_iterations
        iterations: List[LoopIteration] = []
        additional_context = ""

        for i in range(max_iters):
            iter_start = time.time()

            notify(json.dumps({
                "stage_num": 4,
                "total_stages": 4,
                "name": f"Query Generation (Iteration {i + 1})",
                "status": "running",
                "timestamp": time.time(),
                "iteration": i + 1,
            }))

            response = self._query_builder.generate_single_call(
                context, user_query, additional_context
            )

            iter_ms = (time.time() - iter_start) * 1000

            if not response:
                # LLM call failed entirely — break and use fallback
                iterations.append(LoopIteration(
                    iteration_number=i + 1,
                    response_type="error",
                    duration_ms=iter_ms,
                    parsed_response={},
                ))
                break

            response_type = response.get("response_type", "query")

            # Capture prompt log for this iteration
            prompt_log = self._llm.get_prompt_log()
            last_call = prompt_log[-1] if prompt_log else {}

            iteration = LoopIteration(
                iteration_number=i + 1,
                response_type=response_type,
                system_prompt=last_call.get("system_prompt", ""),
                user_prompt=last_call.get("user_prompt", ""),
                raw_response=last_call.get("raw_response", ""),
                parsed_response=response,
                additional_context=additional_context if additional_context else None,
                duration_ms=iter_ms,
            )

            if response_type == "query":
                iterations.append(iteration)
                notify(json.dumps({
                    "stage_num": 4, "total_stages": 4,
                    "name": f"Query Generation (Iteration {i + 1})",
                    "status": "done", "duration_ms": iter_ms,
                    "detail": f"LLM returned query with {len(response.get('fields', []))} fields",
                    "iteration": i + 1, "response_type": "query",
                }))
                response["_loop_outcome"] = "query"
                return response, iterations

            elif response_type == "clarification":
                iterations.append(iteration)
                notify(json.dumps({
                    "stage_num": 4, "total_stages": 4,
                    "name": f"Query Generation (Iteration {i + 1})",
                    "status": "done", "duration_ms": iter_ms,
                    "detail": f"LLM needs clarification: {response.get('reason', '')[:80]}",
                    "iteration": i + 1, "response_type": "clarification",
                }))
                response["_loop_outcome"] = "clarification"
                return response, iterations

            elif response_type == "context_request":
                search_concept = response.get("search_concept", "")
                reason = response.get("reason", "")

                # Perform vector search for the requested concept
                search_results = self._retriever.search_concept(search_concept)
                iteration.vector_search_query = search_concept
                iteration.vector_search_results = search_results

                # Format and append results to accumulated context
                if search_results:
                    formatted = self._format_search_results(search_concept, search_results)
                    additional_context += formatted
                else:
                    additional_context += (
                        f"\n\n## Additional Context (Search: '{search_concept}')\n"
                        f"No additional fields found for '{search_concept}'. "
                        f"Work with the fields already available."
                    )

                iterations.append(iteration)
                notify(json.dumps({
                    "stage_num": 4, "total_stages": 4,
                    "name": f"Query Generation (Iteration {i + 1})",
                    "status": "done", "duration_ms": iter_ms,
                    "detail": (
                        f"Context request: '{search_concept}' — "
                        f"found {len(search_results)} fields"
                    ),
                    "iteration": i + 1, "response_type": "context_request",
                    "data": {
                        "search_concept": search_concept,
                        "reason": reason,
                        "results_count": len(search_results),
                    },
                }))
                logger.info(
                    "Iteration %d: context_request for '%s' — %d results",
                    i + 1, search_concept, len(search_results),
                )
                continue  # Loop again with enriched context

            else:
                # Unknown response_type — treat as query
                iterations.append(iteration)
                logger.warning(
                    "Unknown response_type '%s' in iteration %d, treating as query",
                    response_type, i + 1,
                )
                response["_loop_outcome"] = "query"
                return response, iterations

        # Exhausted all iterations — force a final query
        logger.warning("Generation loop exhausted %d iterations, forcing query", max_iters)
        notify(json.dumps({
            "stage_num": 4, "total_stages": 4,
            "name": "Query Generation (Forced)",
            "status": "running", "timestamp": time.time(),
        }))

        forced_start = time.time()
        forced_response = self._query_builder.generate_single_call(
            context, user_query,
            additional_context + (
                "\n\n## IMPORTANT: Final Iteration\n"
                "You MUST return a Type 1 (query) response now. "
                "Do not request more context or ask clarification questions. "
                "Use the best information available to build the query."
            ),
        )
        forced_ms = (time.time() - forced_start) * 1000

        if forced_response and isinstance(forced_response, dict):
            forced_response["_loop_outcome"] = "forced_query"
            forced_log = self._llm.get_prompt_log()
            last_call = forced_log[-1] if forced_log else {}
            iterations.append(LoopIteration(
                iteration_number=max_iters + 1,
                response_type="forced_query",
                system_prompt=last_call.get("system_prompt", ""),
                user_prompt=last_call.get("user_prompt", ""),
                raw_response=last_call.get("raw_response", ""),
                parsed_response=forced_response,
                duration_ms=forced_ms,
            ))
            notify(json.dumps({
                "stage_num": 4, "total_stages": 4,
                "name": "Query Generation (Forced)",
                "status": "done", "duration_ms": forced_ms,
                "detail": "Forced query after max iterations",
            }))
            return forced_response, iterations

        # Total failure — return empty so the fallback kicks in
        return {}, iterations

    @staticmethod
    def _format_search_results(concept: str, results: List[dict]) -> str:
        """Format vector search results as additional context for the LLM prompt."""
        lines = [
            f"\n\n## Additional Context (Search: '{concept}')",
            f"Found {len(results)} related fields:",
        ]
        for r in results[:15]:
            fqn = f"{r.get('view_name', '?')}.{r.get('field_name', '?')}"
            ftype = r.get("field_type", "?")
            dtype = r.get("data_type", "?")
            desc = r.get("description", "")[:100]
            score = r.get("score", 0)
            lines.append(f"  - {fqn} ({ftype}, {dtype}, score={score:.2f}) — {desc}")
        return "\n".join(lines)

    def _handle_clarification_response(
        self,
        user_message: str,
        session: ConversationSession,
        notify: Callable[[str], None],
    ) -> TurnResponse:
        """
        Process a user's response to a clarification question.

        Two paths depending on how the clarification was generated:

        1. LLM-driven (pending_clarification_source == "llm"):
           Restart the FULL pipeline from scratch, with the user's answer
           appended to the original query for richer context.

        2. Programmatic (pending_clarification_source == "programmatic"):
           Legacy behavior — resolve the specific ambiguity type and proceed.
        """
        clarification_source = session.pending_clarification_source

        if clarification_source == "llm":
            return self._handle_llm_clarification_response(user_message, session, notify)
        else:
            return self._handle_programmatic_clarification_response(user_message, session, notify)

    def _handle_llm_clarification_response(
        self,
        user_message: str,
        session: ConversationSession,
        notify: Callable[[str], None],
    ) -> TurnResponse:
        """
        Handle a user's response to an LLM-driven clarification (Type 2).

        Restarts the full pipeline from scratch with the user's answer
        appended to the original query. This ensures intent extraction,
        retrieval, and query generation all benefit from the new information.
        """
        original_query = session.pending_user_query or ""
        session.clear_pending()

        # Build an enriched query: original question + user's clarification
        enriched_query = f"{original_query} (User clarification: {user_message})"
        notify(f"Restarting pipeline with clarification: {user_message[:50]}...")

        return self._handle_new_query(enriched_query, session, notify)

    def _handle_programmatic_clarification_response(
        self,
        user_message: str,
        session: ConversationSession,
        notify: Callable[[str], None],
    ) -> TurnResponse:
        """
        Handle a user's response to a programmatic ambiguity detection.

        Legacy behavior for attribution, explore_conflict, field_collision.
        """
        stages: List[StageInfo] = []
        t0 = time.time()

        # Save the ambiguity type before resolve_pending clears it
        pending_ambiguity_type = None
        if session.pending_retrieval:
            pending_ambiguity_type = session.pending_retrieval.ambiguity_type

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
            detail=f"Choice: {user_message[:50]}, Type: {pending_ambiguity_type}, "
                   f"Explore: {result.explore_name}",
        ))

        if pending_ambiguity_type == "attribution":
            notify("Re-running retrieval with your choice...")
            t0 = time.time()
            result = self._retriever.retrieve(intent, original_query)
        elif pending_ambiguity_type == "explore_conflict":
            notify("Loading fields from your chosen explore...")
            t0 = time.time()
            chosen_explore = result.explore_name
            explore_candidates = [
                c for c in result._all_candidates
                if c.get("explore_name") == chosen_explore
            ]
            if explore_candidates:
                ctx = self._retriever._cache.get_explore(chosen_explore)
                if ctx:
                    from src.parser.models import LookMLField
                    result.selected_fields = []
                    seen = set()
                    for c in explore_candidates:
                        fqn = f"{c['view_name']}.{c['field_name']}"
                        if fqn not in seen:
                            seen.add(fqn)
                            for f in ctx.get("fields", []):
                                if f.name == c["field_name"] and f.view_name == c["view_name"]:
                                    result.selected_fields.append(f)
                                    break
                    result.model_name = ctx.get("model_name", "")
                    result.joins_needed = ctx.get("joins", [])
        else:
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
