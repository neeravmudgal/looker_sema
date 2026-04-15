"""
Chat interface component — renders conversation history and handles input.

WHY: This is the main user-facing component. It displays the conversation,
     processes new messages through the pipeline, and shows real-time progress.
     When "Show full LLM context" is enabled, it displays the complete context
     (system prompt, user prompt, raw response) for every LLM call, plus
     multi-hop iteration details — giving full transparency into what the
     system is doing.

CALLED BY: streamlit_app.py
CALLS: TurnHandler.handle_turn() for message processing.
"""

from __future__ import annotations

import json
import time
import streamlit as st


def render_chat(system: dict, session) -> None:
    """
    Render the full chat interface: conversation history + input box.

    Displays all past turns from the session, including pipeline stages,
    generated queries, confidence scores, and full LLM context when the
    debug toggle is enabled. Also handles clarification button rendering
    and new message input.

    Args:
        system: The initialized system dict with turn_handler, cache, etc.
        session: The current ConversationSession instance.
    """

    show_context = st.session_state.get("show_full_context", False)

    # ── Conversation history ──────────────────────────────────────
    for turn in session.get_history():
        if turn.role == "user":
            with st.chat_message("user"):
                st.markdown(turn.content)
        else:
            with st.chat_message("assistant"):
                if turn.stages:
                    total_str = f"{turn.total_duration_ms:.0f}ms"
                    with st.expander(f"Pipeline stages — {total_str}", expanded=False):
                        for stage in turn.stages:
                            icon = {"done": "✅", "error": "❌", "skipped": "⏭️"}.get(
                                stage.get("status", ""), "⏳"
                            )
                            ms = stage.get("duration_ms", 0)
                            st.markdown(f"{icon} **{stage['name']}** — {ms:.0f}ms")
                            if stage.get("detail"):
                                st.caption(f"    {stage['detail']}")

                st.markdown(turn.content)

                if turn.turn_type == "answer" and turn.generated_query:
                    with st.expander("Generated Looker Query", expanded=False):
                        display_query = {
                            k: v for k, v in turn.generated_query.items()
                            if not k.startswith("_")
                        }
                        st.code(json.dumps(display_query, indent=2), language="json")

                    if turn.confidence is not None:
                        st.caption(
                            f"Explore: **{turn.explore_used}** | "
                            f"Confidence: **{turn.confidence:.0%}** | "
                            f"Fields: {len(turn.fields_used or [])}"
                        )

                    if turn.warnings:
                        for warning in turn.warnings:
                            st.warning(warning, icon="⚠️")

                if turn.turn_type == "error":
                    st.error("An error occurred processing your question.")

                # Full context display for history turns
                if show_context:
                    _render_history_context(turn)

    # ── Clarification buttons ────────────────────────────────────
    if session.state == "WAITING_FOR_CLARIFICATION" and session.turns:
        latest = session.turns[-1]
        if latest.role == "assistant" and latest.turn_type == "clarification":
            st.markdown("---")
            # For LLM-driven clarifications, show options from the turn content
            if session.pending_clarification_source == "llm":
                options = []
                if hasattr(latest, 'loop_iterations') and latest.loop_iterations:
                    # Get options from the last iteration's parsed response
                    for it in reversed(latest.loop_iterations or []):
                        if it.get("response_type") == "clarification":
                            options = it.get("parsed_response", {}).get("options", [])
                            break
                if not options:
                    # Try to extract from the message itself
                    options = _extract_options_from_message(latest.content)
                for i, option in enumerate(options):
                    if st.button(option, key=f"option_{i}", use_container_width=True):
                        _process_message(option, system, session)
            elif session.pending_retrieval:
                for i, option in enumerate(session.pending_retrieval.clarification_options):
                    if st.button(option, key=f"option_{i}", use_container_width=True):
                        _process_message(option, system, session)

    # ── Chat input ────────────────────────────────────────────────
    user_input = st.chat_input("Ask about your data...")
    if user_input:
        _process_message(user_input, system, session)


def _extract_options_from_message(message: str) -> list:
    """Extract options from a clarification message if they follow A), B), C) pattern."""
    import re
    options = re.findall(r'([A-C]\)[^\n]+)', message)
    return options


def _render_history_context(turn) -> None:
    """
    Render full LLM context for a past conversation turn.

    Shows loop iterations (if multi-hop was used) and all LLM call
    prompts/responses from the turn's stored prompt_log.
    """
    has_iterations = turn.loop_iterations and len(turn.loop_iterations) > 0
    has_prompts = turn.prompt_log and len(turn.prompt_log) > 0

    if not has_iterations and not has_prompts:
        return

    with st.expander("Full LLM Context", expanded=False):
        if has_iterations:
            _render_iterations(turn.loop_iterations)
        if has_prompts:
            _render_prompt_log(turn.prompt_log)


def _process_message(message: str, system: dict, session) -> None:
    """
    Process a user message with real-time stage progress display.

    Shows live pipeline stages as they complete, then renders the full
    response including query, explanation, and full LLM context details
    when the debug toggle is enabled.

    Args:
        message: The user's input message.
        system: The initialized system dict.
        session: The current ConversationSession.
    """
    turn_handler = system.get("turn_handler")
    if not turn_handler:
        st.error("System not ready. Please wait for initialization.")
        return

    show_context = st.session_state.get("show_full_context", False)

    with st.chat_message("user"):
        st.markdown(message)

    with st.chat_message("assistant"):
        with st.status("Processing your question...", expanded=True) as status_container:
            stage_display = st.empty()
            completed_stages = []
            current_stage = None
            pipeline_start = time.time()

            def on_status(stage_msg: str):
                nonlocal current_stage
                try:
                    info = json.loads(stage_msg)
                except (json.JSONDecodeError, TypeError):
                    info = {"name": stage_msg, "status": "running"}

                if info.get("status") == "running":
                    current_stage = info
                elif info.get("status") == "done":
                    completed_stages.append(info)
                    current_stage = None

                # Render live progress with elapsed time
                lines = []
                for s in completed_stages:
                    ms = s.get("duration_ms", 0)
                    lines.append(f"✅ **{s.get('name', '?')}** — {ms:.0f}ms")
                    if s.get("detail"):
                        lines.append(f"  _{s['detail']}_")
                if current_stage:
                    elapsed = (time.time() - current_stage.get("timestamp", pipeline_start)) * 1000
                    lines.append(f"⏳ **{current_stage.get('name', '?')}** — {elapsed:.0f}ms...")
                stage_display.markdown("\n\n".join(lines))

            response = turn_handler.handle_turn(message, session, status_callback=on_status)

            # ── Final render: stages with timing ─────────────────
            if response.stages:
                lines = []
                for stage in response.stages:
                    icon = {"done": "✅", "error": "❌", "skipped": "⏭️"}.get(stage.status, "⏳")
                    lines.append(f"{icon} **{stage.name}** — {stage.duration_ms:.0f}ms")
                    if stage.detail:
                        lines.append(f"  _{stage.detail}_")
                lines.append(f"\n**Total: {response.total_duration_ms:.0f}ms**")
                stage_display.markdown("\n\n".join(lines))

            # Update status label
            if response.turn_type == "answer":
                status_container.update(label=f"Done in {response.total_duration_ms:.0f}ms", state="complete", expanded=False)
            elif response.turn_type == "clarification":
                status_container.update(label="Needs clarification", state="complete", expanded=False)
            elif response.turn_type == "no_match":
                status_container.update(label="No confident match found", state="complete", expanded=False)
            else:
                status_container.update(label="Error occurred", state="error", expanded=True)

        # ── Response content ─────────────────────────────────────
        st.markdown(response.message)

        if response.turn_type == "answer" and response.query:
            with st.expander("Generated Looker Query", expanded=True):
                display_query = {k: v for k, v in response.query.items() if not k.startswith("_")}
                st.code(json.dumps(display_query, indent=2), language="json")

            if response.confidence is not None:
                st.caption(
                    f"Explore: **{response.explore_used}** | "
                    f"Confidence: **{response.confidence:.0%}** | "
                    f"Fields: {len(response.fields_used or [])}"
                )

            if response.warnings:
                for warning in response.warnings:
                    st.warning(warning, icon="⚠️")

        if response.turn_type == "error" and response.error_detail:
            with st.expander("Technical detail"):
                st.code(response.error_detail, language="text")

        # ── Full Context Display ─────────────────────────────────
        if show_context:
            _render_full_context(response, completed_stages)
        else:
            # Collapsed debug panel as fallback
            _render_debug_panel(response, completed_stages)

    st.rerun()


def _render_full_context(response, completed_stages: list):
    """
    Render the FULL LLM context prominently in the chat.

    When "Show full LLM context" is enabled, this displays:
    - Pipeline stage data (intent, candidates, scores) in expanded tabs
    - Multi-hop loop iterations with full prompts and responses
    - Every LLM call's system prompt, user prompt, and raw response

    This is the key transparency feature — the user sees exactly what
    went to the LLM at each step.
    """
    # Build tab labels
    tab_labels = ["Pipeline Stages"]
    if response.loop_iterations:
        for it in response.loop_iterations:
            tab_labels.append(f"Iteration {it['iteration_number']}")
    if response.prompt_log:
        for entry in response.prompt_log:
            tab_labels.append(f"LLM Call {entry['call_number']}")

    if len(tab_labels) == 1 and not response.prompt_log:
        # Only pipeline stages, no LLM calls to show
        with st.expander("Full Pipeline Context", expanded=True):
            _render_stages_data(completed_stages)
        return

    tabs = st.tabs(tab_labels)
    tab_idx = 0

    # Tab: Pipeline Stages
    with tabs[tab_idx]:
        _render_stages_data(completed_stages)
    tab_idx += 1

    # Tabs: Multi-hop iterations
    if response.loop_iterations:
        for it in response.loop_iterations:
            with tabs[tab_idx]:
                _render_single_iteration(it)
            tab_idx += 1

    # Tabs: LLM Calls
    if response.prompt_log:
        for entry in response.prompt_log:
            with tabs[tab_idx]:
                _render_single_llm_call(entry)
            tab_idx += 1


def _render_stages_data(completed_stages: list):
    """Render all pipeline stage data with their payloads."""
    for i, stage_data in enumerate(completed_stages):
        name = stage_data.get("name", f"Stage {i+1}")
        ms = stage_data.get("duration_ms", 0)
        detail = stage_data.get("detail", "")
        data = stage_data.get("data")

        st.markdown(f"### {i+1}. {name} ({ms:.0f}ms)")
        if detail:
            st.markdown(f"_{detail}_")

        if data:
            st.json(data)

        st.divider()


def _render_single_iteration(it: dict):
    """
    Render one multi-hop loop iteration with full details.

    Shows the response type, duration, and for context_request iterations,
    the vector search query and results.
    """
    resp_type = it.get("response_type", "?")
    ms = it.get("duration_ms", 0)

    st.markdown(f"**Response Type:** `{resp_type}` | **Duration:** {ms:.0f}ms")

    if resp_type == "context_request":
        search_q = it.get("vector_search_query", "")
        search_results = it.get("vector_search_results", [])
        st.markdown(f"**Search Concept:** `{search_q}`")
        st.markdown(f"**Search Results:** {len(search_results)} fields")
        if search_results:
            with st.expander("Vector Search Results", expanded=True):
                for r in search_results[:15]:
                    fqn = f"{r.get('view_name', '?')}.{r.get('field_name', '?')}"
                    score = r.get("score", 0)
                    desc = r.get("description", "")[:80]
                    st.caption(f"  {fqn} (score={score:.2f}) — {desc}")

    # Full prompt and response for this iteration
    if it.get("system_prompt"):
        with st.expander("System Prompt", expanded=False):
            st.code(it["system_prompt"], language="text")

    if it.get("user_prompt"):
        with st.expander("User Prompt (Full Context Sent to LLM)", expanded=True):
            st.code(it["user_prompt"], language="text")

    if it.get("raw_response"):
        with st.expander("Raw LLM Response", expanded=True):
            st.code(it["raw_response"], language="json")

    if it.get("additional_context"):
        with st.expander("Accumulated Additional Context", expanded=False):
            st.code(it["additional_context"], language="text")

    if it.get("parsed_response"):
        with st.expander("Parsed Response", expanded=False):
            st.json(it["parsed_response"])


def _render_single_llm_call(entry: dict):
    """Render one LLM call with system prompt, user prompt, and raw response."""
    mode = "JSON" if entry.get("json_mode") else "Text"
    st.markdown(f"**Mode:** {mode}")

    with st.expander("System Prompt", expanded=False):
        st.code(entry.get("system_prompt", ""), language="text")

    with st.expander("User Prompt (Full Context Sent to LLM)", expanded=True):
        st.code(entry.get("user_prompt", ""), language="text")

    with st.expander("Raw LLM Response", expanded=True):
        st.code(entry.get("raw_response", ""), language="json")


def _render_iterations(iterations: list):
    """Render all multi-hop iterations."""
    st.markdown("## Multi-Hop Iterations")
    for it in iterations:
        st.markdown(f"### Iteration {it.get('iteration_number', '?')}")
        _render_single_iteration(it)
        st.divider()


def _render_prompt_log(prompt_log: list):
    """Render all LLM calls from the prompt log."""
    st.markdown("## LLM Calls")
    for entry in prompt_log:
        call_num = entry.get("call_number", "?")
        st.markdown(f"### LLM Call {call_num}")
        _render_single_llm_call(entry)
        st.divider()


def _render_debug_panel(response, completed_stages: list):
    """
    Render a collapsed debug panel (fallback when full context display is off).

    Shows the same information as _render_full_context but inside a collapsed
    expander for users who don't need the full transparency view.
    """
    with st.expander("Debug: Full Pipeline Details", expanded=False):

        # ── Stage-by-stage data ──────────────────────────────────
        st.markdown("## Pipeline Stages")
        _render_stages_data(completed_stages)

        # ── Multi-hop iterations ─────────────────────────────────
        if response.loop_iterations:
            _render_iterations(response.loop_iterations)

        # ── LLM Calls ────────────────────────────────────────────
        if response.prompt_log:
            _render_prompt_log(response.prompt_log)
        else:
            st.markdown("_No LLM calls recorded for this turn._")
