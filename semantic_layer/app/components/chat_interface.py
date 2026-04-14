"""
Chat interface component — renders conversation history and handles input.
"""

from __future__ import annotations

import json
import time
import streamlit as st


def render_chat(system: dict, session) -> None:
    """Render the full chat interface: history + input."""

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

    # ── Clarification buttons ────────────────────────────────────
    if session.state == "WAITING_FOR_CLARIFICATION" and session.turns:
        latest = session.turns[-1]
        if latest.role == "assistant" and latest.turn_type == "clarification":
            st.markdown("---")
            if session.pending_retrieval:
                for i, option in enumerate(session.pending_retrieval.clarification_options):
                    if st.button(option, key=f"option_{i}", use_container_width=True):
                        _process_message(option, system, session)

    # ── Chat input ────────────────────────────────────────────────
    user_input = st.chat_input("Ask about your data...")
    if user_input:
        _process_message(user_input, system, session)


def _process_message(message: str, system: dict, session) -> None:
    """Process a user message with real-time stage progress display."""
    turn_handler = system.get("turn_handler")
    if not turn_handler:
        st.error("System not ready. Please wait for initialization.")
        return

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

        # ── Debug Panel: everything that happened ────────────────
        _render_debug_panel(response, completed_stages)

    st.rerun()


def _render_debug_panel(response, completed_stages: list):
    """
    Render a full debug panel showing everything that happened:
    - Each pipeline stage with its data (intent, candidates, scores)
    - Every LLM call with full system prompt, user prompt, raw response
    """
    with st.expander("Debug: Full Pipeline Details", expanded=False):

        # ── Stage-by-stage data ──────────────────────────────────
        st.markdown("## Pipeline Stages")

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

        # ── LLM Calls ────────────────────────────────────────────
        if response.prompt_log:
            st.markdown("## LLM Calls")

            for entry in response.prompt_log:
                call_num = entry["call_number"]
                mode = "JSON" if entry["json_mode"] else "Text"

                st.markdown(f"### LLM Call {call_num} ({mode} mode)")

                # System prompt
                with st.expander(f"System Prompt (Call {call_num})", expanded=False):
                    st.code(entry["system_prompt"], language="text")

                # User prompt (the big one - full context sent to the LLM)
                with st.expander(f"User Prompt (Call {call_num})", expanded=False):
                    st.code(entry["user_prompt"], language="text")

                # Raw response
                with st.expander(f"Raw LLM Response (Call {call_num})", expanded=True):
                    st.code(entry["raw_response"], language="json")

                st.divider()
        else:
            st.markdown("_No LLM calls recorded for this turn._")
