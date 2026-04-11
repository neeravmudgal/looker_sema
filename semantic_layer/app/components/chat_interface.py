"""
Chat interface component — renders conversation history and handles input.

Renders each turn as a chat message with appropriate formatting:
  - Answer: message + stage timing + collapsible query JSON + explanation
  - Clarification: message + clickable option buttons
  - No match: message + suggestion chips
  - Error: message + expandable error detail

Shows real-time pipeline stage progress while processing.
"""

from __future__ import annotations

import json
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
                # ── Stage timing (collapsible) ────────────────────
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

                # ── Main message ──────────────────────────────────
                st.markdown(turn.content)

                # Answer: show query and explanation
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

    # ── Clarification buttons (only for the latest turn) ──────────
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

    # Show the user message immediately
    with st.chat_message("user"):
        st.markdown(message)

    # Process with real-time stage updates
    with st.chat_message("assistant"):
        # Use st.status() for a collapsible live-updating progress area
        with st.status("Processing your question...", expanded=True) as status_container:
            # This placeholder gets updated with each stage notification
            stage_display = st.empty()
            stage_log = []

            def on_status(stage_msg: str):
                """Callback invoked by TurnHandler at each pipeline stage."""
                stage_log.append(stage_msg)
                # Render all stages so far
                lines = []
                for i, msg in enumerate(stage_log):
                    if i < len(stage_log) - 1:
                        lines.append(f"  ✅ {msg}")
                    else:
                        lines.append(f"  ⏳ {msg}")
                stage_display.markdown("\n\n".join(lines))

            response = turn_handler.handle_turn(message, session, status_callback=on_status)

            # Show final stage summary with timings
            if response.stages:
                timing_lines = []
                for stage in response.stages:
                    if stage.status == "done":
                        icon = "✅"
                    elif stage.status == "error":
                        icon = "❌"
                    elif stage.status == "skipped":
                        icon = "⏭️"
                    else:
                        icon = "⏳"

                    time_str = f"{stage.duration_ms:.0f}ms"
                    timing_lines.append(
                        f"  {icon} **{stage.name}** — {time_str}"
                    )
                    if stage.detail:
                        timing_lines.append(f"     _{stage.detail}_")

                total_str = f"{response.total_duration_ms:.0f}ms"
                timing_lines.append(f"\n  **Total: {total_str}**")

                stage_display.markdown("\n\n".join(timing_lines))

            # Update the status container label
            if response.turn_type == "answer":
                status_container.update(
                    label=f"Done in {response.total_duration_ms:.0f}ms",
                    state="complete",
                    expanded=False,
                )
            elif response.turn_type == "clarification":
                status_container.update(
                    label="Needs clarification",
                    state="complete",
                    expanded=False,
                )
            elif response.turn_type == "no_match":
                status_container.update(
                    label="No confident match found",
                    state="complete",
                    expanded=False,
                )
            else:
                status_container.update(
                    label="Error occurred",
                    state="error",
                    expanded=True,
                )

        # ── Render the actual response below the status ───────────
        st.markdown(response.message)

        if response.turn_type == "answer" and response.query:
            with st.expander("Generated Looker Query", expanded=True):
                display_query = {
                    k: v for k, v in response.query.items()
                    if not k.startswith("_")
                }
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

    st.rerun()
