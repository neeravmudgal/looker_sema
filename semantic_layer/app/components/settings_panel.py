"""
Sidebar settings and status panel.

Displays: LLM/embedding configuration, system health status,
session stats, and action buttons.
"""

from __future__ import annotations

import streamlit as st

from src.config import settings


# Available LLM providers and models
LLM_PROVIDERS = {
    "ollama": ["qwen3:8b", "llama3.2", "llama3.1", "mistral", "deepseek-r1", "gemma2"],
    "openai": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"],
    "anthropic": ["claude-sonnet-4-5", "claude-haiku-4-5-20251001"],
    "google": ["gemini-2.0-flash", "gemini-1.5-pro"],
}

EMBEDDING_PROVIDERS = {
    "ollama": ["nomic-embed-text", "mxbai-embed-large", "all-minilm"],
    "openai": ["text-embedding-3-small", "text-embedding-3-large"],
    "google": ["text-embedding-004"],
}


def render_settings_panel(system: dict, session) -> None:
    """Render the full sidebar settings panel."""

    # ── LLM Settings ─────────────────────────────────────────────
    st.subheader("LLM Settings")

    provider = st.selectbox(
        "Provider",
        options=list(LLM_PROVIDERS.keys()),
        index=list(LLM_PROVIDERS.keys()).index(settings.default_llm_provider)
        if settings.default_llm_provider in LLM_PROVIDERS else 0,
        key="llm_provider",
    )

    models = LLM_PROVIDERS.get(provider, ["default"])
    model = st.selectbox("Model", options=models, key="llm_model")

    # Update LLM provider if changed
    if system.get("ready") and system.get("llm"):
        llm = system["llm"]
        if llm.provider != provider or llm.model != model:
            from src.llm.provider import LLMProvider
            from src.config import settings as _settings
            _api_key = {
                "openai": _settings.openai_api_key,
                "anthropic": _settings.anthropic_api_key,
                "google": _settings.google_api_key,
            }.get(provider)
            system["llm"] = LLMProvider(provider=provider, model=model, api_key=_api_key or None)
            # Re-create turn handler with new LLM
            from src.conversation.turn_handler import TurnHandler
            system["turn_handler"] = TurnHandler(
                system["driver"], system["cache"],
                system["embedder"], system["llm"],
            )

    # ── System Status ────────────────────────────────────────────
    st.subheader("System Status")

    if system.get("ready"):
        stats = system.get("stats", {})
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Explores", len(stats.get("explore_names", [])))
            st.metric("Fields", stats.get("fields", 0))
        with col2:
            st.metric("Embedded", stats.get("fields_embedded", 0))
            st.metric("Views", stats.get("views", 0))

        st.success("System ready", icon="✅")
    elif system.get("error"):
        st.error(f"Error: {system['error'][:100]}")
    else:
        st.warning("Initializing...")

    # ── Session Stats ────────────────────────────────────────────
    st.subheader("Session")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Turns", session.turn_count)
    with col2:
        tokens = session.token_count
        st.metric("Tokens", f"{tokens['input'] + tokens['output']:,}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Export JSON", use_container_width=True):
            st.download_button(
                "Download",
                data=session.export_json(),
                file_name=f"conversation_{session.session_id[:8]}.json",
                mime="application/json",
            )
    with col2:
        if st.button("Clear Chat", use_container_width=True):
            from src.conversation.session import ConversationSession
            st.session_state.conversation = ConversationSession()
            st.rerun()
