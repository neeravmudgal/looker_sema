"""
Main Streamlit entry point — the chat UI for the GraphRAG Semantic Layer.

RUN: streamlit run app/streamlit_app.py  (from the semantic_layer/ directory)

LAYOUT: Left sidebar (settings + status) | Main area (chat interface)

STARTUP: On first load, the app:
  1. Loads config from .env
  2. Connects to Neo4j
  3. Creates schema (indexes, constraints)
  4. Parses LookML files
  5. Builds/updates the Neo4j graph
  6. Generates embeddings for fields/explores
  7. Builds in-memory cache
  8. Marks system as ready

All startup work is cached in st.session_state so it only runs once per session.
"""

from __future__ import annotations

import sys
import os
import logging

# Add project root to path so imports work when running from app/ directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import streamlit as st

from src.config import settings, setup_logging
from app.components.settings_panel import render_settings_panel
from app.components.chat_interface import render_chat
from app.components.graph_explorer import render_graph_explorer

# ── Page configuration ────────────────────────────────────────────────
st.set_page_config(
    page_title="Semantic Layer Chat",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

setup_logging()
logger = logging.getLogger(__name__)


def initialize_system() -> dict:
    """
    One-time startup sequence. Cached in st.session_state.

    Returns a dict with all initialized services, or an error message.
    """
    status = {"ready": False, "error": None, "stats": {}}

    try:
        # ── Step 1: Connect to Neo4j ─────────────────────────────
        from neo4j import GraphDatabase

        progress = st.progress(0, text="Connecting to Neo4j...")
        driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        # Test connection
        driver.verify_connectivity()
        progress.progress(10, text="Neo4j connected")

        # ── Step 2: Create schema ─────────────────────────────────
        progress.progress(15, text="Creating schema...")
        from src.graph.schema import create_schema
        create_schema(driver)

        # ── Step 3: Parse LookML ──────────────────────────────────
        progress.progress(25, text="Parsing LookML files...")
        from src.parser.lookml_parser import parse_directory

        lookml_dir = settings.lookml_dir
        # Resolve relative paths from the project root
        if not os.path.isabs(lookml_dir):
            lookml_dir = os.path.join(project_root, lookml_dir)

        models, views = parse_directory(lookml_dir)
        total_explores = sum(len(m.explores) for m in models)
        progress.progress(40, text=f"Parsed {len(views)} views, {total_explores} explores")

        # ── Step 4: Build graph ───────────────────────────────────
        progress.progress(45, text="Building knowledge graph...")
        from src.graph.schema import drop_all_data
        from src.graph.graph_builder import build_graph

        drop_all_data(driver)  # Clean rebuild each time
        graph_stats = build_graph(models, views, driver)
        progress.progress(60, text=f"Graph built: {graph_stats['fields']} fields indexed")

        # ── Step 5: Build cache ───────────────────────────────────
        progress.progress(65, text="Building explore cache...")
        from src.graph.cache import ExploreContextCache
        cache = ExploreContextCache(driver)
        cache.rebuild()

        # ── Step 6: Generate embeddings ───────────────────────────
        progress.progress(70, text="Generating embeddings...")
        from src.embeddings.embedder import Embedder
        embedder = Embedder(driver)

        all_fields = cache.all_fields()
        explore_contexts = {
            name: cache.get_explore(name)
            for name in cache.all_explore_names()
        }

        try:
            embed_stats = embedder.embed_all(all_fields, explore_contexts, views=views)
            progress.progress(90, text=f"Embedded {embed_stats['fields_embedded']} fields, {embed_stats.get('views_embedded', 0)} views")
        except Exception as exc:
            logger.warning("Embedding failed (system will work without ANN search): %s", exc)
            embed_stats = {"fields_embedded": 0, "explores_embedded": 0}

        # ── Step 7: Initialize services ───────────────────────────
        progress.progress(95, text="Initializing services...")
        from src.llm.provider import LLMProvider
        from src.conversation.turn_handler import TurnHandler

        llm = LLMProvider(
            provider=settings.default_llm_provider,
            model=settings.default_llm_model,
        )
        turn_handler = TurnHandler(driver, cache, embedder, llm)

        progress.progress(100, text="System ready!")

        status.update({
            "ready": True,
            "driver": driver,
            "cache": cache,
            "embedder": embedder,
            "llm": llm,
            "turn_handler": turn_handler,
            "models": models,
            "views": views,
            "stats": {
                **graph_stats,
                **embed_stats,
                "explore_names": cache.all_explore_names(),
            },
        })

    except Exception as exc:
        status["error"] = str(exc)
        logger.error("Startup failed: %s", exc, exc_info=True)

    return status


def main():
    """Main app entry point."""

    # ── Initialize once per session ───────────────────────────────
    if "system" not in st.session_state:
        st.session_state.system = initialize_system()

    if "conversation" not in st.session_state:
        from src.conversation.session import ConversationSession
        st.session_state.conversation = ConversationSession()

    system = st.session_state.system
    session = st.session_state.conversation

    # ── Sidebar ───────────────────────────────────────────────────
    with st.sidebar:
        render_settings_panel(system, session)

        if system.get("ready"):
            st.divider()
            render_graph_explorer(system.get("cache"))

    # ── Main chat area ────────────────────────────────────────────
    st.title("Semantic Layer Chat")

    if system.get("error"):
        st.error(f"System failed to initialize: {system['error']}")
        st.info("Check your .env file and ensure Neo4j is running (docker compose up -d)")
        return

    if not system.get("ready"):
        st.warning("System is initializing...")
        return

    # Render chat interface
    render_chat(system, session)


if __name__ == "__main__":
    main()
