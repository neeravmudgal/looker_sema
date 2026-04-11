"""
Centralized configuration loaded from environment variables.

WHY: Every service needs connection strings, API keys, and tuning parameters.
     Centralizing them here with pydantic-settings gives us:
     - Automatic .env file loading
     - Type validation on startup (fail fast if NEO4J_URI is missing)
     - A single import for any module that needs config

WHO CALLS THIS: Every other module imports `from src.config import settings`.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables or .env file.

    Required variables will cause a startup error if missing.
    Optional variables have sensible defaults for local development.
    """

    # ── Neo4j Connection ─────────────────────────────────────────
    neo4j_uri: str = Field(
        default="bolt://localhost:7687",
        description="Bolt URI for the Neo4j instance",
    )
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(default="semantic_layer_dev")

    # ── API Keys ─────────────────────────────────────────────────
    # All optional at load time. Validated when the provider is actually used.
    openai_api_key: str = Field(default="")
    anthropic_api_key: str = Field(default="")
    google_api_key: str = Field(default="")

    # ── Embedding ────────────────────────────────────────────────
    embedding_provider: str = Field(
        default="openai",
        description="Which embedding provider to use: openai | google",
    )
    embedding_model: str = Field(default="text-embedding-3-small")
    embedding_dimensions: int = Field(default=1536)

    # ── LLM Defaults (overridable at runtime from Streamlit UI) ──
    default_llm_provider: str = Field(default="openai")
    default_llm_model: str = Field(default="gpt-4o-mini")

    # ── LookML Source Directory ──────────────────────────────────
    lookml_dir: str = Field(
        default="./looker_fixtures",
        description="Path to the directory containing .lkml files to index",
    )

    # ── Retrieval Tuning ─────────────────────────────────────────
    confidence_threshold: float = Field(
        default=0.6,
        description="ANN score below this → 'no match' response",
    )
    ambiguity_threshold: float = Field(
        default=0.1,
        description="If top-2 explore scores differ by less than this → explore conflict",
    )
    top_k_fields: int = Field(
        default=15,
        description="Number of candidate fields returned by ANN search",
    )
    top_k_explores: int = Field(
        default=5,
        description="Number of candidate explores returned by ANN search",
    )

    # ── Ollama (local models) ────────────────────────────────────
    ollama_base_url: str = Field(
        default="http://localhost:11434/v1",
        description="Base URL for the Ollama OpenAI-compatible API",
    )
    ollama_embedding_model: str = Field(
        default="nomic-embed-text",
        description="Ollama model to use for embeddings",
    )

    # ── Logging ──────────────────────────────────────────────────
    log_level: str = Field(default="INFO")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",  # Don't crash on unexpected env vars
    }


def get_settings() -> Settings:
    """
    Factory function that creates Settings from current environment.

    We use a function (not a module-level singleton) so that tests can
    override environment variables and get fresh settings.
    """
    return Settings()


# Module-level singleton for convenience — most code imports this directly.
# Tests that need different settings should monkeypatch or use get_settings().
settings = get_settings()


def setup_logging() -> None:
    """Configure root logger based on settings.log_level."""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
