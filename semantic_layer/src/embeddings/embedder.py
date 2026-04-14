"""
Embedding service — generates and stores vector embeddings for LookML objects.

WHY: The retrieval system uses ANN (approximate nearest neighbor) search to find
     fields that semantically match a user's natural language question.
     This requires pre-computed vector embeddings for every field and explore,
     stored on their Neo4j nodes and indexed for fast cosine similarity search.

CALLED BY: Startup sequence (embed all), retriever.py (embed user queries)
CALLS: OpenAI / Google embedding APIs, Neo4j driver, strategies.py

SUPPORTED PROVIDERS:
  - openai:  text-embedding-3-small / text-embedding-3-large
  - google:  text-embedding-004

INCREMENTAL REFRESH: Tracks SHA256 hash of each field's embedding text.
Only re-embeds fields whose text changed (e.g. description was edited).
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Dict, List, Optional

from neo4j import Driver

from src.config import settings
from src.parser.models import LookMLField, LookMLExplore
from src.embeddings.strategies import format_field_text, format_explore_text, format_view_text

logger = logging.getLogger(__name__)


class EmbeddingError(Exception):
    """Raised when an embedding API call fails after retries."""
    pass


class Embedder:
    """
    Generates vector embeddings and stores them on Neo4j nodes.

    Usage:
        embedder = Embedder(driver)
        embedder.embed_all(fields, explores)     # Startup — embed everything
        vector = embedder.embed_query("revenue")  # Runtime — embed a user query
    """

    def __init__(self, driver: Driver, provider: str = "", model: str = ""):
        self._driver = driver
        self._provider = provider or settings.embedding_provider
        self._model = model or settings.embedding_model
        self._dimensions = settings.embedding_dimensions
        self._client = None

    def _get_client(self):
        """Lazy-init the embedding API client."""
        if self._client is not None:
            return self._client

        if self._provider == "openai":
            import openai
            key = settings.openai_api_key or None
            self._client = openai.OpenAI(api_key=key)
        elif self._provider == "google":
            import google.genai as genai
            self._client = genai.Client(api_key=settings.google_api_key)
        elif self._provider == "ollama":
            # Ollama exposes an OpenAI-compatible embeddings endpoint.
            # We reuse the OpenAI SDK pointed at localhost.
            import openai
            self._client = openai.OpenAI(
                base_url=settings.ollama_base_url,
                api_key="ollama",  # Ollama doesn't need a real key
            )
            # Override model to the Ollama embedding model
            if not self._model or self._model.startswith("text-embedding"):
                self._model = settings.ollama_embedding_model
        else:
            raise EmbeddingError(f"Unsupported embedding provider: {self._provider}")

        return self._client

    def embed_all(
        self,
        fields: List[LookMLField],
        explore_contexts: Dict[str, dict],
        views: Optional[Dict] = None,
    ) -> Dict[str, int]:
        """
        Embed all fields, explores, and views, store vectors on Neo4j nodes.

        Args:
            fields: All LookMLField objects to embed (typically from cache.all_fields())
            explore_contexts: Dict of explore_name → context dict (from cache)
            views: Dict of view_name → LookMLView (optional, for view embeddings)

        Returns:
            {"fields_embedded": n, "explores_embedded": m, "views_embedded": v, "skipped": k}
        """
        stats = {"fields_embedded": 0, "explores_embedded": 0, "skipped": 0}

        # ── Embed fields (deduplicated) ───────────────────────────
        # Same field (name+view) exists as separate nodes per explore,
        # but the embedding text is identical (explore_name excluded).
        # We embed each unique (name, view_name) ONCE and copy the vector
        # to all explore-scoped copies. This halves API calls and ensures
        # ANN search returns diverse fields instead of duplicates.
        unique_texts = {}   # (name, view_name) → text
        unique_hashes = {}  # (name, view_name) → hash
        all_field_keys = [] # All (name, view_name, explore_name) for storage

        for field in fields:
            if field.field_type == "dimension_group":
                # Skip the raw dimension_group — we embed the expanded fields instead
                stats["skipped"] += 1
                continue

            key = (field.name, field.view_name)
            all_field_keys.append({
                "name": field.name,
                "view_name": field.view_name,
                "explore_name": field.explore_name,
            })

            if key not in unique_texts:
                text = format_field_text(field)
                unique_texts[key] = text
                unique_hashes[key] = hashlib.sha256(text.encode()).hexdigest()

        # Embed only unique fields
        unique_keys_ordered = list(unique_texts.keys())
        unique_texts_ordered = [unique_texts[k] for k in unique_keys_ordered]

        if unique_texts_ordered:
            logger.info(
                "Embedding %d unique fields (from %d total, %.0f%% dedup savings)...",
                len(unique_texts_ordered), len(all_field_keys),
                (1 - len(unique_texts_ordered) / max(len(all_field_keys), 1)) * 100,
            )
            unique_embeddings = self._embed_batch(unique_texts_ordered)

            # Build lookup: (name, view_name) → embedding
            embedding_lookup = {}
            for i, key in enumerate(unique_keys_ordered):
                embedding_lookup[key] = unique_embeddings[i]

            # Store: copy the same embedding to every explore-scoped node
            store_batch = []
            for fk in all_field_keys:
                key = (fk["name"], fk["view_name"])
                store_batch.append({
                    "name": fk["name"],
                    "view_name": fk["view_name"],
                    "explore_name": fk["explore_name"],
                    "text_hash": unique_hashes[key],
                })

            self._store_field_embeddings(store_batch, [
                embedding_lookup[(fk["name"], fk["view_name"])] for fk in all_field_keys
            ])
            stats["fields_embedded"] = len(all_field_keys)
            stats["unique_embedded"] = len(unique_texts_ordered)

        # ── Embed explores ────────────────────────────────────────
        explore_texts = []
        explore_keys = []

        for explore_name, ctx in explore_contexts.items():
            exp_info = ctx.get("explore", {})
            if exp_info.get("is_hidden", False):
                continue

            # Build explore embedding text
            join_view_names = [j["view_name"] for j in ctx.get("joins", [])]
            explore_text_input = {
                "name": explore_name,
                "label": exp_info.get("label", ""),
                "description": exp_info.get("description", ""),
                "base_view": ctx.get("base_view", ""),
                "join_view_names": join_view_names,
            }
            text = format_explore_text(explore_text_input)
            text_hash = hashlib.sha256(text.encode()).hexdigest()

            explore_texts.append(text)
            explore_keys.append({
                "name": explore_name,
                "model_name": ctx.get("model_name", ""),
                "text_hash": text_hash,
            })

        if explore_texts:
            logger.info("Embedding %d explores...", len(explore_texts))
            embeddings = self._embed_batch(explore_texts)
            self._store_explore_embeddings(explore_keys, embeddings)
            stats["explores_embedded"] = len(embeddings)

        # ── Embed views ──────────────────────────────────────────
        stats["views_embedded"] = 0
        if views:
            view_texts = []
            view_keys = []

            for view_name, view_obj in views.items():
                text = format_view_text(
                    view_name=view_name,
                    fields=view_obj.fields,
                    sql_table_name=view_obj.sql_table_name or "",
                    view_label=view_obj.view_label or "",
                )
                text_hash = hashlib.sha256(text.encode()).hexdigest()
                view_texts.append(text)
                view_keys.append({
                    "name": view_name,
                    "text_hash": text_hash,
                })

            if view_texts:
                logger.info("Embedding %d views...", len(view_texts))
                embeddings = self._embed_batch(view_texts)
                self._store_view_embeddings(view_keys, embeddings)
                stats["views_embedded"] = len(embeddings)

        logger.info(
            "Embedding complete: %d fields, %d explores, %d views, %d skipped",
            stats["fields_embedded"], stats["explores_embedded"],
            stats["views_embedded"], stats["skipped"],
        )
        return stats

    def embed_query(self, text: str) -> List[float]:
        """
        Embed a single user query for ANN search.

        This is called at runtime for every user question, so it must be fast.
        No batching — single API call.
        """
        result = self._embed_batch([text])
        if result:
            return result[0]
        raise EmbeddingError("Failed to embed query text")

    def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a batch of texts using the configured provider.

        Batches into groups of 100 to respect API limits.
        Retries up to 3 times on rate limit errors with exponential backoff.
        """
        all_embeddings: List[List[float]] = []
        batch_size = 100

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            embeddings = self._call_api(batch)
            all_embeddings.extend(embeddings)

            # Brief pause between batches to avoid rate limits
            if i + batch_size < len(texts):
                time.sleep(0.3)

        return all_embeddings

    def _call_api(self, texts: List[str], retries: int = 3) -> List[List[float]]:
        """
        Make the actual API call with retry logic.

        Retries on rate limit errors with exponential backoff: 1s, 2s, 4s.
        """
        client = self._get_client()

        for attempt in range(retries):
            try:
                if self._provider in ("openai", "ollama"):
                    # Both use the OpenAI SDK — Ollama is API-compatible
                    response = client.embeddings.create(
                        model=self._model,
                        input=texts,
                    )
                    return [item.embedding for item in response.data]

                elif self._provider == "google":
                    result = client.models.embed_content(
                        model=self._model,
                        contents=texts,
                    )
                    return [e.values for e in result.embeddings]

                else:
                    raise EmbeddingError(f"Unsupported provider: {self._provider}")

            except Exception as exc:
                exc_str = str(exc).lower()
                is_rate_limit = "rate" in exc_str or "429" in exc_str or "quota" in exc_str

                if is_rate_limit and attempt < retries - 1:
                    wait = 2 ** attempt  # 1s, 2s, 4s
                    logger.warning(
                        "Rate limited on embedding attempt %d, waiting %ds: %s",
                        attempt + 1, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    logger.error("Embedding API error: %s", exc)
                    raise EmbeddingError(f"Embedding failed after {retries} attempts: {exc}")

        raise EmbeddingError("Embedding failed: exhausted retries")

    def _store_field_embeddings(
        self,
        field_keys: List[dict],
        embeddings: List[List[float]],
    ) -> None:
        """Write embedding vectors back to Field nodes in Neo4j."""
        with self._driver.session() as session:
            # Batch update in chunks
            chunk_size = 100
            for i in range(0, len(field_keys), chunk_size):
                batch = []
                for j in range(i, min(i + chunk_size, len(field_keys))):
                    batch.append({
                        "name": field_keys[j]["name"],
                        "view_name": field_keys[j]["view_name"],
                        "explore_name": field_keys[j]["explore_name"],
                        "embedding": embeddings[j],
                        "embedding_hash": field_keys[j]["text_hash"],
                    })

                session.run(
                    """
                    UNWIND $batch AS b
                    MATCH (f:Field {name: b.name, view_name: b.view_name, explore_name: b.explore_name})
                    SET f.embedding = b.embedding,
                        f.embedding_hash = b.embedding_hash
                    """,
                    batch=batch,
                )

    def _store_explore_embeddings(
        self,
        explore_keys: List[dict],
        embeddings: List[List[float]],
    ) -> None:
        """Write embedding vectors back to Explore nodes in Neo4j."""
        with self._driver.session() as session:
            batch = []
            for i, key in enumerate(explore_keys):
                batch.append({
                    "name": key["name"],
                    "model_name": key["model_name"],
                    "embedding": embeddings[i],
                    "embedding_hash": key["text_hash"],
                })

            session.run(
                """
                UNWIND $batch AS b
                MATCH (e:Explore {name: b.name, model_name: b.model_name})
                SET e.embedding = b.embedding,
                    e.embedding_hash = b.embedding_hash
                """,
                batch=batch,
            )

    def _store_view_embeddings(
        self,
        view_keys: List[dict],
        embeddings: List[List[float]],
    ) -> None:
        """Write embedding vectors back to View nodes in Neo4j."""
        with self._driver.session() as session:
            batch = []
            for i, key in enumerate(view_keys):
                batch.append({
                    "name": key["name"],
                    "embedding": embeddings[i],
                    "embedding_hash": key["text_hash"],
                })

            session.run(
                """
                UNWIND $batch AS b
                MATCH (v:View {name: b.name})
                SET v.embedding = b.embedding,
                    v.embedding_hash = b.embedding_hash
                """,
                batch=batch,
            )

    def get_embedding_stats(self) -> Dict[str, int]:
        """How many nodes have embeddings vs total."""
        stats = {}
        with self._driver.session() as session:
            r = session.run("MATCH (f:Field) WHERE f.embedding IS NOT NULL RETURN count(f) AS c")
            stats["fields_with_embeddings"] = r.single()["c"]

            r = session.run("MATCH (f:Field) RETURN count(f) AS c")
            stats["total_fields"] = r.single()["c"]

            r = session.run("MATCH (e:Explore) WHERE e.embedding IS NOT NULL RETURN count(e) AS c")
            stats["explores_with_embeddings"] = r.single()["c"]

            r = session.run("MATCH (e:Explore) RETURN count(e) AS c")
            stats["total_explores"] = r.single()["c"]

        return stats
