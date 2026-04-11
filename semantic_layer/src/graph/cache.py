"""
In-memory cache of explore contexts — eliminates Neo4j round-trips during retrieval.

WHY: During retrieval, we need to answer "which explores contain this field?"
     hundreds of times per query. Hitting Neo4j for each lookup adds ~5ms per call,
     which means 100 lookups = 500ms of pure latency. By caching explore contexts
     in a Python dict at startup, each lookup drops to ~0.001ms.

CALLED BY: retriever.py (field-to-explore lookups), context_assembler.py (full context)
CALLS: Neo4j driver (only during rebuild), graph_queries.py

THREAD SAFETY: Uses threading.RLock so Streamlit's multi-threaded rerenders
don't corrupt the cache.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Dict, List, Optional

from neo4j import Driver

from src.parser.models import LookMLField, LookMLJoin, LookMLExplore
from src.graph import graph_queries as Q

logger = logging.getLogger(__name__)


class ExploreContextCache:
    """
    Two-index in-memory cache built from the Neo4j graph at startup.

    INDEX 1 — Explore context (forward index):
        explore_name → {
            "explore": LookMLExplore metadata,
            "fields": [LookMLField, ...],
            "joins": [{"view_name", "sql_on", "join_type", "relationship", "is_pdt"}, ...],
            "base_view": str,
            "always_filter": dict,
            "model_name": str,
        }

    INDEX 2 — Field-to-explore (reverse index):
        "view_name.field_name" → ["explore_a", "explore_b", ...]

    The reverse index is the key to fast retrieval:
    when ANN search returns a candidate field "users.country", we instantly
    know which explores can serve it without any graph query.

    Usage:
        cache = ExploreContextCache(driver)
        cache.rebuild()
        explores = cache.get_explores_for_field("users.country")
        context = cache.get_explore("events")
    """

    def __init__(self, driver: Driver):
        self._driver = driver
        self._lock = threading.RLock()
        self._explore_index: Dict[str, dict] = {}
        self._field_to_explores: Dict[str, List[str]] = {}
        self._all_fields: List[LookMLField] = []

    def rebuild(self) -> None:
        """
        Rebuild both indexes from the Neo4j graph.

        Called once at startup and again if LookML files change.
        Acquires the write lock for the entire duration to ensure
        consistent state.
        """
        logger.info("Rebuilding explore context cache...")

        new_explore_index: Dict[str, dict] = {}
        new_field_to_explores: Dict[str, List[str]] = {}
        new_all_fields: List[LookMLField] = []

        with self._driver.session() as session:
            # ── Load all explores ─────────────────────────────────
            explore_records = session.run(Q.GET_ALL_EXPLORES).data()

            for rec in explore_records:
                explore_name = rec["name"]
                model_name = rec["model_name"]

                # Parse always_filter from JSON string
                always_filter = {}
                af_json = rec.get("always_filter_json", "{}")
                if af_json:
                    try:
                        always_filter = json.loads(af_json)
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Load joins for this explore
                join_records = session.run(
                    Q.GET_EXPLORE_JOINS,
                    explore_name=explore_name,
                    model_name=model_name,
                ).data()

                joins = [
                    {
                        "view_name": jr["view_name"],
                        "sql_on": jr["sql_on"],
                        "join_type": jr["join_type"],
                        "relationship": jr["relationship"],
                        "is_pdt": jr.get("is_pdt", False),
                    }
                    for jr in join_records
                ]

                # Load fields for this explore
                field_records = session.run(
                    Q.GET_FIELDS_IN_EXPLORE,
                    explore_name=explore_name,
                    model_name=model_name,
                ).data()

                fields = []
                for fr in field_records:
                    field = LookMLField(
                        name=fr["name"],
                        view_name=fr["view_name"],
                        field_type=fr["field_type"],
                        data_type=fr.get("data_type", ""),
                        sql=fr.get("sql", ""),
                        label=fr.get("label", ""),
                        description=fr.get("description", ""),
                        tags=fr.get("tags", []) or [],
                        is_hidden=fr.get("is_hidden", False),
                        value_format=fr.get("value_format", ""),
                        explore_name=explore_name,
                        model_name=model_name,
                    )
                    fields.append(field)
                    new_all_fields.append(field)

                    # Build reverse index: field_id → [explore_names]
                    field_id = f"{field.view_name}.{field.name}"
                    if field_id not in new_field_to_explores:
                        new_field_to_explores[field_id] = []
                    if explore_name not in new_field_to_explores[field_id]:
                        new_field_to_explores[field_id].append(explore_name)

                new_explore_index[explore_name] = {
                    "explore": {
                        "name": explore_name,
                        "model_name": model_name,
                        "label": rec.get("label", ""),
                        "description": rec.get("description", ""),
                        "base_view": rec.get("base_view", ""),
                        "is_hidden": rec.get("is_hidden", False),
                        "tags": rec.get("tags", []) or [],
                    },
                    "fields": fields,
                    "joins": joins,
                    "base_view": rec.get("base_view", ""),
                    "always_filter": always_filter,
                    "model_name": model_name,
                }

        # ── Swap indexes atomically under the lock ────────────────
        with self._lock:
            self._explore_index = new_explore_index
            self._field_to_explores = new_field_to_explores
            self._all_fields = new_all_fields

        logger.info(
            "Cache rebuilt: %d explores, %d unique field IDs, %d total fields",
            len(new_explore_index),
            len(new_field_to_explores),
            len(new_all_fields),
        )

    def get_explore(self, explore_name: str) -> Optional[dict]:
        """
        Thread-safe lookup of a full explore context.

        Returns None if the explore is not in the cache.
        """
        with self._lock:
            return self._explore_index.get(explore_name)

    def get_explores_for_field(self, field_id: str) -> List[str]:
        """
        Which explores can serve this field?

        Args:
            field_id: "view_name.field_name" format

        Returns:
            List of explore names. Empty list if field not found.
        """
        with self._lock:
            return self._field_to_explores.get(field_id, [])

    def all_explore_names(self) -> List[str]:
        """All non-hidden explore names in the model."""
        with self._lock:
            return [
                name for name, ctx in self._explore_index.items()
                if not ctx["explore"].get("is_hidden", False)
            ]

    def all_fields(self) -> List[LookMLField]:
        """All non-hidden fields across all explores."""
        with self._lock:
            return [f for f in self._all_fields if not f.is_hidden]

    def get_field_in_explore(
        self, explore_name: str, view_name: str, field_name: str
    ) -> Optional[LookMLField]:
        """Look up a specific field in a specific explore."""
        with self._lock:
            ctx = self._explore_index.get(explore_name)
            if not ctx:
                return None
            for f in ctx["fields"]:
                if f.view_name == view_name and f.name == field_name:
                    return f
            return None

    def get_all_tags(self) -> List[str]:
        """Collect all unique tags across all fields — useful for prompts."""
        tags = set()
        with self._lock:
            for field in self._all_fields:
                tags.update(field.tags)
        return sorted(tags)

    @property
    def explore_count(self) -> int:
        with self._lock:
            return len(self._explore_index)

    @property
    def field_count(self) -> int:
        with self._lock:
            return len(self._all_fields)
