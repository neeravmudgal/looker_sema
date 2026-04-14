"""
Neo4j schema definitions — indexes, constraints, and vector indexes.

WHY: Constraints enforce data integrity (no duplicate explores or fields).
     Indexes speed up the Cypher queries in graph_queries.py.
     Vector indexes enable native ANN (approximate nearest neighbor) search
     inside Neo4j, so we don't need a separate vector database.

CALLED BY: Startup sequence (streamlit_app.py) — runs once, idempotent.
CALLS: Neo4j driver to execute DDL statements.

ALL STATEMENTS USE 'IF NOT EXISTS' — safe to run multiple times.
"""

from __future__ import annotations

import logging
from typing import List

from neo4j import Driver

from src.config import settings

logger = logging.getLogger(__name__)

# ── Schema DDL Statements ─────────────────────────────────────────────
# Each statement is idempotent (IF NOT EXISTS). Order matters only for
# constraints that must exist before indexes that reference them.

CONSTRAINTS: List[str] = [
    # Model names must be unique
    "CREATE CONSTRAINT model_name_unique IF NOT EXISTS "
    "FOR (m:Model) REQUIRE m.name IS UNIQUE",

    # Explore uniqueness is (name, model_name) — an explore name can appear
    # in different models, but not twice in the same model
    "CREATE CONSTRAINT explore_unique IF NOT EXISTS "
    "FOR (e:Explore) REQUIRE (e.name, e.model_name) IS UNIQUE",

    # View names must be unique (across the whole graph — LookML enforces this)
    "CREATE CONSTRAINT view_name_unique IF NOT EXISTS "
    "FOR (v:View) REQUIRE v.name IS UNIQUE",
]

INDEXES: List[str] = [
    # Speed up field lookups by type (dimension vs measure filtering)
    "CREATE INDEX field_type_idx IF NOT EXISTS FOR (f:Field) ON (f.field_type)",

    # Speed up field lookups by view name
    "CREATE INDEX field_view_idx IF NOT EXISTS FOR (f:Field) ON (f.view_name)",

    # Speed up field lookups by explore name
    "CREATE INDEX field_explore_idx IF NOT EXISTS FOR (f:Field) ON (f.explore_name)",

    # Speed up explore lookups by name
    "CREATE INDEX explore_name_idx IF NOT EXISTS FOR (e:Explore) ON (e.name)",

    # Speed up field lookups by hidden status (retrieval filters these out)
    "CREATE INDEX field_hidden_idx IF NOT EXISTS FOR (f:Field) ON (f.is_hidden)",
]

# Full-text indexes for hybrid retrieval (exact keyword + vector semantic)
FULLTEXT_INDEXES: List[str] = [
    # Hybrid search: find fields by exact name/label/description keywords
    # Complements vector search which finds semantic matches
    "CREATE FULLTEXT INDEX field_fulltext IF NOT EXISTS "
    "FOR (f:Field) ON EACH [f.name, f.label, f.description]",

    # Search explores by name/description
    "CREATE FULLTEXT INDEX explore_fulltext IF NOT EXISTS "
    "FOR (e:Explore) ON EACH [e.name, e.label, e.description]",

    # Search views by name
    "CREATE FULLTEXT INDEX view_fulltext IF NOT EXISTS "
    "FOR (v:View) ON EACH [v.name, v.view_label]",
]

def _vector_indexes() -> List[str]:
    """
    Build vector index DDL at runtime so it reads the current settings.

    This is a function (not a module-level constant) because
    settings.embedding_dimensions must reflect the actual .env value,
    which may not be loaded at import time.

    nomic-embed-text = 768 dims, text-embedding-3-small = 1536 dims.
    """
    dims = settings.embedding_dimensions
    return [
        f"""CREATE VECTOR INDEX field_embeddings IF NOT EXISTS
        FOR (f:Field) ON (f.embedding)
        OPTIONS {{indexConfig: {{
          `vector.dimensions`: {dims},
          `vector.similarity_function`: 'cosine'
        }}}}""",

        f"""CREATE VECTOR INDEX explore_embeddings IF NOT EXISTS
        FOR (e:Explore) ON (e.embedding)
        OPTIONS {{indexConfig: {{
          `vector.dimensions`: {dims},
          `vector.similarity_function`: 'cosine'
        }}}}""",

        f"""CREATE VECTOR INDEX view_embeddings IF NOT EXISTS
        FOR (v:View) ON (v.embedding)
        OPTIONS {{indexConfig: {{
          `vector.dimensions`: {dims},
          `vector.similarity_function`: 'cosine'
        }}}}""",
    ]


def create_schema(driver: Driver) -> None:
    """
    Create all Neo4j constraints, indexes, and vector indexes.

    This is idempotent — safe to call on every startup.
    Failures on individual statements are logged but don't stop the process,
    because some Neo4j editions don't support all index types.

    Args:
        driver: Active Neo4j driver connection.
    """
    logger.info("Creating Neo4j schema (constraints, indexes, vector indexes)...")

    with driver.session() as session:
        # Constraints first (indexes may depend on them)
        for stmt in CONSTRAINTS:
            _execute_ddl(session, stmt, "constraint")

        for stmt in INDEXES:
            _execute_ddl(session, stmt, "index")

        for stmt in FULLTEXT_INDEXES:
            _execute_ddl(session, stmt, "fulltext index")

        for stmt in _vector_indexes():
            _execute_ddl(session, stmt, "vector index")

    logger.info("Schema creation complete")


def _execute_ddl(session, statement: str, kind: str) -> None:
    """Execute a single DDL statement, logging success or failure."""
    try:
        session.run(statement)
        logger.debug("Created %s: %s", kind, statement[:80])
    except Exception as exc:
        # Common: vector indexes not supported in Community Edition
        logger.warning("Could not create %s: %s — %s", kind, statement[:60], exc)


def drop_all_data(driver: Driver) -> None:
    """
    Delete all nodes and relationships. Used for rebuilding the graph.

    WARNING: This is destructive! Only call during startup when re-indexing.
    """
    logger.warning("Dropping all nodes and relationships from Neo4j")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    logger.info("All data dropped")
