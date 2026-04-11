"""
Builds the Neo4j property graph from parsed LookML objects.

WHY: The graph is the backbone of the retrieval system. It encodes:
     - Which fields belong to which views
     - Which views are joined in which explores
     - Which fields are directly accessible from each explore (CAN_ACCESS edges)
     This structure lets us answer "which explore can serve this set of fields?"
     with a single graph traversal instead of complex Python logic.

CALLED BY: Startup sequence in streamlit_app.py
CALLS: graph_queries.py for all Cypher, lookml_parser.get_accessible_fields()

PERFORMANCE: Uses UNWIND batch writes — all nodes/edges of each type are created
in a single Cypher statement, not one at a time in a Python loop. This makes
building a graph with thousands of fields take seconds, not minutes.
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List

from neo4j import Driver

from src.parser.models import (
    LookMLField,
    LookMLJoin,
    LookMLView,
    LookMLExplore,
    LookMLModel,
)
from src.parser.lookml_parser import get_accessible_fields
from src.graph import graph_queries as Q

logger = logging.getLogger(__name__)


def build_graph(
    models: List[LookMLModel],
    views: Dict[str, LookMLView],
    driver: Driver,
) -> Dict[str, int]:
    """
    Build the complete Neo4j graph from parsed LookML.

    Pipeline:
    1. Create Model nodes
    2. Create View nodes
    3. Create Explore nodes
    4. For each explore, determine accessible fields and create Field nodes
    5. Create all relationship edges (HAS_EXPLORE, BASE_VIEW, JOINS, etc.)
    6. Create CAN_ACCESS edges (the critical shortcut for retrieval)

    Args:
        models: Parsed LookML models from parse_directory()
        views:  Dict of view_name → LookMLView
        driver: Active Neo4j driver

    Returns:
        Dict of node/edge counts for diagnostics display.
    """
    stats = {
        "models": 0, "views": 0, "explores": 0, "fields": 0,
        "relationships": 0,
    }

    with driver.session() as session:
        # ── Step 1: Create Model nodes ────────────────────────────
        model_data = [
            {"name": m.name, "connection": m.connection, "file_path": m.file_path}
            for m in models
        ]
        if model_data:
            session.run(Q.CREATE_MODELS, models=model_data)
            stats["models"] = len(model_data)
            logger.info("Created %d Model nodes", len(model_data))

        # ── Step 2: Create View nodes ─────────────────────────────
        view_data = [
            {
                "name": v.name,
                "sql_table_name": v.sql_table_name,
                "derived_table_sql": v.derived_table_sql,
                "is_pdt": v.is_pdt,
                "view_label": v.view_label,
            }
            for v in views.values()
        ]
        if view_data:
            session.run(Q.CREATE_VIEWS, views=view_data)
            stats["views"] = len(view_data)
            logger.info("Created %d View nodes", len(view_data))

        # ── Step 3: Create Explore nodes ──────────────────────────
        explore_data = []
        for model in models:
            for exp in model.explores:
                explore_data.append({
                    "name": exp.name,
                    "model_name": exp.model_name,
                    "label": exp.label,
                    "description": exp.description,
                    "base_view": exp.base_view,
                    "is_hidden": exp.is_hidden,
                    "always_filter_json": json.dumps(exp.always_filter) if exp.always_filter else "{}",
                    "tags": exp.tags,
                })
        if explore_data:
            session.run(Q.CREATE_EXPLORES, explores=explore_data)
            stats["explores"] = len(explore_data)
            logger.info("Created %d Explore nodes", len(explore_data))

        # ── Step 4: For each explore, create Field nodes ──────────
        # We determine which fields are accessible in each explore,
        # then batch-create all Field nodes.
        all_fields_data = []
        all_can_access_links = []
        all_view_field_links = []

        for model in models:
            for exp in model.explores:
                accessible = get_accessible_fields(exp, views)

                for field in accessible:
                    field_dict = {
                        "name": field.name,
                        "view_name": field.view_name,
                        "explore_name": exp.name,
                        "field_type": field.field_type,
                        "data_type": field.data_type,
                        "sql": field.sql,
                        "label": field.label,
                        "description": field.description,
                        "tags": field.tags,
                        "is_hidden": field.is_hidden,
                        "value_format": field.value_format,
                        "timeframes": field.timeframes,
                        "model_name": model.name,
                    }
                    all_fields_data.append(field_dict)

                    # CAN_ACCESS link: explore → field
                    all_can_access_links.append({
                        "explore_name": exp.name,
                        "model_name": model.name,
                        "field_name": field.name,
                        "view_name": field.view_name,
                    })

                    # View → Field link
                    all_view_field_links.append({
                        "view_name": field.view_name,
                        "field_name": field.name,
                        "explore_name": exp.name,
                    })

        if all_fields_data:
            # Batch create in chunks to avoid massive single transactions
            chunk_size = 500
            for i in range(0, len(all_fields_data), chunk_size):
                chunk = all_fields_data[i:i + chunk_size]
                session.run(Q.CREATE_FIELDS, fields=chunk)
            stats["fields"] = len(all_fields_data)
            logger.info("Created %d Field nodes", len(all_fields_data))

        # ── Step 5: Create relationship edges ─────────────────────

        # Model → Explore
        model_explore_links = [
            {"model_name": m.name, "explore_name": e.name}
            for m in models
            for e in m.explores
        ]
        if model_explore_links:
            session.run(Q.LINK_MODEL_TO_EXPLORE, links=model_explore_links)

        # Explore → Base View
        base_view_links = [
            {
                "explore_name": e.name,
                "model_name": m.name,
                "view_name": e.base_view,
            }
            for m in models
            for e in m.explores
            if e.base_view in views
        ]
        if base_view_links:
            session.run(Q.LINK_EXPLORE_TO_BASE_VIEW, links=base_view_links)

        # Explore → Joined Views
        join_links = []
        for model in models:
            for exp in model.explores:
                for j in exp.joins:
                    if j.view_name in views:
                        join_links.append({
                            "explore_name": exp.name,
                            "model_name": model.name,
                            "view_name": j.view_name,
                            "sql_on": j.sql_on,
                            "join_type": j.join_type,
                            "relationship": j.relationship,
                        })
        if join_links:
            session.run(Q.LINK_EXPLORE_JOINS, joins=join_links)

        # View → Field
        if all_view_field_links:
            for i in range(0, len(all_view_field_links), chunk_size):
                chunk = all_view_field_links[i:i + chunk_size]
                session.run(Q.LINK_VIEW_TO_FIELD, links=chunk)

        # Explore → Field (CAN_ACCESS) — the critical shortcut
        if all_can_access_links:
            for i in range(0, len(all_can_access_links), chunk_size):
                chunk = all_can_access_links[i:i + chunk_size]
                session.run(Q.LINK_EXPLORE_CAN_ACCESS, links=chunk)

        # View → View (EXTENDS)
        extends_links = [
            {"child_name": v.name, "parent_name": parent}
            for v in views.values()
            for parent in v.extends
            if parent in views
        ]
        if extends_links:
            session.run(Q.LINK_VIEW_EXTENDS, links=extends_links)

        # Count total relationships
        rel_count = (
            len(model_explore_links) + len(base_view_links) + len(join_links)
            + len(all_view_field_links) + len(all_can_access_links) + len(extends_links)
        )
        stats["relationships"] = rel_count

    logger.info(
        "Graph build complete: %d models, %d views, %d explores, %d fields, %d relationships",
        stats["models"], stats["views"], stats["explores"],
        stats["fields"], stats["relationships"],
    )
    return stats


def get_graph_stats(driver: Driver) -> Dict[str, int]:
    """
    Query the graph for diagnostic node and relationship counts.

    Returns a dict like:
    {"Model": 1, "Explore": 3, "View": 13, "Field": 450, ...}
    """
    stats = {}
    with driver.session() as session:
        result = session.run(Q.COUNT_NODES)
        for record in result:
            stats[record["label"]] = record["count"]

        result = session.run(Q.COUNT_RELATIONSHIPS)
        for record in result:
            stats[f"rel_{record['rel_type']}"] = record["count"]

    return stats
