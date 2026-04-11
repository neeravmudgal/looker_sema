"""
Resolves LookML view inheritance (extends:) chains.

WHY: In LookML, a view can "extend" another view, inheriting all its fields.
     A child view can override parent fields by re-declaring them with the same name.
     This module resolves the full chain so downstream code only sees fully-merged views.

CALLED BY: lookml_parser.parse_directory() — called once after initial view parsing.
CALLS: Nothing external — pure data transformation.

EXAMPLE:
    view: session_purchase_facts { ... revenue measure ... }
    view: session_attribution { extends: [session_purchase_facts] ... ROI measure ... }

    After resolution, session_attribution has all fields from session_purchase_facts
    plus its own fields. If both define a field with the same name, the child wins.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Set

from src.parser.models import LookMLView, LookMLField

logger = logging.getLogger(__name__)


class CircularExtendsError(Exception):
    """Raised when view inheritance forms a cycle."""
    pass


def resolve_extends(views: Dict[str, LookMLView]) -> Dict[str, LookMLView]:
    """
    Resolve all extends: relationships across views.

    Algorithm:
    1. Build a dependency graph: child → [parents]
    2. Topological sort to detect cycles and determine resolution order
    3. For each view (in dependency order), merge parent fields into child
       - Child fields override parent fields with the same name
       - Parent sets are merged into child sets (child entries win on collision)

    Args:
        views: Dict mapping view name → LookMLView (with extends populated).

    Returns:
        The same dict, but with all views fully resolved (extends applied).

    Raises:
        CircularExtendsError: If a cycle is detected (A extends B extends A).
    """
    # ── Step 1: Identify which views have extends ─────────────────
    views_with_extends = {
        name: view for name, view in views.items() if view.extends
    }

    if not views_with_extends:
        return views  # Nothing to resolve

    logger.info("Resolving extends for %d views", len(views_with_extends))

    # ── Step 2: Topological sort ──────────────────────────────────
    resolution_order = _topological_sort(views_with_extends, views)

    # ── Step 3: Merge parent fields into children ─────────────────
    for view_name in resolution_order:
        view = views[view_name]
        for parent_name in view.extends:
            parent = views.get(parent_name)
            if not parent:
                logger.warning(
                    "View '%s' extends '%s' but parent view not found — skipping",
                    view_name, parent_name,
                )
                continue
            _merge_parent_into_child(parent, view)

    return views


def _topological_sort(
    views_with_extends: Dict[str, LookMLView],
    all_views: Dict[str, LookMLView],
) -> List[str]:
    """
    Topological sort of views by their extends dependencies.

    Returns a list of view names in the order they should be resolved
    (parents before children).

    Uses Kahn's algorithm (BFS-based) which naturally detects cycles:
    if any nodes remain after processing, there's a cycle.
    """
    # Build in-degree map for views that participate in extends
    in_degree: Dict[str, int] = {}
    dependents: Dict[str, List[str]] = {}  # parent → [children]

    # Initialize for all views that extend something
    for name, view in views_with_extends.items():
        in_degree.setdefault(name, 0)
        for parent_name in view.extends:
            in_degree.setdefault(parent_name, 0)
            dependents.setdefault(parent_name, []).append(name)
            in_degree[name] = in_degree.get(name, 0) + 1

    # Start with views that have no parents (in_degree == 0)
    queue: List[str] = [name for name, deg in in_degree.items() if deg == 0]
    result: List[str] = []

    while queue:
        current = queue.pop(0)
        result.append(current)
        for child in dependents.get(current, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # If not all nodes were processed, there's a cycle
    remaining = [name for name, deg in in_degree.items() if deg > 0]
    if remaining:
        # Build a readable cycle description
        cycle_desc = " → ".join(remaining)
        raise CircularExtendsError(
            f"Circular extends detected involving: {cycle_desc}. "
            f"Check your LookML extends: declarations."
        )

    # Only return views that actually have extends (those are the ones we need to merge)
    return [name for name in result if name in views_with_extends]


def _merge_parent_into_child(parent: LookMLView, child: LookMLView) -> None:
    """
    Merge a parent view's fields and sets into a child view.

    Rules:
    - Parent fields are added to the child ONLY if the child doesn't
      already have a field with the same name (child wins).
    - Parent sets are merged into child sets (child entries win on collision).
    - The child's sql_table_name is NOT inherited (Looker behavior).
    - The child's derived_table_sql is NOT inherited either.

    This mutates the child view in place.
    """
    # Build a set of child field names for fast lookup
    child_field_names: Set[str] = {f.name for f in child.fields}

    # Add parent fields that the child doesn't override
    for parent_field in parent.fields:
        if parent_field.name not in child_field_names:
            # Create a copy with the child's view name
            inherited = LookMLField(
                name=parent_field.name,
                field_type=parent_field.field_type,
                data_type=parent_field.data_type,
                sql=parent_field.sql,
                label=parent_field.label,
                description=parent_field.description,
                tags=parent_field.tags.copy(),
                is_hidden=parent_field.is_hidden,
                value_format=parent_field.value_format,
                timeframes=parent_field.timeframes.copy(),
                view_name=child.name,  # Field now belongs to the child view
            )
            child.fields.append(inherited)
            child_field_names.add(parent_field.name)

    # Merge sets: parent sets are inherited, child entries win on collision
    for set_name, set_fields in parent.sets.items():
        if set_name not in child.sets:
            child.sets[set_name] = set_fields.copy()

    logger.debug(
        "Merged %d fields from parent '%s' into child '%s' (child now has %d fields)",
        len(parent.fields), parent.name, child.name, len(child.fields),
    )
