"""
LookML file parser — reads .lkml files and produces normalized Python objects.

WHY: LookML has complex structure (dimension_groups expand into multiple fields,
     views can extend other views, joins restrict accessible fields via field sets).
     This parser handles all that complexity once, so every downstream service
     gets clean, normalized LookMLField / LookMLExplore / LookMLModel objects.

CALLED BY: Startup sequence in streamlit_app.py → parse_directory()
CALLS: lkml library for raw parsing, inheritance_resolver for extends chains.

GENERIC DESIGN: This parser works with ANY valid LookML — no field names,
explore names, or model-specific logic is hardcoded.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import lkml

from src.parser.models import (
    LookMLField,
    LookMLJoin,
    LookMLView,
    LookMLExplore,
    LookMLModel,
)
from src.parser.inheritance_resolver import resolve_extends

logger = logging.getLogger(__name__)


class ParseError(Exception):
    """Raised when LookML parsing encounters an unrecoverable problem.

    WHY: Callers need a specific exception type to distinguish parse failures
    (e.g. missing directory, circular extends) from generic Python errors so
    they can present user-friendly diagnostics.
    """
    pass


# ── Public API ────────────────────────────────────────────────────────────


def parse_directory(lookml_dir: str) -> Tuple[List[LookMLModel], Dict[str, LookMLView]]:
    """
    Parse all .lkml files in a directory tree.

    Returns:
        models:   List of LookMLModel objects with fully resolved explores.
        views:    Dict mapping view name → LookMLView with all fields.

    The parsing pipeline is:
      1. Find all .lkml files recursively
      2. Parse each file with the lkml library
      3. Extract views and resolve inheritance (extends)
      4. Extract models and their explores
      5. Expand dimension_groups into individual field objects
      6. For each explore, determine which fields are accessible
         (base view fields + joined view fields, subject to field set restrictions)

    Raises:
        ParseError: If a file cannot be parsed or has circular extends.
    """
    lookml_path = Path(lookml_dir)
    if not lookml_path.exists():
        raise ParseError(f"LookML directory not found: {lookml_dir}")

    # ── Step 1: Find all .lkml files ──────────────────────────────────
    lkml_files = sorted(lookml_path.rglob("*.lkml"))
    if not lkml_files:
        raise ParseError(f"No .lkml files found in {lookml_dir}")

    logger.info("Found %d .lkml files in %s", len(lkml_files), lookml_dir)

    # ── Step 2: Parse each file ───────────────────────────────────────
    raw_views: Dict[str, dict] = {}        # view_name → raw lkml dict
    raw_models: List[dict] = []            # list of raw model-level dicts
    raw_model_paths: List[str] = []        # parallel list of file paths
    raw_view_sources: Dict[str, str] = {}  # view_name → source file path

    for fpath in lkml_files:
        try:
            text = fpath.read_text(encoding="utf-8")
            parsed = lkml.load(text)
        except Exception as exc:
            logger.warning("Skipping unparseable file %s: %s", fpath, exc)
            continue

        # Collect views from this file
        for v in parsed.get("views", []):
            vname = v.get("name", "")
            if vname:
                raw_views[vname] = v
                raw_view_sources[vname] = str(fpath)

        # If the file has explores or a connection, treat it as a model file
        if parsed.get("explores") or parsed.get("connection"):
            raw_models.append(parsed)
            raw_model_paths.append(str(fpath))

    logger.info("Parsed %d views and %d model files", len(raw_views), len(raw_models))

    # ── Step 3: Build LookMLView objects and resolve extends ──────────
    views = _build_views(raw_views)
    views = resolve_extends(views)

    # ── Step 4: Expand dimension_groups into individual fields ────────
    for view in views.values():
        view.fields = _expand_dimension_groups(view.fields)

    # ── Step 5: Build models with resolved explores ───────────────────
    models = []
    for raw_model, model_path in zip(raw_models, raw_model_paths):
        model = _build_model(raw_model, model_path, views)
        models.append(model)

    # ── Summary stats ─────────────────────────────────────────────────
    total_fields = sum(len(v.fields) for v in views.values())
    total_explores = sum(len(m.explores) for m in models)
    logger.info(
        "Parse complete: %d models, %d explores, %d views, %d fields",
        len(models), total_explores, len(views), total_fields,
    )

    return models, views


# ── Internal helpers ──────────────────────────────────────────────────────


def _build_views(raw_views: Dict[str, dict]) -> Dict[str, LookMLView]:
    """
    Convert raw lkml view dicts into LookMLView objects.

    Handles:
    - sql_table_name extraction
    - derived_table SQL extraction (marks view as PDT)
    - extends chain recording (resolved in a later step)
    - Field set definitions (sets: { set_name: [field, ...] })
    - Individual field parsing for dimensions, measures, filters, parameters
    """
    views: Dict[str, LookMLView] = {}

    for vname, raw in raw_views.items():
        view = LookMLView(name=vname)

        # Table name or derived table
        view.sql_table_name = raw.get("sql_table_name")
        derived = raw.get("derived_table")
        if derived:
            view.derived_table_sql = derived.get("sql", "")
            view.is_pdt = True
            # Also capture explore_source-based PDTs
            if not view.derived_table_sql and derived.get("explore_source"):
                view.derived_table_sql = f"explore_source: {derived['explore_source']}"
                view.is_pdt = True

        # Extends
        extends_val = raw.get("extends") or raw.get("extends__all")
        if extends_val:
            if isinstance(extends_val, list):
                # lkml may nest this as [[name]] or [name]
                flat = []
                for item in extends_val:
                    if isinstance(item, list):
                        flat.extend(item)
                    else:
                        flat.append(item)
                view.extends = flat
            elif isinstance(extends_val, str):
                view.extends = [extends_val]

        # View label
        view.view_label = raw.get("view_label", "")

        # Field sets (used for join field restrictions)
        for s in raw.get("sets", []):
            set_name = s.get("name", "")
            set_fields = s.get("fields", [])
            if set_name:
                view.sets[set_name] = set_fields

        # Parse all field types
        view.fields = []
        for field_type_key in ["dimensions", "measures", "filters", "parameters", "dimension_groups"]:
            for raw_field in raw.get(field_type_key, []):
                field = _parse_field(raw_field, field_type_key, vname)
                if field:
                    view.fields.append(field)

        views[vname] = view

    return views


def _parse_field(raw: dict, field_type_key: str, view_name: str) -> Optional[LookMLField]:
    """Parse a single raw field dict from lkml into a LookMLField object.

    WHY: The raw lkml library output uses plural keys ("dimensions", "measures")
    and LookML-specific boolean strings. This function normalizes everything
    into a clean LookMLField with Python-native types.

    Args:
        raw: A single field dict as returned by lkml.load().
        field_type_key: The lkml dict key this field came from -- one of
            "dimensions", "measures", "dimension_groups", "filters", "parameters".
        view_name: Name of the view this field belongs to.

    Returns:
        A LookMLField, or None if the raw dict has no name.
    """
    name = raw.get("name", "")
    if not name:
        return None

    # Map lkml plural key to our singular field_type
    type_map = {
        "dimensions": "dimension",
        "measures": "measure",
        "dimension_groups": "dimension_group",
        "filters": "filter",
        "parameters": "parameter",
    }
    field_type = type_map.get(field_type_key, field_type_key)

    # Data type — lkml stores this as "type"
    data_type = raw.get("type", "string")

    # Tags
    tags = raw.get("tags", [])
    if isinstance(tags, str):
        tags = [tags]

    # Timeframes for dimension_groups
    timeframes = raw.get("timeframes", [])

    # SQL expression
    sql = raw.get("sql", "")

    # Label (default to title-cased name)
    label = raw.get("label", name.replace("_", " ").title())

    # Description
    description = raw.get("description", "")

    # Hidden
    is_hidden = raw.get("hidden", "no")
    if isinstance(is_hidden, str):
        is_hidden = is_hidden.lower() in ("yes", "true")

    # Value format
    value_format = raw.get("value_format_name", "") or raw.get("value_format", "")

    return LookMLField(
        name=name,
        field_type=field_type,
        data_type=data_type,
        sql=sql,
        label=label,
        description=description,
        tags=tags,
        is_hidden=is_hidden,
        value_format=value_format,
        timeframes=timeframes,
        view_name=view_name,
        # explore_name and model_name are set later when we assign fields to explores
    )


def _expand_dimension_groups(fields: List[LookMLField]) -> List[LookMLField]:
    """
    Expand dimension_group fields into one LookMLField per timeframe.

    A LookML dimension_group like:
        dimension_group: created {
            type: time
            timeframes: [date, week, month, quarter, year]
        }

    Generates 5 separate fields:
        created_date, created_week, created_month, created_quarter, created_year

    Each expanded field is a regular "dimension" with data_type set to the
    timeframe name (e.g. "date", "week"). The original dimension_group is
    also kept (with field_type "dimension_group") for metadata purposes.

    Non-time dimension_groups (type: duration) generate _hours, _minutes, etc.
    """
    expanded: List[LookMLField] = []

    for field in fields:
        if field.field_type != "dimension_group":
            expanded.append(field)
            continue

        # Keep the original dimension_group for metadata
        expanded.append(field)

        # Default timeframes if none specified
        timeframes = field.timeframes
        if not timeframes:
            if field.data_type == "duration":
                timeframes = ["hours", "minutes", "days"]
            else:
                timeframes = ["raw", "time", "date", "week", "month", "quarter", "year"]

        # Create one dimension per timeframe
        for tf in timeframes:
            tf_name = tf.strip()
            if not tf_name:
                continue

            # Looker naming convention: {group_name}_{timeframe}
            expanded_name = f"{field.name}_{tf_name}"

            expanded_field = LookMLField(
                name=expanded_name,
                field_type="dimension",
                data_type=tf_name,  # "date", "week", "month", etc.
                sql=field.sql,
                label=f"{field.label} {tf_name.replace('_', ' ').title()}",
                description=field.description or f"{field.label} at {tf_name} granularity",
                tags=field.tags.copy(),
                is_hidden=field.is_hidden,
                value_format=field.value_format,
                timeframes=[],  # Expanded fields don't have timeframes themselves
                view_name=field.view_name,
                explore_name=field.explore_name,
                model_name=field.model_name,
            )
            expanded.append(expanded_field)

    return expanded


def _build_model(
    raw_model: dict,
    file_path: str,
    views: Dict[str, LookMLView],
) -> LookMLModel:
    """
    Build a LookMLModel from a raw lkml model dict.

    Derives the model name from the file path (e.g. "thelook_adwords" from
    "thelook_adwords.model.lkml").

    For each explore, resolves:
    - base_view (defaults to explore name if not specified)
    - all joins with their sql_on, relationship, and field set restrictions
    - always_filter / conditionally_filter
    - hidden status
    """
    # Derive model name from filename
    model_name = Path(file_path).stem
    if model_name.endswith(".model"):
        model_name = model_name[: -len(".model")]

    connection = raw_model.get("connection", "")

    model = LookMLModel(
        name=model_name,
        connection=connection,
        file_path=file_path,
    )

    for raw_explore in raw_model.get("explores", []):
        explore = _build_explore(raw_explore, model_name, views)
        if explore:
            model.explores.append(explore)

    return model


def _build_explore(
    raw: dict,
    model_name: str,
    views: Dict[str, LookMLView],
) -> Optional[LookMLExplore]:
    """
    Build a LookMLExplore from a raw lkml explore dict.

    Key logic:
    - base_view defaults to explore name (Looker convention)
    - Joins are parsed with sql_on, relationship, and optional field restrictions
    - Field restrictions reference view-level sets; we resolve set members here
    """
    name = raw.get("name", "")
    if not name:
        return None

    # Base view: explicit from_view or defaults to explore name
    base_view = raw.get("from", name)
    if raw.get("view_name"):
        base_view = raw["view_name"]

    explore = LookMLExplore(
        name=name,
        label=raw.get("label", name.replace("_", " ").title()),
        model_name=model_name,
        description=raw.get("description", ""),
        base_view=base_view,
        is_hidden=_parse_bool(raw.get("hidden", "no")),
        tags=raw.get("tags", []),
    )

    # Top-level fields restriction on the explore itself
    fields_spec = raw.get("fields")
    if fields_spec:
        if isinstance(fields_spec, list):
            explore.fields_spec = fields_spec
        elif isinstance(fields_spec, str):
            explore.fields_spec = [fields_spec]

    # Always filter
    for af in raw.get("always_filter", {}).get("filters", []):
        dim = af.get("field", af.get("dimension", ""))
        val = af.get("value", "")
        if dim:
            explore.always_filter[dim] = val

    # Conditionally filter — treated as always_filter for our purposes
    for cf in raw.get("conditionally_filter", {}).get("filters", []):
        dim = cf.get("field", cf.get("dimension", ""))
        val = cf.get("value", "")
        if dim:
            explore.always_filter[dim] = val

    # Joins
    for raw_join in raw.get("joins", []):
        join = _build_join(raw_join, views)
        if join:
            explore.joins.append(join)

    return explore


def _build_join(raw: dict, views: Dict[str, LookMLView]) -> Optional[LookMLJoin]:
    """
    Build a LookMLJoin from a raw lkml join dict.

    Handles:
    - from: (alias a different view)
    - fields: [set_name*] (restrict which fields are accessible)
    - sql_on, type, relationship
    """
    name = raw.get("name", "")
    if not name:
        return None

    # The join can alias a different view with "from:"
    view_name = raw.get("from", name)

    join = LookMLJoin(
        view_name=view_name,
        sql_on=raw.get("sql_on", ""),
        join_type=raw.get("type", "left_outer"),
        relationship=raw.get("relationship", "many_to_one"),
        view_label=raw.get("view_label", ""),
    )

    # Field restrictions: e.g. fields: [user_facts*]
    raw_fields = raw.get("fields")
    if raw_fields:
        if isinstance(raw_fields, str):
            raw_fields = [raw_fields]
        join.fields = raw_fields

    return join


def get_accessible_fields(
    explore: LookMLExplore,
    views: Dict[str, LookMLView],
) -> List[LookMLField]:
    """Determine all fields accessible from a given explore.

    WHY: Not every field defined in a view is queryable from every explore.
    Joins can restrict visible fields via named sets, and explores can have
    top-level field restrictions. This function is the single source of truth
    for field visibility.

    WHAT: Resolves field visibility by:
    1. Starting with base_view fields (all visible unless explore-level
       fields: restricts them).
    2. For each join, adding that view's fields -- but if the join has a
       fields: restriction (e.g. [user_facts*]), only including fields
       that are members of that named set.
    3. Setting explore_name and model_name on each emitted field.

    Args:
        explore: The explore whose accessible fields we want to resolve.
        views: Dict mapping view name to LookMLView (fully resolved).

    Returns:
        A new list of LookMLField copies with explore_name and model_name
        populated. Does not mutate the original view fields.
    """
    accessible: List[LookMLField] = []

    # ── Base view fields ──────────────────────────────────────────
    base_view = views.get(explore.base_view)
    if base_view:
        for f in base_view.fields:
            field_copy = _copy_field_for_explore(f, explore)
            accessible.append(field_copy)

    # ── Joined view fields ────────────────────────────────────────
    for join in explore.joins:
        joined_view = views.get(join.view_name)
        if not joined_view:
            logger.warning(
                "Explore '%s' joins view '%s' but that view was not found",
                explore.name, join.view_name,
            )
            continue

        # Determine which fields are accessible from this join
        allowed_field_names = _resolve_field_restriction(join, joined_view)

        for f in joined_view.fields:
            # If there's a field restriction, only include allowed fields
            if allowed_field_names is not None:
                if not _field_matches_restriction(f.name, allowed_field_names, joined_view):
                    continue

            field_copy = _copy_field_for_explore(f, explore)
            accessible.append(field_copy)

    return accessible


def _copy_field_for_explore(field: LookMLField, explore: LookMLExplore) -> LookMLField:
    """Create a copy of a field stamped with an explore's identity.

    WHY: The same physical field appears in multiple explores, but each
    occurrence needs its own explore_name and model_name so the knowledge
    graph can distinguish them. Copying avoids mutating the shared view-level
    field objects.

    Args:
        field: The source field from a view's field list.
        explore: The explore this field copy will belong to.

    Returns:
        A new LookMLField with explore_name and model_name populated.
    """
    return LookMLField(
        name=field.name,
        field_type=field.field_type,
        data_type=field.data_type,
        sql=field.sql,
        label=field.label,
        description=field.description,
        tags=field.tags.copy(),
        is_hidden=field.is_hidden,
        value_format=field.value_format,
        timeframes=field.timeframes.copy(),
        view_name=field.view_name,
        explore_name=explore.name,
        model_name=explore.model_name,
    )


def _resolve_field_restriction(
    join: LookMLJoin,
    view: LookMLView,
) -> Optional[List[str]]:
    """
    Resolve a join's field restriction to a flat list of field names.

    If join.fields is empty → None (all fields accessible).
    If join.fields contains "set_name*" → expand by looking up the set in the view.
    If join.fields contains plain field names → use directly.
    Also handles "ALL_FIELDS*" which means no restriction.

    Returns None if no restriction applies.
    """
    if not join.fields:
        return None

    allowed: List[str] = []
    for spec in join.fields:
        spec = spec.strip()

        # ALL_FIELDS* means no restriction
        if spec.upper().startswith("ALL_FIELDS"):
            return None

        # set_name* → expand from view.sets
        if spec.endswith("*"):
            set_name = spec[:-1]  # Remove trailing *
            if set_name in view.sets:
                allowed.extend(view.sets[set_name])
            else:
                # Maybe it's a pattern — log and skip
                logger.debug(
                    "Field set '%s' not found in view '%s', treating as pattern",
                    set_name, view.name,
                )
        # Negation: -view.field_set* means exclude
        elif spec.startswith("-"):
            # Exclusions are complex — for now, we don't restrict
            # (the excluded fields are handled at the explore level)
            return None
        else:
            allowed.append(spec)

    return allowed if allowed else None


def _field_matches_restriction(
    field_name: str,
    allowed: List[str],
    view: LookMLView,
) -> bool:
    """Check if a field name passes a join's field-set restriction.

    WHY: Expanded dimension_group fields (e.g. "created_date") are not
    listed by name in field sets -- only the group name ("created") appears.
    This function handles the prefix-matching convention so expanded fields
    are correctly included.

    Args:
        field_name: The name of the field to check.
        allowed: List of explicitly allowed field names (from set expansion).
        view: The view the field belongs to (unused currently, reserved for
            future pattern matching).

    Returns:
        True if the field should be included, False otherwise.
    """
    if field_name in allowed:
        return True

    # Check if this is an expanded dimension_group field
    # e.g. "created_date" should match if "created" is in the set
    for allowed_name in allowed:
        if field_name.startswith(f"{allowed_name}_"):
            return True

    return False


def _parse_bool(value: Any) -> bool:
    """Parse a LookML boolean value into a Python bool.

    WHY: LookML uses "yes"/"no" strings rather than true/false. This helper
    normalizes all variants so callers can use standard Python booleans.

    Args:
        value: A bool, string ("yes", "no", "true", "false", "1"), or other type.

    Returns:
        True if the value represents a truthy LookML value, False otherwise.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("yes", "true", "1")
    return False
