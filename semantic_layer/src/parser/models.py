"""
Dataclass definitions for every LookML concept the system works with.

WHY: Every service in the pipeline (parser → graph → embeddings → retrieval →
     query generator) passes these objects around. Having a single, well-typed
     definition prevents each service from inventing its own dict schema.

CALLED BY: lookml_parser.py creates these, everything else reads them.

DESIGN NOTES:
  - All fields have defaults so partial construction works during parsing.
  - Tags are List[str] — the ambiguity detector scans these for attribution
    hints, KPI markers, etc.  Tags are generic; no specific tag names are
    hardcoded in this file.
  - explore_name on LookMLField is critical: the same physical field
    (e.g. users.country) generates one LookMLField object *per explore*
    it's accessible in. This is how the graph knows which explore can
    serve which field.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import Dict, List, Optional


@dataclass
class LookMLField:
    """
    One field (dimension, measure, filter, or parameter) inside one explore.

    If the same view-level field is accessible in 3 explores, there will be
    3 LookMLField objects — identical except for explore_name.
    """

    name: str = ""
    field_type: str = ""        # "dimension" | "measure" | "dimension_group" | "filter" | "parameter"
    data_type: str = ""         # "string" | "number" | "date" | "yesno" | "tier" | "location" | "zipcode"
    sql: str = ""               # Raw SQL expression, e.g. "${TABLE}.amount"
    label: str = ""             # Human-readable label shown in Looker UI
    description: str = ""       # Free-text description — key input for embeddings
    tags: List[str] = dataclass_field(default_factory=list)
    is_hidden: bool = False
    value_format: str = ""
    timeframes: List[str] = dataclass_field(default_factory=list)
    view_name: str = ""         # Which view this field belongs to
    explore_name: str = ""      # Which explore this field is accessible from
    model_name: str = ""        # Which model this explore lives in

    @property
    def fully_qualified_name(self) -> str:
        """view_name.field_name — the format Looker uses in API queries."""
        return f"{self.view_name}.{self.name}"

    @property
    def unique_id(self) -> str:
        """Globally unique within the graph: explore + view + field."""
        return f"{self.explore_name}::{self.view_name}.{self.name}"


@dataclass
class LookMLJoin:
    """
    A join relationship between an explore and a view.

    sql_on contains the join condition using ${view.field} references.
    relationship tells us about fanout risk (one_to_many = potential double-counting).
    fields may restrict which fields from the joined view are accessible.
    """

    view_name: str = ""
    sql_on: str = ""
    join_type: str = "left_outer"      # "left_outer" | "full_outer" | "inner" | "cross"
    relationship: str = "many_to_one"  # "many_to_one" | "one_to_many" | "one_to_one" | "many_to_many"
    fields: List[str] = dataclass_field(default_factory=list)  # Field set restrictions
    view_label: str = ""               # Optional override for the view's display name


@dataclass
class LookMLView:
    """
    A LookML view — either backed by a database table or a derived table (PDT).

    is_pdt is True when derived_table_sql is populated.
    extends lists parent view names this view inherits from.
    sets maps set names to lists of field names (used for field restrictions on joins).
    """

    name: str = ""
    sql_table_name: Optional[str] = None
    derived_table_sql: Optional[str] = None
    fields: List[LookMLField] = dataclass_field(default_factory=list)
    is_pdt: bool = False
    extends: List[str] = dataclass_field(default_factory=list)
    sets: Dict[str, List[str]] = dataclass_field(default_factory=dict)
    view_label: str = ""


@dataclass
class LookMLExplore:
    """
    A LookML explore — the top-level query context in Looker.

    An explore has one base_view and zero or more joins to other views.
    Fields are accessible from the base view + all joined views (subject
    to field set restrictions).

    always_filter: filters that MUST be included in every generated query.
    """

    name: str = ""
    label: str = ""
    model_name: str = ""
    description: str = ""
    base_view: str = ""
    joins: List[LookMLJoin] = dataclass_field(default_factory=list)
    tags: List[str] = dataclass_field(default_factory=list)
    is_hidden: bool = False
    always_filter: Dict[str, str] = dataclass_field(default_factory=dict)
    fields_spec: List[str] = dataclass_field(default_factory=list)  # Top-level fields: restriction


@dataclass
class LookMLModel:
    """
    A LookML model file — contains a connection and one or more explores.

    file_path stores the original .lkml file path for debugging and
    incremental refresh (we can detect when the file changes).
    """

    name: str = ""
    connection: str = ""
    explores: List[LookMLExplore] = dataclass_field(default_factory=list)
    file_path: str = ""
