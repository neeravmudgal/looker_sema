"""
Text formatting strategies for embedding LookML objects.

WHY: Embedding quality depends entirely on the text we feed to the model.
     A raw field name like "created_date" tells the embedding model nothing.
     But "dimension named Created Date in view sessions inside explore events.
     Description: Timestamp when the session began. Data type: date."
     gives the model rich semantic context to match against user queries.

CALLED BY: embedder.py — calls format_field_text() and format_explore_text()
CALLS: Nothing external — pure string formatting.

GENERIC DESIGN: These functions work with any LookML field or explore.
No field names, view names, or model-specific logic is hardcoded.
"""

from __future__ import annotations

from src.parser.models import LookMLField, LookMLExplore


def format_field_text(field: LookMLField) -> str:
    """
    Build the embedding text for a single LookML field.

    We combine multiple signals into one string because the embedding model
    needs to capture:
    - What KIND of field this is (dimension vs measure)
    - Its human-readable label and description
    - Which view it belongs to (for context)
    - Its tags (so "revenue" queries match "kpi"-tagged fields)
    - Its SQL (so users asking about "sum" match SUM measures)
    - Its data type (so "date" queries match date fields)

    NOTE: We intentionally EXCLUDE explore_name from the embedding text.
    The same field (e.g. users.age) has identical semantics regardless of
    which explore it's accessed from. Including explore_name would create
    different embeddings for the same logical field, wasting top-k slots
    with duplicate results. The retriever maps fields to explores via the
    cache's reverse index — the embedding just needs to capture WHAT the
    field is, not WHERE it lives.

    Example output:
        "measure named Total Revenue in view order_items.
         Description: Sum of all order item sale prices.
         Tags: revenue, kpi.
         SQL: ${TABLE}.sale_price.
         Data type: number."
    """
    # Lead with the field name repeated — this anchors the embedding so
    # that a query for "revenue" strongly matches a field NAMED "revenue",
    # not just one that MENTIONS revenue in its description.
    name_words = field.name.replace("_", " ")
    label_words = field.label or name_words

    parts = [
        f"{label_words}.",
        f"{field.field_type} named {label_words}",
        f"in view {field.view_name}.",
    ]

    if field.description:
        parts.append(f"Description: {field.description}.")

    if field.tags:
        parts.append(f"Tags: {', '.join(field.tags)}.")

    if field.sql:
        # Truncate very long SQL (derived table references can be huge)
        sql_text = field.sql[:200]
        parts.append(f"SQL: {sql_text}.")

    if field.data_type:
        parts.append(f"Data type: {field.data_type}.")

    return " ".join(parts)


def format_view_text(view_name: str, fields: list, sql_table_name: str = "", view_label: str = "") -> str:
    """
    Build the embedding text for a LookML view.

    Views need to be findable by:
    - Their name and label (so "users" matches the users view)
    - The fields they contain (so "age, gender, country" matches users)
    - Their table name (so "ecomm.users" matches users)
    - The types of data they represent (dimensions vs measures)

    This helps the retriever identify which views are relevant when
    the user mentions concepts that span multiple fields in a view.

    Example output:
        "view users (ecomm.users).
         Dimensions: age, age_tier, gender, country, city, state, email.
         Measures: count, average_age, count_percent_of_total."
    """
    label = view_label or view_name
    parts = [f"view {label}"]
    if sql_table_name:
        parts.append(f"({sql_table_name})")
    parts.append(".")

    dims = [f.name.replace("_", " ") for f in fields
            if f.field_type == "dimension" and not f.is_hidden]
    measures = [f.name.replace("_", " ") for f in fields
                if f.field_type == "measure" and not f.is_hidden]

    if dims:
        parts.append(f"Dimensions: {', '.join(dims[:20])}.")
    if measures:
        parts.append(f"Measures: {', '.join(measures[:15])}.")

    # Add descriptions of key fields for richer semantic content
    described = [f for f in fields if f.description and not f.is_hidden][:5]
    if described:
        desc_parts = [f"{f.name}: {f.description[:60]}" for f in described]
        parts.append(f"Key fields: {'; '.join(desc_parts)}.")

    return " ".join(parts)


def format_explore_text(explore: dict) -> str:
    """
    Build the embedding text for a LookML explore.

    Explores need to be findable by:
    - Their name and label
    - Their description
    - The views they join (so "user data" matches an explore that joins users)

    Args:
        explore: A dict with keys: name, label, description, base_view,
                 join_view_names (list of joined view names).

    Example output:
        "explore Digital Ads - Event Data: base view events,
         joins sessions, users, adevents, campaigns.
         Explore website event-level data including page views and purchases."
    """
    parts = [f"explore {explore.get('label', explore.get('name', ''))}:"]

    base_view = explore.get("base_view", "")
    if base_view:
        parts.append(f"base view {base_view},")

    join_names = explore.get("join_view_names", [])
    if join_names:
        parts.append(f"joins {', '.join(join_names)}.")
    else:
        parts.append("no joins.")

    description = explore.get("description", "")
    if description:
        parts.append(description)

    return " ".join(parts)
