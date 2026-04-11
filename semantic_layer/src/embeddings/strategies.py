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
    - Which view and explore it belongs to (for context)
    - Its tags (so "revenue" queries match "kpi"-tagged fields)
    - Its SQL (so users asking about "sum" match SUM measures)
    - Its data type (so "date" queries match date fields)

    Example output:
        "measure named Total Revenue in view order_items
         inside explore orders.
         Description: Sum of all order item sale prices.
         Tags: revenue, kpi.
         SQL: ${TABLE}.sale_price.
         Data type: number."
    """
    parts = [
        f"{field.field_type} named {field.label}",
        f"in view {field.view_name}",
    ]

    if field.explore_name:
        parts.append(f"inside explore {field.explore_name}")

    parts.append(".")

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
