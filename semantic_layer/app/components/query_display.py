"""
Query display component — renders generated Looker queries with formatting.

Provides syntax-highlighted JSON display, copy-to-clipboard, and a
"Run in Looker" link builder.
"""

from __future__ import annotations

import json
import urllib.parse
import streamlit as st


def render_query(query: dict, explore_name: str = "", model_name: str = "") -> None:
    """
    Render a Looker query with syntax highlighting and action buttons.

    Args:
        query: The Looker API-compatible query dict.
        explore_name: Name of the explore (for the Looker URL).
        model_name: Name of the model (for the Looker URL).
    """
    # Clean internal keys
    display_query = {
        k: v for k, v in query.items()
        if not k.startswith("_")
    }

    # Syntax-highlighted JSON
    st.code(json.dumps(display_query, indent=2), language="json")

    # Action buttons
    col1, col2 = st.columns(2)

    with col1:
        # Copy button — Streamlit doesn't have native clipboard,
        # so we use a download button as a workaround
        st.download_button(
            "Download Query JSON",
            data=json.dumps(display_query, indent=2),
            file_name="looker_query.json",
            mime="application/json",
            use_container_width=True,
        )

    with col2:
        # Build Looker URL if we have explore info
        if explore_name and model_name:
            fields = query.get("fields", [])
            fields_param = ",".join(fields)
            looker_url = (
                f"/explore/{model_name}/{explore_name}"
                f"?fields={urllib.parse.quote(fields_param)}"
            )
            st.markdown(
                f"[Open in Looker]({looker_url})",
                help="Opens this query in your Looker instance",
            )


def format_query_summary(query: dict) -> str:
    """Build a one-line summary of a query for display."""
    explore = query.get("explore", "?")
    field_count = len(query.get("fields", []))
    filter_count = len(query.get("filters", {}))
    limit = query.get("limit", "?")

    return f"{explore} | {field_count} fields | {filter_count} filters | limit {limit}"
