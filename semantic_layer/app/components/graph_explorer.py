"""
Graph explorer component — browse the indexed LookML graph visually.

Lets users explore what data is available:
  - Select an explore from a dropdown
  - See all fields grouped by view
  - Filter by field type (dimension/measure) and tags
  - Click a field to see full details
"""

from __future__ import annotations

import streamlit as st
from src.graph.cache import ExploreContextCache


def render_graph_explorer(cache: ExploreContextCache) -> None:
    """
    Render the graph explorer panel in the sidebar.

    WHY: Users need a way to browse what data is available before asking
    questions. This component lets them drill into explores, filter by
    field type, and inspect individual fields — building confidence in
    what the semantic layer knows about.

    Args:
        cache: The in-memory ExploreContextCache holding all parsed
            LookML explores, views, fields, and join metadata.

    Side effects:
        Renders Streamlit widgets (selectbox, expanders, markdown) into
        the current sidebar context. Reads from st.session_state keys
        explorer_explore and explorer_field_type.
    """
    st.subheader("Graph Explorer")

    if not cache:
        st.info("Cache not initialized yet.")
        return

    explore_names = cache.all_explore_names()
    if not explore_names:
        st.info("No explores found in the model.")
        return

    # ── Explore selector ──────────────────────────────────────────
    selected_explore = st.selectbox(
        "Browse Explore",
        options=explore_names,
        key="explorer_explore",
    )

    if not selected_explore:
        return

    ctx = cache.get_explore(selected_explore)
    if not ctx:
        return

    # ── Explore info ──────────────────────────────────────────────
    explore_info = ctx.get("explore", {})
    if explore_info.get("description"):
        st.caption(explore_info["description"][:200])

    # ── Field type filter ─────────────────────────────────────────
    fields = ctx.get("fields", [])
    field_types = sorted({f.field_type for f in fields})
    selected_type = st.selectbox(
        "Field Type",
        options=["All"] + field_types,
        key="explorer_field_type",
    )

    if selected_type != "All":
        fields = [f for f in fields if f.field_type == selected_type]

    # ── Group by view ─────────────────────────────────────────────
    views = {}
    for f in fields:
        if f.is_hidden:
            continue
        views.setdefault(f.view_name, []).append(f)

    st.caption(f"{len(fields)} fields across {len(views)} views")

    for view_name in sorted(views.keys()):
        view_fields = views[view_name]
        with st.expander(f"{view_name} ({len(view_fields)} fields)"):
            for f in sorted(view_fields, key=lambda x: x.name):
                icon = "📊" if f.field_type == "measure" else "📐"
                st.markdown(f"{icon} **{f.name}** ({f.data_type})")
                if f.description:
                    st.caption(f.description[:150])

    # ── Joins info ────────────────────────────────────────────────
    joins = ctx.get("joins", [])
    if joins:
        with st.expander(f"Joins ({len(joins)})"):
            for j in joins:
                pdt_tag = " [PDT]" if j.get("is_pdt") else ""
                st.markdown(
                    f"**{j['view_name']}**{pdt_tag} "
                    f"({j.get('relationship', '')})"
                )
