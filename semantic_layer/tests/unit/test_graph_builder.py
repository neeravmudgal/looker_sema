"""
Unit tests for the graph builder module.

Tests that parsed LookML objects are correctly converted into
Neo4j-compatible data structures. Does NOT require a running Neo4j
instance — tests the data preparation logic only.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.parser.models import LookMLField, LookMLView, LookMLExplore, LookMLModel, LookMLJoin
from src.parser.lookml_parser import get_accessible_fields


class TestAccessibleFieldsLogic:
    """Test the field accessibility logic that graph_builder relies on."""

    def test_empty_explore_returns_base_fields_only(self):
        base_view = LookMLView(
            name="events",
            fields=[
                LookMLField(name="id", field_type="dimension", view_name="events"),
                LookMLField(name="count", field_type="measure", view_name="events"),
            ],
        )
        explore = LookMLExplore(name="events", base_view="events", model_name="test")
        views = {"events": base_view}

        fields = get_accessible_fields(explore, views)
        assert len(fields) == 2
        assert all(f.explore_name == "events" for f in fields)

    def test_joined_view_fields_are_included(self):
        events = LookMLView(
            name="events",
            fields=[LookMLField(name="id", view_name="events")],
        )
        users = LookMLView(
            name="users",
            fields=[
                LookMLField(name="country", view_name="users"),
                LookMLField(name="age", view_name="users"),
            ],
        )
        explore = LookMLExplore(
            name="events",
            base_view="events",
            model_name="test",
            joins=[LookMLJoin(view_name="users")],
        )
        views = {"events": events, "users": users}

        fields = get_accessible_fields(explore, views)
        field_names = {f.name for f in fields}
        assert "id" in field_names
        assert "country" in field_names
        assert "age" in field_names

    def test_missing_joined_view_handled_gracefully(self):
        events = LookMLView(
            name="events",
            fields=[LookMLField(name="id", view_name="events")],
        )
        explore = LookMLExplore(
            name="events",
            base_view="events",
            model_name="test",
            joins=[LookMLJoin(view_name="nonexistent_view")],
        )
        views = {"events": events}

        # Should not crash — just skip the missing view
        fields = get_accessible_fields(explore, views)
        assert len(fields) == 1

    def test_field_set_restriction_filters_fields(self):
        users = LookMLView(
            name="users",
            fields=[
                LookMLField(name="name", view_name="users"),
                LookMLField(name="email", view_name="users"),
                LookMLField(name="internal_id", view_name="users"),
            ],
            sets={"user_facts": ["name", "email"]},
        )
        events = LookMLView(
            name="events",
            fields=[LookMLField(name="id", view_name="events")],
        )

        explore = LookMLExplore(
            name="events",
            base_view="events",
            model_name="test",
            joins=[LookMLJoin(view_name="users", fields=["user_facts*"])],
        )
        views = {"events": events, "users": users}

        fields = get_accessible_fields(explore, views)
        field_names = {f.name for f in fields}

        assert "name" in field_names, "name should be accessible (in set)"
        assert "email" in field_names, "email should be accessible (in set)"
        # internal_id is NOT in the user_facts set, should be excluded
        assert "internal_id" not in field_names, "internal_id should be excluded"


class TestGraphDataPreparation:
    """Test that model data is properly structured for Neo4j."""

    def test_model_data_structure(self):
        model = LookMLModel(
            name="test_model",
            connection="snowflake",
            file_path="/path/to/model.lkml",
        )
        data = {
            "name": model.name,
            "connection": model.connection,
            "file_path": model.file_path,
        }
        assert data["name"] == "test_model"
        assert data["connection"] == "snowflake"

    def test_field_unique_id_format(self):
        field = LookMLField(
            name="country",
            view_name="users",
            explore_name="events",
        )
        assert field.unique_id == "events::users.country"
        assert field.fully_qualified_name == "users.country"
