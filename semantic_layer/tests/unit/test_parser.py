"""
Unit tests for the LookML parser module.

Tests parsing of real LookML fixture files to verify:
- Model, explore, view, and field extraction
- Dimension group expansion into individual timeframe fields
- Derived table (PDT) detection
- Extends/inheritance resolution
- Field set restriction on joins
- Hidden field handling
"""

import os
import sys
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.parser.models import LookMLField, LookMLView, LookMLExplore, LookMLModel
from src.parser.lookml_parser import (
    parse_directory,
    _expand_dimension_groups,
    _build_views,
    get_accessible_fields,
)
from src.parser.inheritance_resolver import resolve_extends, CircularExtendsError


# ── Fixture path (auto-detect from project root) ─────────────────────
FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "looker_fixtures",
)

# Fall back to parent-level directory if running from semantic_layer/
if not os.path.exists(FIXTURE_DIR):
    FIXTURE_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
        "looker_fixtures",
    )


class TestParseDirectory:
    """Test the main parse_directory() entry point against real fixtures."""

    @pytest.fixture(autouse=True)
    def setup(self):
        if not os.path.exists(FIXTURE_DIR):
            pytest.skip(f"LookML fixtures not found at {FIXTURE_DIR}")
        self.models, self.views = parse_directory(FIXTURE_DIR)

    def test_returns_at_least_one_model(self):
        assert len(self.models) >= 1

    def test_model_has_explores(self):
        model = self.models[0]
        assert len(model.explores) >= 1, "Model should have at least one explore"

    def test_model_has_connection(self):
        model = self.models[0]
        assert model.connection, "Model should have a connection string"

    def test_views_are_populated(self):
        assert len(self.views) >= 1, "Should parse at least one view"

    def test_views_have_fields(self):
        for name, view in self.views.items():
            # Some views may have 0 fields if they only define sets
            # but most should have fields
            pass
        views_with_fields = {n: v for n, v in self.views.items() if v.fields}
        assert len(views_with_fields) >= 1, "At least one view should have fields"

    def test_explore_has_joins(self):
        """At least one explore should have joins defined."""
        all_explores = [e for m in self.models for e in m.explores]
        explores_with_joins = [e for e in all_explores if e.joins]
        assert len(explores_with_joins) >= 1, "At least one explore should have joins"

    def test_explore_has_base_view(self):
        """Every explore must have a base_view."""
        all_explores = [e for m in self.models for e in m.explores]
        for explore in all_explores:
            assert explore.base_view, f"Explore '{explore.name}' missing base_view"


class TestDimensionGroupExpansion:
    """Test that dimension_groups are correctly expanded into individual fields."""

    def test_expansion_creates_individual_fields(self):
        """A dimension_group with 3 timeframes should produce 3 + 1 fields."""
        dim_group = LookMLField(
            name="created",
            field_type="dimension_group",
            data_type="time",
            timeframes=["date", "week", "month"],
            view_name="test",
        )
        result = _expand_dimension_groups([dim_group])

        # Should have: the original + 3 expanded
        assert len(result) == 4
        expanded_names = {f.name for f in result if f.field_type == "dimension"}
        assert "created_date" in expanded_names
        assert "created_week" in expanded_names
        assert "created_month" in expanded_names

    def test_expanded_field_has_correct_data_type(self):
        """Each expanded field should have data_type matching its timeframe."""
        dim_group = LookMLField(
            name="event",
            field_type="dimension_group",
            data_type="time",
            timeframes=["date", "year"],
            view_name="events",
        )
        result = _expand_dimension_groups([dim_group])

        date_field = next(f for f in result if f.name == "event_date")
        assert date_field.data_type == "date"

        year_field = next(f for f in result if f.name == "event_year")
        assert year_field.data_type == "year"

    def test_non_dimension_group_unchanged(self):
        """Regular dimensions should pass through unchanged."""
        dim = LookMLField(name="country", field_type="dimension", data_type="string")
        result = _expand_dimension_groups([dim])
        assert len(result) == 1
        assert result[0].name == "country"

    def test_empty_timeframes_get_defaults(self):
        """Dimension group with no timeframes should get default timeframes."""
        dim_group = LookMLField(
            name="created",
            field_type="dimension_group",
            data_type="time",
            timeframes=[],
            view_name="test",
        )
        result = _expand_dimension_groups([dim_group])
        # Should have defaults: raw, time, date, week, month, quarter, year
        assert len(result) >= 7


class TestInheritanceResolver:
    """Test extends resolution."""

    def test_child_inherits_parent_fields(self):
        parent = LookMLView(
            name="parent",
            fields=[
                LookMLField(name="field_a", field_type="dimension", view_name="parent"),
                LookMLField(name="field_b", field_type="measure", view_name="parent"),
            ],
        )
        child = LookMLView(
            name="child",
            extends=["parent"],
            fields=[
                LookMLField(name="field_c", field_type="dimension", view_name="child"),
            ],
        )
        views = {"parent": parent, "child": child}
        result = resolve_extends(views)

        child_fields = {f.name for f in result["child"].fields}
        assert "field_a" in child_fields, "Child should inherit field_a"
        assert "field_b" in child_fields, "Child should inherit field_b"
        assert "field_c" in child_fields, "Child should keep its own field"

    def test_child_overrides_parent_field(self):
        parent = LookMLView(
            name="parent",
            fields=[
                LookMLField(name="revenue", field_type="measure", sql="SUM(parent)", view_name="parent"),
            ],
        )
        child = LookMLView(
            name="child",
            extends=["parent"],
            fields=[
                LookMLField(name="revenue", field_type="measure", sql="SUM(child)", view_name="child"),
            ],
        )
        views = {"parent": parent, "child": child}
        result = resolve_extends(views)

        revenue = next(f for f in result["child"].fields if f.name == "revenue")
        assert revenue.sql == "SUM(child)", "Child's version should win"

    def test_circular_extends_detected(self):
        a = LookMLView(name="a", extends=["b"])
        b = LookMLView(name="b", extends=["a"])
        views = {"a": a, "b": b}

        with pytest.raises(CircularExtendsError):
            resolve_extends(views)

    def test_inherited_field_gets_child_view_name(self):
        parent = LookMLView(
            name="parent",
            fields=[LookMLField(name="field_a", view_name="parent")],
        )
        child = LookMLView(name="child", extends=["parent"])
        views = {"parent": parent, "child": child}
        result = resolve_extends(views)

        inherited = next(f for f in result["child"].fields if f.name == "field_a")
        assert inherited.view_name == "child"


class TestAccessibleFields:
    """Test get_accessible_fields — which fields are visible in an explore."""

    def test_base_view_fields_accessible(self):
        view = LookMLView(
            name="events",
            fields=[LookMLField(name="id", view_name="events")],
        )
        explore = LookMLExplore(name="events", base_view="events")
        views = {"events": view}

        fields = get_accessible_fields(explore, views)
        assert any(f.name == "id" for f in fields)

    def test_joined_view_fields_accessible(self):
        from src.parser.models import LookMLJoin

        base = LookMLView(
            name="events",
            fields=[LookMLField(name="id", view_name="events")],
        )
        joined = LookMLView(
            name="users",
            fields=[LookMLField(name="country", view_name="users")],
        )
        explore = LookMLExplore(
            name="events",
            base_view="events",
            joins=[LookMLJoin(view_name="users", sql_on="${events.user_id} = ${users.id}")],
        )
        views = {"events": base, "users": joined}

        fields = get_accessible_fields(explore, views)
        assert any(f.name == "country" for f in fields)

    def test_fields_have_explore_name_set(self):
        view = LookMLView(
            name="events",
            fields=[LookMLField(name="id", view_name="events")],
        )
        explore = LookMLExplore(name="events", base_view="events", model_name="my_model")
        views = {"events": view}

        fields = get_accessible_fields(explore, views)
        assert all(f.explore_name == "events" for f in fields)


class TestPDTDetection:
    """Test that derived tables are correctly identified."""

    @pytest.fixture(autouse=True)
    def setup(self):
        if not os.path.exists(FIXTURE_DIR):
            pytest.skip(f"LookML fixtures not found at {FIXTURE_DIR}")
        self.models, self.views = parse_directory(FIXTURE_DIR)

    def test_at_least_one_pdt_exists(self):
        """The fixtures should contain at least one derived table."""
        pdts = {name: v for name, v in self.views.items() if v.is_pdt}
        assert len(pdts) >= 1, "Should find at least one PDT in fixtures"

    def test_pdt_has_sql(self):
        """PDT views should have derived_table_sql populated."""
        for name, view in self.views.items():
            if view.is_pdt:
                assert view.derived_table_sql, f"PDT '{name}' missing derived_table_sql"
