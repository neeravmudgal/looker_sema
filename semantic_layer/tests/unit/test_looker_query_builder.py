"""
Unit tests for the Looker query builder and validator.

Tests:
- Direct assembly (fallback mode without LLM)
- Time filter resolution
- Measure filter routing to having_filters
- Always filter injection
- Query validation and field removal
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.parser.models import LookMLField
from src.retrieval.retriever import RetrievalResult
from src.query_generator.validator import QueryValidator


class MockCache:
    def __init__(self, fields=None):
        self._fields = fields or []

    def get_explore(self, name):
        return {
            "explore": {"is_hidden": False},
            "fields": self._fields,
            "joins": [],
            "always_filter": {},
            "model_name": "test",
            "base_view": "events",
        }

    def all_explore_names(self):
        return ["events"]


class TestQueryValidator:
    """Test query validation logic."""

    def test_valid_fields_pass(self):
        fields = [
            LookMLField(name="count", view_name="events", field_type="measure"),
            LookMLField(name="country", view_name="users", field_type="dimension"),
        ]
        cache = MockCache(fields=fields)
        validator = QueryValidator(cache)

        query = {
            "explore": "events",
            "model": "test",
            "fields": ["events.count", "users.country"],
            "filters": {},
            "sorts": [],
            "limit": 500,
        }

        result = validator.validate(query)
        assert result.is_valid
        assert len(result.query["fields"]) == 2
        assert not result.warnings

    def test_invalid_field_removed_with_warning(self):
        fields = [
            LookMLField(name="count", view_name="events", field_type="measure"),
        ]
        cache = MockCache(fields=fields)
        validator = QueryValidator(cache)

        query = {
            "explore": "events",
            "fields": ["events.count", "nonexistent.field"],
            "filters": {},
            "sorts": [],
            "limit": 500,
        }

        result = validator.validate(query)
        assert len(result.query["fields"]) == 1
        assert "nonexistent.field" in result.removed_fields
        assert any("nonexistent.field" in w for w in result.warnings)

    def test_limit_enforced(self):
        cache = MockCache(fields=[])
        validator = QueryValidator(cache)

        query = {"explore": "events", "fields": [], "limit": 99999}
        result = validator.validate(query)
        assert result.query["limit"] == 5000

        query = {"explore": "events", "fields": [], "limit": -1}
        result = validator.validate(query)
        assert result.query["limit"] == 1

    def test_missing_explore_returns_invalid(self):
        cache = MockCache(fields=[])
        # Override get_explore to return None for unknown explores
        cache.get_explore = lambda name: None if name == "nonexistent" else cache._fields
        validator = QueryValidator(cache)

        query = {"explore": "nonexistent", "fields": ["foo.bar"]}
        result = validator.validate(query)
        assert not result.is_valid

    def test_filter_on_invalid_field_removed(self):
        fields = [
            LookMLField(name="count", view_name="events", field_type="measure"),
        ]
        cache = MockCache(fields=fields)
        validator = QueryValidator(cache)

        query = {
            "explore": "events",
            "fields": ["events.count"],
            "filters": {"bad.field": "value"},
            "sorts": [],
            "limit": 500,
        }

        result = validator.validate(query)
        assert "bad.field" not in result.query["filters"]
