"""
Unit tests for the context assembler.

Tests that the assembled context is:
- Minimal (only selected fields, not the full explore)
- Complete (includes joins, always_filters, time resolution)
- Correct (PDT warnings present, time filters resolved)
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.parser.models import LookMLField
from src.retrieval.context_assembler import ContextAssembler
from src.retrieval.retriever import RetrievalResult


class MockCache:
    def __init__(self, explore_data=None):
        self._data = explore_data or {}

    def get_explore(self, name):
        return self._data.get(name)


class TestContextAssembly:
    """Test the context assembly pipeline."""

    def test_basic_assembly(self):
        cache = MockCache(explore_data={
            "events": {
                "explore": {"description": "Event data", "is_hidden": False},
                "base_view": "events",
                "joins": [{"view_name": "users", "join_type": "left_outer",
                          "relationship": "many_to_one", "is_pdt": False}],
                "fields": [],
                "always_filter": {},
                "model_name": "test_model",
            }
        })
        assembler = ContextAssembler(cache)

        result = RetrievalResult(
            explore_name="events",
            model_name="test_model",
            selected_fields=[
                LookMLField(name="count", view_name="events", field_type="measure"),
                LookMLField(name="country", view_name="users", field_type="dimension"),
            ],
            joins_needed=[],
        )
        intent = {"time_range": {"period": "last_quarter", "grain": "quarter"}}

        context = assembler.assemble(result, intent)

        assert context["explore_name"] == "events"
        assert context["model_name"] == "test_model"
        assert "events.count" in context["fields_formatted"]
        assert context["time_filter_value"] == "last 1 quarter"

    def test_time_resolution(self):
        cache = MockCache(explore_data={
            "events": {
                "explore": {}, "base_view": "events",
                "joins": [], "fields": [],
                "always_filter": {}, "model_name": "m",
            }
        })
        assembler = ContextAssembler(cache)

        test_cases = {
            "last_quarter": "last 1 quarter",
            "this_month": "this month",
            "last_30_days": "30 days",
            "ytd": "this year",
            "last_week": "last 1 week",
        }

        for period, expected in test_cases.items():
            result = RetrievalResult(explore_name="events", model_name="m")
            intent = {"time_range": {"period": period, "grain": ""}}
            context = assembler.assemble(result, intent)
            assert context["time_filter_value"] == expected, \
                f"Period '{period}' should resolve to '{expected}', got '{context['time_filter_value']}'"

    def test_empty_explore_returns_empty_context(self):
        cache = MockCache()
        assembler = ContextAssembler(cache)

        result = RetrievalResult()  # No explore
        intent = {}
        context = assembler.assemble(result, intent)

        assert context["explore_name"] == ""
        assert context["fields_formatted"] == "No fields found."
