"""
Unit tests for the ambiguity detector.

Tests all four ambiguity types:
1. Attribution — multiple attribution-tagged fields match
2. Field collision — same field name in different views
3. Explore conflict — top-2 scores too close
4. Cross-explore impossible (tested indirectly)
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.retrieval.ambiguity_detector import AmbiguityDetector, AmbiguityResult


class MockCache:
    """Minimal mock of ExploreContextCache for testing."""

    def __init__(self, explores=None):
        self._explores = explores or {}

    def get_explore(self, name):
        return self._explores.get(name)

    def all_explore_names(self):
        return list(self._explores.keys())


class TestAttributionAmbiguity:
    """Test that attribution ambiguity is correctly detected."""

    def test_triggered_when_both_first_and_last_touch_present(self):
        cache = MockCache()
        detector = AmbiguityDetector(cache)

        candidates = [
            {
                "field_name": "site_acquisition_source",
                "view_name": "user_session_fact",
                "description": "Traffic source of first visit — first touch attribution",
                "tags": [],
                "score": 0.9,
            },
            {
                "field_name": "purchase_session_source",
                "view_name": "session_purchase_facts",
                "description": "Last touch attribution: source of last session before purchase",
                "tags": [],
                "score": 0.85,
            },
        ]
        intent = {"metrics": ["revenue"], "dimensions": ["channel"], "attribution_hint": None}
        scores = {"events": 0.8}

        result = detector.detect(
            candidates=candidates,
            intent=intent,
            explore_scores=scores,
            user_query="Which marketing channel drives the most revenue?",
        )

        assert result.detected, "Should detect attribution ambiguity"
        assert result.ambiguity_type == "attribution"
        assert result.is_blocking
        assert len(result.options) >= 2

    def test_not_triggered_when_attribution_hint_provided(self):
        cache = MockCache()
        detector = AmbiguityDetector(cache)

        candidates = [
            {"field_name": "site_acquisition_source", "view_name": "v",
             "description": "first touch", "tags": [], "score": 0.9},
            {"field_name": "purchase_source", "view_name": "v",
             "description": "last touch", "tags": [], "score": 0.85},
        ]
        # User already specified first_touch
        intent = {"attribution_hint": "first_touch"}
        scores = {"events": 0.8}

        result = detector.detect(candidates, intent, scores, "channel revenue")
        assert not result.detected, "Should NOT detect ambiguity when hint provided"

    def test_not_triggered_for_non_attribution_query(self):
        cache = MockCache()
        detector = AmbiguityDetector(cache)

        candidates = [
            {"field_name": "count", "view_name": "events",
             "description": "Total events", "tags": [], "score": 0.9},
        ]
        intent = {"metrics": ["count"], "attribution_hint": None}
        scores = {"events": 0.8}

        result = detector.detect(candidates, intent, scores, "how many events total?")
        assert not result.detected


class TestExploreConflict:
    """Test explore conflict detection."""

    def test_triggered_when_scores_are_close(self):
        cache = MockCache(explores={
            "events": {"explore": {"description": "Event data"}, "joins": [], "fields": []},
            "sessions": {"explore": {"description": "Session data"}, "joins": [], "fields": []},
        })
        detector = AmbiguityDetector(cache)

        # Scores within 0.1 of each other (threshold default)
        scores = {"events": 0.75, "sessions": 0.72}

        result = detector.detect(
            candidates=[],
            intent={"attribution_hint": None},
            explore_scores=scores,
            user_query="show me data",
        )

        assert result.detected
        assert result.ambiguity_type == "explore_conflict"

    def test_not_triggered_when_clear_winner(self):
        cache = MockCache(explores={
            "events": {"explore": {"description": "Event data"}, "joins": [], "fields": []},
            "sessions": {"explore": {"description": "Session data"}, "joins": [], "fields": []},
        })
        detector = AmbiguityDetector(cache)

        # Clear winner — events is much higher
        scores = {"events": 0.90, "sessions": 0.50}

        result = detector.detect(
            candidates=[],
            intent={"attribution_hint": None},
            explore_scores=scores,
            user_query="show me events",
        )

        assert not result.detected


class TestFieldCollision:
    """Test field name collision detection."""

    def test_triggered_when_same_name_different_views(self):
        cache = MockCache()
        detector = AmbiguityDetector(cache)

        candidates = [
            {"field_name": "created_date", "view_name": "sessions",
             "description": "When the session started", "tags": [], "score": 0.85},
            {"field_name": "created_date", "view_name": "orders",
             "description": "When the order was placed", "tags": [], "score": 0.82},
        ]
        intent = {"dimensions": ["date"], "attribution_hint": None}
        scores = {"events": 0.8}

        result = detector.detect(candidates, intent, scores, "revenue by date")

        assert result.detected
        assert result.ambiguity_type == "field_collision"
        assert result.is_blocking
