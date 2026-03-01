"""Tests for src/scoring/scorer.py"""

from datetime import datetime, timedelta, timezone

import pytest

from src.scoring.scorer import LeadScorer, ScoredLead


def _permit(
    valuation=0,
    permit_type="",
    property_use="",
    issue_date=None,
):
    p: dict = {}
    if valuation:
        p["total_valuation"] = str(valuation)
    if permit_type:
        p["permit_type_desc"] = permit_type
    if property_use:
        p["property_use_code_desc"] = property_use
    if issue_date:
        p["issue_date"] = issue_date
    return p


class TestLeadScorer:
    def test_empty_input(self):
        scorer = LeadScorer()
        assert scorer.score([]) == []

    def test_high_value_permit(self):
        scorer = LeadScorer()
        p = _permit(valuation=100_000)
        result = scorer.score([p])
        assert result[0].score >= 30
        assert any("High-value" in r for r in result[0].reasons)

    def test_medium_value_permit(self):
        scorer = LeadScorer()
        p = _permit(valuation=20_000)
        result = scorer.score([p])
        assert result[0].score >= 15
        assert any("Medium-value" in r for r in result[0].reasons)

    def test_low_value_permit_gets_zero_value_points(self):
        scorer = LeadScorer()
        p = _permit(valuation=500)
        result = scorer.score([p])
        assert not any("value" in r.lower() for r in result[0].reasons)

    def test_new_construction_type(self):
        scorer = LeadScorer()
        p = _permit(permit_type="New Construction Single Family")
        result = scorer.score([p])
        assert any("New construction" in r for r in result[0].reasons)

    def test_renovation_type(self):
        scorer = LeadScorer()
        p = _permit(permit_type="Full Home Renovation")
        result = scorer.score([p])
        assert any("renovation" in r.lower() for r in result[0].reasons)

    def test_residential_property(self):
        scorer = LeadScorer()
        p = _permit(property_use="Residential Single Family")
        result = scorer.score([p])
        assert any("Residential" in r for r in result[0].reasons)

    def test_commercial_property(self):
        scorer = LeadScorer()
        p = _permit(property_use="Commercial Office")
        result = scorer.score([p])
        assert any("Commercial" in r for r in result[0].reasons)

    def test_very_recent_permit(self):
        scorer = LeadScorer()
        recent = (datetime.now(tz=timezone.utc) - timedelta(days=5)).strftime(
            "%Y-%m-%d"
        )
        p = _permit(issue_date=recent)
        result = scorer.score([p])
        assert any("Very recent" in r for r in result[0].reasons)

    def test_recent_permit_within_90_days(self):
        scorer = LeadScorer()
        date = (datetime.now(tz=timezone.utc) - timedelta(days=60)).strftime(
            "%Y-%m-%d"
        )
        p = _permit(issue_date=date)
        result = scorer.score([p])
        assert any("Recent permit" in r for r in result[0].reasons)

    def test_old_permit_gets_no_recency_points(self):
        scorer = LeadScorer()
        old = (datetime.now(tz=timezone.utc) - timedelta(days=200)).strftime(
            "%Y-%m-%d"
        )
        p = _permit(issue_date=old)
        result = scorer.score([p])
        assert not any("recent" in r.lower() for r in result[0].reasons)

    def test_score_capped_at_100(self):
        scorer = LeadScorer()
        recent = (datetime.now(tz=timezone.utc) - timedelta(days=5)).strftime(
            "%Y-%m-%d"
        )
        p = _permit(
            valuation=200_000,
            permit_type="New Construction",
            property_use="Residential Single Family",
            issue_date=recent,
        )
        result = scorer.score([p])
        assert result[0].score <= 100

    def test_qualified_filters_by_min_score(self, monkeypatch):
        monkeypatch.setenv("MIN_LEAD_SCORE", "70")
        scorer = LeadScorer()
        recent = (datetime.now(tz=timezone.utc) - timedelta(days=5)).strftime(
            "%Y-%m-%d"
        )
        high_score = _permit(
            valuation=200_000,
            permit_type="New Construction",
            property_use="Residential Single Family",
            issue_date=recent,
        )
        low_score = _permit(valuation=500)
        qualified = scorer.qualified([high_score, low_score])
        assert all(lead.score >= 70 for lead in qualified)

    def test_is_qualified_property(self, monkeypatch):
        monkeypatch.setenv("MIN_LEAD_SCORE", "50")
        lead_above = ScoredLead(permit={}, score=60)
        lead_below = ScoredLead(permit={}, score=40)
        assert lead_above.is_qualified is True
        assert lead_below.is_qualified is False

    def test_valuation_with_currency_formatting(self):
        scorer = LeadScorer()
        p = {"total_valuation": "$75,000.00"}
        result = scorer.score([p])
        assert any("High-value" in r for r in result[0].reasons)
