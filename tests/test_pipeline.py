"""Integration tests for src/pipeline.py"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.ghl.client import GHLClientError
from src.permits.fetcher import PermitFetchError
from src.pipeline import Pipeline
from src.scoring.scorer import LeadScorer, ScoredLead


def _recent_date(days_ago=10):
    return (datetime.now(tz=timezone.utc) - timedelta(days=days_ago)).strftime(
        "%Y-%m-%d"
    )


SAMPLE_PERMITS = [
    {
        "permit_num": "BP-001",
        "status_current": "open",
        "address": "100 Elm St",
        "city": "Austin",
        "state": "TX",
        "zip_code": "78701",
        "total_valuation": "120000",
        "permit_type_desc": "New Construction",
        "property_use_code_desc": "Residential Single Family",
        "issue_date": _recent_date(5),
    },
    {
        "permit_num": "BP-002",
        "status_current": "open",
        "address": "200 Oak Ave",
        "total_valuation": "500",
        "permit_type_desc": "Minor Repair",
        "issue_date": _recent_date(200),
    },
]


class TestPipeline:
    def _make_pipeline(self, permits=SAMPLE_PERMITS, ghl_responses=None):
        fetcher = MagicMock()
        fetcher.fetch.return_value = permits

        ghl_client = MagicMock()
        ghl_client.push_leads.return_value = ghl_responses or [{"contact": {"id": "c1"}}]

        pipeline = Pipeline(
            fetcher=fetcher,
            scorer=LeadScorer(),
            ghl_client=ghl_client,
        )
        return pipeline, fetcher, ghl_client

    def test_run_returns_pipeline_result(self):
        pipeline, _, _ = self._make_pipeline()
        result = pipeline.run()
        assert result.permits_fetched == 2

    def test_run_scores_all_permits(self):
        pipeline, _, _ = self._make_pipeline()
        result = pipeline.run()
        assert result.leads_scored == 2

    def test_run_qualifies_high_score_only(self, monkeypatch):
        monkeypatch.setenv("MIN_LEAD_SCORE", "50")
        pipeline, _, ghl_client = self._make_pipeline(
            ghl_responses=[{"contact": {"id": "c1"}}]
        )
        result = pipeline.run()
        # BP-001 (high-value new construction, residential, recent) qualifies
        # BP-002 (low-value minor repair, old) does not qualify
        assert result.leads_qualified == 1

    def test_run_pushes_qualified_leads(self, monkeypatch):
        monkeypatch.setenv("MIN_LEAD_SCORE", "50")
        pipeline, _, ghl_client = self._make_pipeline(
            ghl_responses=[{"contact": {"id": "c1"}}]
        )
        result = pipeline.run()
        assert ghl_client.push_leads.called

    def test_run_fetch_error_returns_early(self):
        fetcher = MagicMock()
        fetcher.fetch.side_effect = PermitFetchError("Network error")
        pipeline = Pipeline(fetcher=fetcher, scorer=LeadScorer())
        result = pipeline.run()
        assert result.permits_fetched == 0
        assert len(result.errors) > 0
        assert "Permit fetch failed" in result.errors[0]

    def test_run_ghl_error_captured_in_errors(self):
        fetcher = MagicMock()
        fetcher.fetch.return_value = SAMPLE_PERMITS
        ghl_client = MagicMock()
        ghl_client.push_leads.side_effect = GHLClientError("Auth failed")
        pipeline = Pipeline(fetcher=fetcher, scorer=LeadScorer(), ghl_client=ghl_client)
        result = pipeline.run()
        assert any("GHL push failed" in e for e in result.errors)

    def test_run_without_ghl_skips_push(self, monkeypatch):
        monkeypatch.delenv("GHL_API_KEY", raising=False)
        monkeypatch.delenv("GHL_LOCATION_ID", raising=False)
        fetcher = MagicMock()
        fetcher.fetch.return_value = SAMPLE_PERMITS
        pipeline = Pipeline(fetcher=fetcher, scorer=LeadScorer())
        result = pipeline.run()
        assert result.leads_pushed == 0
        assert any("not configured" in e for e in result.errors)

    def test_pipeline_result_repr(self):
        pipeline, _, _ = self._make_pipeline()
        result = pipeline.run()
        assert "PipelineResult" in repr(result)
