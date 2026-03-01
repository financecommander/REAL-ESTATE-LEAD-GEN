"""Tests for src/permits/fetcher.py"""

import pytest
import responses as rsps_lib

from src.permits.fetcher import PermitFetcher, PermitFetchError


SAMPLE_PERMITS = [
    {
        "permit_num": "BP-2024-001",
        "status_current": "open",
        "address": "123 Main St",
        "total_valuation": "75000",
        "permit_type_desc": "New Construction",
        "issue_date": "2026-02-01T00:00:00.000",
    },
    {
        "permit_num": "BP-2024-002",
        "status_current": "open",
        "address": "456 Oak Ave",
        "total_valuation": "5000",
        "permit_type_desc": "Minor Repair",
        "issue_date": "2025-01-01T00:00:00.000",
    },
]


class TestPermitFetcher:
    def test_init_defaults(self, monkeypatch):
        monkeypatch.delenv("PERMITS_API_URL", raising=False)
        monkeypatch.delenv("PERMITS_APP_TOKEN", raising=False)
        monkeypatch.delenv("PERMITS_LIMIT", raising=False)
        fetcher = PermitFetcher()
        assert "austintexas" in fetcher.api_url
        assert fetcher.limit == 200

    def test_init_custom_values(self):
        fetcher = PermitFetcher(
            api_url="https://example.com/api",
            app_token="token123",
            limit=50,
        )
        assert fetcher.api_url == "https://example.com/api"
        assert fetcher.app_token == "token123"
        assert fetcher.limit == 50

    @rsps_lib.activate
    def test_fetch_returns_list(self):
        fetcher = PermitFetcher(api_url="https://fake.api/permits.json", limit=10)
        rsps_lib.add(
            rsps_lib.GET,
            "https://fake.api/permits.json",
            json=SAMPLE_PERMITS,
            status=200,
        )
        result = fetcher.fetch()
        assert isinstance(result, list)
        assert len(result) == 2

    @rsps_lib.activate
    def test_fetch_passes_status_filter(self):
        fetcher = PermitFetcher(api_url="https://fake.api/permits.json", limit=10)
        rsps_lib.add(
            rsps_lib.GET,
            "https://fake.api/permits.json",
            json=SAMPLE_PERMITS,
            status=200,
        )
        fetcher.fetch(status_filter="open")
        assert "status_current=open" in rsps_lib.calls[0].request.url

    @rsps_lib.activate
    def test_fetch_no_status_filter(self):
        fetcher = PermitFetcher(api_url="https://fake.api/permits.json", limit=10)
        rsps_lib.add(
            rsps_lib.GET,
            "https://fake.api/permits.json",
            json=SAMPLE_PERMITS,
            status=200,
        )
        fetcher.fetch(status_filter="")
        assert "status_current" not in rsps_lib.calls[0].request.url

    @rsps_lib.activate
    def test_fetch_http_error_raises(self):
        fetcher = PermitFetcher(api_url="https://fake.api/permits.json", limit=10)
        rsps_lib.add(
            rsps_lib.GET,
            "https://fake.api/permits.json",
            json={"error": "not found"},
            status=404,
        )
        with pytest.raises(PermitFetchError):
            fetcher.fetch()

    @rsps_lib.activate
    def test_fetch_non_list_response_raises(self):
        fetcher = PermitFetcher(api_url="https://fake.api/permits.json", limit=10)
        rsps_lib.add(
            rsps_lib.GET,
            "https://fake.api/permits.json",
            json={"message": "not a list"},
            status=200,
        )
        with pytest.raises(PermitFetchError, match="JSON array"):
            fetcher.fetch()

    @rsps_lib.activate
    def test_fetch_sends_app_token_header(self):
        fetcher = PermitFetcher(
            api_url="https://fake.api/permits.json",
            app_token="mytoken",
            limit=10,
        )
        rsps_lib.add(
            rsps_lib.GET,
            "https://fake.api/permits.json",
            json=[],
            status=200,
        )
        fetcher.fetch()
        assert rsps_lib.calls[0].request.headers["X-App-Token"] == "mytoken"
