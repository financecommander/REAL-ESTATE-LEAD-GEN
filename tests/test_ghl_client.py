"""Tests for src/ghl/client.py"""

import pytest
import responses as rsps_lib

from src.ghl.client import GHLClient, GHLClientError, GHL_CONTACTS_URL
from src.scoring.scorer import ScoredLead


def _make_client(**kwargs):
    defaults = {"api_key": "test-key", "location_id": "loc-123"}
    defaults.update(kwargs)
    return GHLClient(**defaults)


def _make_lead(score=75, **permit_fields):
    permit = {
        "permit_num": "BP-001",
        "address": "123 Main St",
        "city": "Austin",
        "state": "TX",
        "zip_code": "78701",
        "permit_type_desc": "New Construction",
        "issue_date": "2026-01-15",
    }
    permit.update(permit_fields)
    return ScoredLead(permit=permit, score=score, reasons=["New construction permit"])


class TestGHLClient:
    def test_raises_without_api_key(self):
        with pytest.raises(GHLClientError, match="GHL_API_KEY"):
            GHLClient(api_key="", location_id="loc-123")

    def test_raises_without_location_id(self):
        with pytest.raises(GHLClientError, match="GHL_LOCATION_ID"):
            GHLClient(api_key="key", location_id="")

    def test_init_from_env(self, monkeypatch):
        monkeypatch.setenv("GHL_API_KEY", "envkey")
        monkeypatch.setenv("GHL_LOCATION_ID", "envloc")
        client = GHLClient()
        assert client.api_key == "envkey"
        assert client.location_id == "envloc"

    @rsps_lib.activate
    def test_push_single_lead(self):
        client = _make_client()
        rsps_lib.add(rsps_lib.POST, GHL_CONTACTS_URL, json={"contact": {"id": "c1"}}, status=200)
        leads = [_make_lead()]
        results = client.push_leads(leads)
        assert len(results) == 1
        assert results[0]["contact"]["id"] == "c1"

    @rsps_lib.activate
    def test_push_multiple_leads(self):
        client = _make_client()
        rsps_lib.add(rsps_lib.POST, GHL_CONTACTS_URL, json={"contact": {"id": "c1"}}, status=200)
        rsps_lib.add(rsps_lib.POST, GHL_CONTACTS_URL, json={"contact": {"id": "c2"}}, status=200)
        leads = [_make_lead(score=80), _make_lead(score=60)]
        results = client.push_leads(leads)
        assert len(results) == 2

    @rsps_lib.activate
    def test_push_empty_list(self):
        client = _make_client()
        results = client.push_leads([])
        assert results == []

    @rsps_lib.activate
    def test_push_sends_auth_header(self):
        client = _make_client(api_key="my-secret-key")
        rsps_lib.add(rsps_lib.POST, GHL_CONTACTS_URL, json={}, status=200)
        client.push_leads([_make_lead()])
        assert rsps_lib.calls[0].request.headers["Authorization"] == "Bearer my-secret-key"

    @rsps_lib.activate
    def test_push_raises_on_http_error(self):
        client = _make_client()
        rsps_lib.add(rsps_lib.POST, GHL_CONTACTS_URL, json={"error": "unauthorized"}, status=401)
        with pytest.raises(GHLClientError):
            client.push_leads([_make_lead()])

    def test_contact_payload_structure(self):
        client = _make_client()
        lead = _make_lead()
        payload = client._build_contact_payload(lead)
        assert payload["locationId"] == "loc-123"
        assert "123 Main St" in payload["address1"]
        assert payload["city"] == "Austin"
        assert payload["state"] == "TX"
        assert payload["postalCode"] == "78701"
        assert "open-permit" in payload["tags"]
        assert "75" in payload["notes"]

    def test_contact_payload_missing_fields(self):
        client = _make_client()
        lead = ScoredLead(permit={}, score=55, reasons=[])
        payload = client._build_contact_payload(lead)
        assert payload["locationId"] == "loc-123"
        assert payload["address1"] == ""
        assert "unknown" in payload["notes"]
