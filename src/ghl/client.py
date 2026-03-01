"""GoHighLevel (GHL) CRM integration.

Pushes qualified leads into GoHighLevel as contacts using the GHL REST API
(v1 Contacts endpoint).

Required environment variables:
    GHL_API_KEY        – Private API key (Location API key).
    GHL_LOCATION_ID    – Location (sub-account) ID.
"""

import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv

from src.scoring.scorer import ScoredLead

load_dotenv()

logger = logging.getLogger(__name__)

GHL_CONTACTS_URL = "https://rest.gohighlevel.com/v1/contacts/"


class GHLClientError(Exception):
    """Raised when the GHL API returns an unexpected response."""


class GHLClient:
    """Thin wrapper around the GoHighLevel Contacts REST API."""

    def __init__(
        self,
        api_key: str | None = None,
        location_id: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key or os.getenv("GHL_API_KEY", "")
        self.location_id = location_id or os.getenv("GHL_LOCATION_ID", "")
        self._session = session or requests.Session()

        if not self.api_key:
            raise GHLClientError("GHL_API_KEY is required but not set.")
        if not self.location_id:
            raise GHLClientError("GHL_LOCATION_ID is required but not set.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push_leads(self, leads: list[ScoredLead]) -> list[dict[str, Any]]:
        """Upsert each qualified lead as a GHL contact.

        Args:
            leads: Scored leads to push (all are pushed; the caller should
                pass pre-filtered qualified leads).

        Returns:
            List of GHL API response bodies (one per lead).
        """
        results = []
        for lead in leads:
            payload = self._build_contact_payload(lead)
            result = self._upsert_contact(payload)
            results.append(result)
        logger.info("Pushed %d leads to GHL", len(results))
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_contact_payload(self, lead: ScoredLead) -> dict[str, Any]:
        permit = lead.permit

        # Best-effort field mapping from common Socrata permit schemas
        address = (
            permit.get("address")
            or permit.get("work_location_address")
            or permit.get("original_address1")
            or ""
        )
        city = permit.get("city") or permit.get("work_location_city") or ""
        state = permit.get("state") or permit.get("work_location_state") or ""
        postal_code = (
            permit.get("zip_code")
            or permit.get("work_location_zip")
            or permit.get("original_zip")
            or ""
        )
        permit_id = (
            permit.get("permit_num")
            or permit.get("permit_number")
            or permit.get("permitnumber")
            or "unknown"
        )

        notes = (
            f"[Auto-generated lead]\n"
            f"Permit ID: {permit_id}\n"
            f"Lead Score: {lead.score}/100\n"
            f"Scoring reasons: {', '.join(lead.reasons)}\n"
            f"Permit type: {permit.get('permit_type_desc') or permit.get('permit_class_mapped') or 'N/A'}\n"
            f"Issued: {permit.get('issue_date') or permit.get('issued_date') or 'N/A'}"
        )

        return {
            "locationId": self.location_id,
            "name": f"Permit Lead – {address or permit_id}",
            "address1": address,
            "city": city,
            "state": state,
            "postalCode": postal_code,
            "source": "Open Permits Pipeline",
            "tags": ["open-permit", "auto-lead"],
            "customField": [
                {"id": "permit_id", "value": str(permit_id)},
                {"id": "lead_score", "value": str(lead.score)},
            ],
            "notes": notes,
        }

    def _upsert_contact(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Version": "2021-07-28",
        }
        try:
            response = self._session.post(
                GHL_CONTACTS_URL, json=payload, headers=headers, timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise GHLClientError(f"Failed to push contact to GHL: {exc}") from exc

        return response.json()
