"""
OpenShovels — Hartford, CT Jurisdiction Scraper
Source: Hartford Open Data Portal (Socrata)
Dataset: Building Permits
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import httpx
from loguru import logger

from openshovels.schema import (
    StandardPermit, PermitType, PermitStatus, PropertyType, DataSource
)
from openshovels.jurisdictions.template.base import JurisdictionScraper


# Hartford open data — adjust dataset ID after confirming on data.hartford.gov
HARTFORD_SOCRATA_DOMAIN = "data.hartford.gov"
HARTFORD_DATASET_ID = "building-permits"  # Placeholder — verify actual ID
SOCRATA_APP_TOKEN = None  # Optional, increases rate limit


class HartfordScraper(JurisdictionScraper):
    jurisdiction_code = "ct_hartford"
    jurisdiction_name = "Hartford, CT"
    data_source_url = f"https://{HARTFORD_SOCRATA_DOMAIN}/resource/{HARTFORD_DATASET_ID}.json"
    data_source_type = "socrata"

    async def fetch_permits(
        self,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """Fetch from Hartford's Socrata open data API."""
        params = {
            "$limit": limit or 10000,
            "$order": "issue_date DESC",
        }
        if since:
            params["$where"] = f"issue_date > '{since.strftime('%Y-%m-%dT%H:%M:%S')}'"

        headers = {}
        if SOCRATA_APP_TOKEN:
            headers["X-App-Token"] = SOCRATA_APP_TOKEN

        async with httpx.AsyncClient(timeout=60) as client:
            # Try Socrata endpoint first
            try:
                resp = await client.get(self.data_source_url, params=params, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                logger.info(f"[ct_hartford] Socrata returned {len(data)} records")
                return data
            except httpx.HTTPError as e:
                logger.warning(f"[ct_hartford] Socrata failed: {e}")

            # Fallback: CT statewide open data
            fallback_url = "https://data.ct.gov/resource/building-permits.json"
            try:
                params["$where"] = (
                    f"city = 'HARTFORD'" +
                    (f" AND issue_date > '{since.strftime('%Y-%m-%dT%H:%M:%S')}'" if since else "")
                )
                resp = await client.get(fallback_url, params=params, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                logger.info(f"[ct_hartford] CT statewide fallback returned {len(data)} records")
                return data
            except httpx.HTTPError as e:
                logger.error(f"[ct_hartford] All sources failed: {e}")
                return []

    def normalize(self, raw: dict) -> StandardPermit:
        """
        Normalize Hartford permit data into StandardPermit.
        Field mapping may need adjustment based on actual Socrata schema.
        """
        return StandardPermit(
            permit_id=raw.get("permit_number", raw.get("permit_no", raw.get("id", "UNKNOWN"))),
            jurisdiction=self.jurisdiction_code,
            source=DataSource.OPEN_DATA_PORTAL,

            # Dates
            filed_date=self._parse_date(raw.get("application_date")),
            issued_date=self._parse_date(raw.get("issue_date", raw.get("issued_date"))),
            expiration_date=self._parse_date(raw.get("expiration_date")),

            # Location
            address=raw.get("address", raw.get("location", "")),
            city="Hartford",
            state="CT",
            zip_code=raw.get("zip_code", raw.get("zip", "")),
            latitude=self._parse_float(raw.get("latitude")),
            longitude=self._parse_float(raw.get("longitude")),

            # Permit details
            permit_type=self._classify_permit_type(raw),
            permit_status=self._classify_status(raw),
            description=raw.get("description", raw.get("work_description", "")),
            job_value=self._parse_decimal(raw.get("estimated_cost", raw.get("job_value"))),

            # Property
            property_type=self._classify_property_type(raw),
            unit_count=self._parse_int(raw.get("units", raw.get("unit_count"))),

            # Parties
            owner_name=raw.get("owner_name", raw.get("applicant_name")),
            contractor_name=raw.get("contractor_name", raw.get("contractor")),
        )

    # === Field Mapping Helpers ===

    @staticmethod
    def _parse_date(val) -> Optional[date]:
        if not val:
            return None
        try:
            if "T" in str(val):
                return datetime.fromisoformat(str(val).replace("Z", "")).date()
            return date.fromisoformat(str(val)[:10])
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_float(val) -> Optional[float]:
        try:
            return float(val) if val else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_decimal(val) -> Optional[Decimal]:
        if not val:
            return None
        try:
            cleaned = str(val).replace("$", "").replace(",", "").strip()
            return Decimal(cleaned) if cleaned else None
        except Exception:
            return None

    @staticmethod
    def _parse_int(val) -> Optional[int]:
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None

    def _classify_permit_type(self, raw: dict) -> PermitType:
        desc = (
            raw.get("permit_type", "") + " " +
            raw.get("description", "") + " " +
            raw.get("work_type", "")
        ).lower()

        if any(kw in desc for kw in ["new construct", "new build", "ground up", "new dwelling"]):
            return PermitType.NEW_CONSTRUCTION
        if any(kw in desc for kw in ["demolit", "raze", "tear down"]):
            return PermitType.DEMOLITION
        if any(kw in desc for kw in ["renovati", "rehab", "remodel", "alteration", "gut"]):
            return PermitType.RENOVATION
        if any(kw in desc for kw in ["addition", "extend", "expand"]):
            return PermitType.ADDITION
        return PermitType.OTHER

    def _classify_status(self, raw: dict) -> PermitStatus:
        status = (raw.get("status", raw.get("permit_status", ""))).lower()
        mapping = {
            "filed": PermitStatus.FILED, "pending": PermitStatus.FILED,
            "approved": PermitStatus.APPROVED, "issued": PermitStatus.APPROVED,
            "active": PermitStatus.ACTIVE, "in progress": PermitStatus.ACTIVE,
            "final": PermitStatus.FINAL, "complete": PermitStatus.FINAL,
            "closed": PermitStatus.FINAL,
            "expired": PermitStatus.EXPIRED, "void": PermitStatus.REVOKED,
        }
        for keyword, permit_status in mapping.items():
            if keyword in status:
                return permit_status
        return PermitStatus.FILED

    def _classify_property_type(self, raw: dict) -> PropertyType:
        desc = (
            raw.get("property_type", "") + " " +
            raw.get("description", "") + " " +
            raw.get("use_type", "")
        ).lower()

        if any(kw in desc for kw in ["single", "1 family", "one family", "sfr"]):
            return PropertyType.SINGLE_FAMILY
        if any(kw in desc for kw in ["duplex", "2 family", "two family", "2-unit"]):
            return PropertyType.DUPLEX
        if any(kw in desc for kw in ["triplex", "3 family", "three family", "3-unit"]):
            return PropertyType.TRIPLEX
        if any(kw in desc for kw in ["fourplex", "4 family", "four family", "4-unit", "quadplex"]):
            return PropertyType.FOURPLEX
        if any(kw in desc for kw in ["multi", "5+", "apartment", "condo"]):
            return PropertyType.MULTI_5PLUS
        if any(kw in desc for kw in ["mixed", "retail", "commercial residential"]):
            return PropertyType.MIXED_USE
        return PropertyType.UNKNOWN
