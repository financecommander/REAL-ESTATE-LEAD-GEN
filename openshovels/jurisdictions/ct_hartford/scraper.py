"""
OpenShovels — Hartford, CT Jurisdiction Scraper
Source: Hartford Open Data / ArcGIS Feature Server
Dataset: Building Permits 20200101 to Current (34,194 records)
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


HARTFORD_FEATURE_SERVER = (
    "https://utility.arcgis.com/usrsvcs/servers/"
    "d595ae995fb049d3ac54919ebf24b1ac/rest/services/"
    "HartfordOpenDataTables/FeatureServer/0"
)


class HartfordScraper(JurisdictionScraper):
    jurisdiction_code = "ct_hartford"
    jurisdiction_name = "Hartford, CT"
    data_source_url = HARTFORD_FEATURE_SERVER
    data_source_type = "arcgis"

    async def fetch_permits(
        self,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """Fetch from Hartford's ArcGIS Feature Server."""
        where = "Total_Construction_Cost > 2000000"
        if since:
            since_str = since.strftime("%Y-%m-%d")
            where += f" AND DATE_OPENED >= '{since_str}'"

        result_limit = limit or 2000
        all_records = []
        offset = 0

        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            while True:
                params = {
                    "where": where,
                    "outFields": "*",
                    "resultRecordCount": min(result_limit - len(all_records), 2000),
                    "resultOffset": offset,
                    "orderByFields": "DATE_OPENED DESC",
                    "f": "json",
                }

                try:
                    resp = await client.get(
                        f"{HARTFORD_FEATURE_SERVER}/query",
                        params=params,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    if "error" in data:
                        logger.error(f"[ct_hartford] ArcGIS error: {data['error']}")
                        break

                    features = data.get("features", [])
                    if not features:
                        break

                    records = [f.get("attributes", {}) for f in features]
                    all_records.extend(records)

                    logger.info(
                        f"[ct_hartford] Fetched {len(records)} records "
                        f"(total: {len(all_records)})"
                    )

                    if len(all_records) >= result_limit:
                        break
                    if not data.get("exceededTransferLimit", False):
                        break

                    offset += len(records)

                except httpx.HTTPError as e:
                    logger.error(f"[ct_hartford] ArcGIS fetch failed: {e}")
                    break

        logger.info(f"[ct_hartford] Total records fetched: {len(all_records)}")
        return all_records

    def normalize(self, raw: dict) -> StandardPermit:
        """Normalize Hartford ArcGIS permit data into StandardPermit."""
        return StandardPermit(
            permit_id=raw.get("RECORD_ID", "UNKNOWN"),
            jurisdiction=self.jurisdiction_code,
            source=DataSource.OPEN_DATA_PORTAL,

            filed_date=self._parse_date(raw.get("DATE_OPENED")),
            issued_date=self._parse_date(raw.get("DateIssued")),
            expiration_date=self._parse_date(raw.get("DATE_CLOSED")),

            address=raw.get("Location", raw.get("PROPERTY_ADDRESS", "")),
            city=raw.get("PROPERTY_CITY", "Hartford"),
            state=raw.get("PROPERTY_STATE", "CT"),
            zip_code=raw.get("PROPERTY_ZIP", ""),

            permit_type=self._classify_permit_type(raw),
            permit_status=self._classify_status(raw),
            description=raw.get("DESCRIPTION") or raw.get("B1_APP_TYPE_ALIAS", ""),
            job_value=self._parse_decimal(raw.get("Total_Construction_Cost")),

            property_type=self._classify_property_type(raw),
            unit_count=self._parse_int(raw.get("UNIT")),

            owner_name=raw.get("ASSIGNED_TO"),
        )

    @staticmethod
    def _parse_date(val) -> Optional[date]:
        if not val:
            return None
        try:
            s = str(val).strip()
            if s.isdigit() and len(s) > 10:
                return datetime.fromtimestamp(int(s) / 1000).date()
            if "T" in s:
                return datetime.fromisoformat(s.replace("Z", "")).date()
            return date.fromisoformat(s[:10])
        except (ValueError, TypeError, OSError):
            return None

    @staticmethod
    def _parse_decimal(val) -> Optional[Decimal]:
        if not val:
            return None
        try:
            cleaned = str(val).replace("$", "").replace(",", "").strip()
            d = Decimal(cleaned) if cleaned else None
            return d if d and d > 0 else None
        except Exception:
            return None

    @staticmethod
    def _parse_int(val) -> Optional[int]:
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None

    def _classify_permit_type(self, raw: dict) -> PermitType:
        desc = " ".join(filter(None, [
            raw.get("B1_APP_TYPE_ALIAS", ""),
            raw.get("RECORD_TYPE_TYPE", ""),
            raw.get("DESCRIPTION", ""),
        ])).lower()

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
        status = (raw.get("RECORD_STATUS", "")).lower()
        if any(kw in status for kw in ["closed - approved", "closed - finaled", "closed - completed"]):
            return PermitStatus.FINAL
        if "issued" in status:
            return PermitStatus.APPROVED
        if any(kw in status for kw in ["active", "in progress", "in review", "pending"]):
            return PermitStatus.ACTIVE
        if "approved" in status:
            return PermitStatus.APPROVED
        if any(kw in status for kw in ["expired", "void", "withdrawn", "denied"]):
            return PermitStatus.EXPIRED
        return PermitStatus.FILED

    def _classify_property_type(self, raw: dict) -> PropertyType:
        desc = " ".join(filter(None, [
            raw.get("B1_APP_TYPE_ALIAS", ""),
            raw.get("DESCRIPTION", ""),
        ])).lower()

        if any(kw in desc for kw in ["single", "1 family", "one family", "sfr"]):
            return PropertyType.SINGLE_FAMILY
        if any(kw in desc for kw in ["duplex", "2 family", "two family"]):
            return PropertyType.DUPLEX
        if any(kw in desc for kw in ["triplex", "3 family", "three family"]):
            return PropertyType.TRIPLEX
        if any(kw in desc for kw in ["fourplex", "4 family", "four family", "quadplex"]):
            return PropertyType.FOURPLEX
        if any(kw in desc for kw in ["multi", "5+", "apartment", "condo"]):
            return PropertyType.MULTI_5PLUS
        if any(kw in desc for kw in ["mixed", "retail"]):
            return PropertyType.MIXED_USE
        return PropertyType.UNKNOWN
