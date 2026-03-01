"""
OpenShovels — Jurisdiction Base Class
Every jurisdiction scraper inherits from this.
"""
import abc
from datetime import datetime
from typing import Optional
from loguru import logger
from openshovels.schema import StandardPermit, PermitBatch, DataSource


class JurisdictionScraper(abc.ABC):
    """
    Base class for jurisdiction-specific scrapers.

    To add a new jurisdiction:
    1. Create a folder: jurisdictions/<state>_<city>/
    2. Create scraper.py inheriting this class
    3. Implement fetch_permits() and normalize()
    4. Add to config/jurisdictions.yaml
    """

    jurisdiction_code: str = ""  # e.g. "ct_hartford"
    jurisdiction_name: str = ""  # e.g. "Hartford, CT"
    data_source_url: str = ""    # e.g. Socrata endpoint
    data_source_type: str = ""   # "socrata", "arcgis", "html", "csv"

    @abc.abstractmethod
    async def fetch_permits(
        self,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """
        Fetch raw permit records from the jurisdiction.
        Returns list of raw dicts (jurisdiction-specific format).
        If `since` is provided, only fetch permits updated after that date.
        """
        ...

    @abc.abstractmethod
    def normalize(self, raw: dict) -> StandardPermit:
        """
        Normalize a single raw permit record into StandardPermit schema.
        This is where jurisdiction-specific field mapping happens.
        """
        ...

    async def run(
        self,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> PermitBatch:
        """Full ingestion run: fetch → normalize → return batch."""
        logger.info(f"[{self.jurisdiction_code}] Starting ingestion...")

        raw_records = await self.fetch_permits(since=since, limit=limit)
        logger.info(f"[{self.jurisdiction_code}] Fetched {len(raw_records)} raw records")

        permits = []
        errors = 0
        for raw in raw_records:
            try:
                permit = self.normalize(raw)
                permit.source = DataSource.JURISDICTION_RAW
                permit.jurisdiction = self.jurisdiction_code
                permits.append(permit)
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.warning(
                        f"[{self.jurisdiction_code}] Normalize error: {e} | "
                        f"Raw: {str(raw)[:200]}"
                    )

        logger.info(
            f"[{self.jurisdiction_code}] Normalized {len(permits)} permits "
            f"({errors} errors)"
        )

        return PermitBatch(
            jurisdiction=self.jurisdiction_code,
            record_count=len(raw_records),
            new_records=len(permits),  # Updated after DB dedup
            updated_records=0,
            permits=permits,
        )
