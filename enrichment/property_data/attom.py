"""
Layer 3: Property Intelligence
ATTOM Data API for property ownership, mortgage, equity, and market data.
County assessor fallback for tax records and mailing addresses.
"""
import os
from datetime import datetime
from decimal import Decimal
from typing import Optional

import httpx
from loguru import logger

from openshovels.schema import PropertyIntelligence


class PropertyIntel:
    """
    Property data enrichment via ATTOM Data Solutions API.
    Provides ownership, mortgage, valuation, and market data.

    ATTOM API: https://api.gateway.attomdata.com/
    Pricing: ~$0.05/record (property detail), ~$0.10/record (with AVM)
    """

    ATTOM_BASE = "https://api.gateway.attomdata.com/propertyapi/v1.0.0"

    def __init__(self, attom_api_key: Optional[str] = None):
        self.api_key = attom_api_key or os.getenv("ATTOM_API_KEY")
        self.cost_tracker = 0.0

    async def lookup(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: Optional[str] = None,
    ) -> Optional[PropertyIntelligence]:
        """
        Full property lookup: ownership, mortgage, valuation, tax.
        """
        if not self.api_key:
            logger.debug("ATTOM_API_KEY not set — skipping property lookup")
            return None

        try:
            # Step 1: Get property ID from address
            prop_data = await self._property_detail(address, city, state)
            if not prop_data:
                return None

            # Step 2: Get AVM (Automated Valuation Model)
            avm_data = await self._property_avm(address, city, state)

            # Step 3: Get sales history
            sales_data = await self._sales_history(address, city, state)

            return self._build_intel(prop_data, avm_data, sales_data, address)

        except Exception as e:
            logger.warning(f"Property intel lookup failed for {address}: {e}")
            return None

    async def _property_detail(
        self, address: str, city: str, state: str
    ) -> Optional[dict]:
        """ATTOM Property Detail API."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.ATTOM_BASE}/property/detail",
                params={
                    "address1": address,
                    "address2": f"{city}, {state}",
                },
                headers={
                    "apikey": self.api_key,
                    "Accept": "application/json",
                },
            )

            self.cost_tracker += 0.05

            if resp.status_code != 200:
                logger.debug(f"ATTOM detail {resp.status_code}: {resp.text[:200]}")
                return None

            data = resp.json()
            properties = data.get("property", [])
            return properties[0] if properties else None

    async def _property_avm(
        self, address: str, city: str, state: str
    ) -> Optional[dict]:
        """ATTOM AVM (Automated Valuation Model)."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.ATTOM_BASE}/valuation/homeequity",
                params={
                    "address1": address,
                    "address2": f"{city}, {state}",
                },
                headers={
                    "apikey": self.api_key,
                    "Accept": "application/json",
                },
            )

            self.cost_tracker += 0.05

            if resp.status_code != 200:
                return None

            data = resp.json()
            properties = data.get("property", [])
            return properties[0] if properties else None

    async def _sales_history(
        self, address: str, city: str, state: str
    ) -> Optional[dict]:
        """ATTOM Sales History."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{self.ATTOM_BASE}/sale/detail",
                params={
                    "address1": address,
                    "address2": f"{city}, {state}",
                },
                headers={
                    "apikey": self.api_key,
                    "Accept": "application/json",
                },
            )

            self.cost_tracker += 0.03

            if resp.status_code != 200:
                return None

            data = resp.json()
            properties = data.get("property", [])
            return properties[0] if properties else None

    def _build_intel(
        self,
        prop: dict,
        avm: Optional[dict],
        sales: Optional[dict],
        address: str,
    ) -> PropertyIntelligence:
        """Build PropertyIntelligence from ATTOM responses."""
        # Owner info
        owner_info = prop.get("owner", {}) or {}
        owner1 = owner_info.get("owner1", {}) or {}

        # Assessment
        assessment = prop.get("assessment", {}) or {}
        assessed = assessment.get("assessed", {}) or {}
        market = assessment.get("market", {}) or {}
        tax = assessment.get("tax", {}) or {}

        # Building
        building = prop.get("building", {}) or {}
        size = building.get("size", {}) or {}
        summary = building.get("summary", {}) or {}

        # Lot
        lot = prop.get("lot", {}) or {}

        # Mortgage
        mortgage = prop.get("mortgage", {}) or {}
        first_mortgage = mortgage.get("firstConcurrent", {}) or {}

        # AVM data
        avm_data = {}
        if avm:
            avm_data = avm.get("avm", {}) or {}

        # Sales data
        last_sale = {}
        if sales:
            sale_history = sales.get("saleHistory", []) or []
            if sale_history:
                last_sale = sale_history[0] if isinstance(sale_history, list) else {}

        # Mailing address (key NOO indicator)
        mail_addr = owner_info.get("mailingAddressOneLine", "")

        intel = PropertyIntelligence(
            property_address=address,
            owner_name=owner1.get("fullName", ""),
            owner_mailing_address=mail_addr if mail_addr else None,
            assessed_value=self._to_decimal(assessed.get("assdTtlValue")),
            market_value=self._to_decimal(market.get("mktTtlValue")),
            last_sale_date=self._parse_date(last_sale.get("saleTransDate")),
            last_sale_price=self._to_decimal(last_sale.get("saleAmt")),
            mortgage_amount=self._to_decimal(first_mortgage.get("amount")),
            mortgage_lender=first_mortgage.get("lenderName", ""),
            equity_estimate=self._to_decimal(avm_data.get("equity")),
            year_built=self._to_int(summary.get("yearBuilt")),
            lot_size_sqft=self._to_int(lot.get("lotSize1")),
            building_sqft=self._to_int(size.get("livingSize")),
            zoning=lot.get("zoningType", ""),
            tax_amount=self._to_decimal(tax.get("taxAmt")),
            rental_estimate=self._to_decimal(avm_data.get("rentalAvm")),
            arv_estimate=self._to_decimal(avm_data.get("amount", {}).get("value") if isinstance(avm_data.get("amount"), dict) else avm_data.get("amount")),
            data_source="attom",
            lookup_timestamp=datetime.now(),
        )

        return intel

    @staticmethod
    def _to_decimal(value) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    @staticmethod
    def _to_int(value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _parse_date(value) -> Optional[str]:
        if not value:
            return None
        try:
            from datetime import date as d
            # ATTOM dates are typically YYYY-MM-DD or MM/DD/YYYY
            for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                try:
                    return d.fromisoformat(value) if "-" in str(value) else None
                except Exception:
                    continue
            return None
        except Exception:
            return None
