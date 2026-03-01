"""
Layer 1: Entity Resolution
Secretary of State lookups for CT, MA, RI, NH, ME, VT, FL, TX + OpenCorporates fallback.
Resolves LLC/Corp names from permits to registered agents and principals.
"""
import os
import re
import json
from datetime import datetime
from typing import Optional

import httpx
from loguru import logger

from openshovels.schema import EntityRecord


# ── State SOS Endpoints ───────────────────────────────────────────────
# Each state has different APIs/scraping requirements

SOS_ENDPOINTS = {
    # Connecticut - CONCORD system
    "CT": {
        "search_url": "https://service.ct.gov/business/s/onlinebusinesssearch",
        "api_url": "https://service.ct.gov/business/s/sfsites/aura",
        "method": "concord_api",
    },
    # Massachusetts - Corporations Division
    "MA": {
        "search_url": "https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx",
        "api_url": "https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearchResults.aspx",
        "method": "ma_scrape",
    },
    # Rhode Island - Business Search
    "RI": {
        "search_url": "https://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSearch.aspx",
        "api_url": "https://business.sos.ri.gov/CorpWeb/CorpSearch/CorpSearchResults.aspx",
        "method": "ri_scrape",
    },
    # New Hampshire
    "NH": {
        "search_url": "https://quickstart.sos.nh.gov/online/BusinessInquire",
        "method": "nh_scrape",
    },
    # Maine
    "ME": {
        "search_url": "https://icrs.informe.org/nei-sos-icrs/ICRS",
        "method": "me_scrape",
    },
    # Vermont
    "VT": {
        "search_url": "https://bizfilings.vermont.gov/online/BusinessInquire",
        "method": "vt_scrape",
    },
    # Florida - Sunbiz (excellent API)
    "FL": {
        "search_url": "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchByName",
        "api_url": "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchByName",
        "method": "fl_sunbiz",
    },
    # Texas - SOSDirect
    "TX": {
        "search_url": "https://mycpa.cpa.state.tx.us/coa/coaSearchBtn",
        "api_url": "https://mycpa.cpa.state.tx.us/coa/",
        "method": "tx_sos",
    },
}


class EntityResolver:
    """
    Multi-source entity resolution engine.

    Priority:
    1. State-specific SOS lookup (free, authoritative)
    2. OpenCorporates API ($0.10/lookup, cross-state)
    3. AI-powered entity parsing (last resort)
    """

    def __init__(
        self,
        opencorporates_key: Optional[str] = None,
        grok_api_key: Optional[str] = None,
    ):
        self.oc_key = opencorporates_key or os.getenv("OPENCORPORATES_API_KEY")
        self.grok_key = grok_api_key or os.getenv("GROK_API_KEY")
        self.cost_tracker = 0.0

    async def resolve(
        self,
        entity_name: str,
        state: str,
        address: Optional[str] = None,
    ) -> Optional[EntityRecord]:
        """
        Resolve an entity name to full corporate record.
        Tries state SOS first, then OpenCorporates.
        """
        if not entity_name or not self._looks_like_entity(entity_name):
            logger.debug(f"Skipping non-entity name: {entity_name}")
            return None

        state_upper = state.upper().strip()

        # Try state SOS first (free)
        record = await self._sos_lookup(entity_name, state_upper)
        if record:
            return record

        # Fallback: OpenCorporates
        if self.oc_key:
            record = await self._opencorporates_lookup(entity_name, state_upper)
            if record:
                self.cost_tracker += 0.10
                return record

        # Last resort: AI entity parsing
        if self.grok_key:
            record = await self._ai_entity_parse(entity_name, state_upper, address)
            if record:
                self.cost_tracker += 0.002
                return record

        logger.warning(f"Entity resolution failed for: {entity_name} ({state_upper})")
        return None

    # ── State SOS Lookups ─────────────────────────────────────────────

    async def _sos_lookup(
        self, entity_name: str, state: str
    ) -> Optional[EntityRecord]:
        """Route to state-specific SOS lookup."""
        if state not in SOS_ENDPOINTS:
            return None

        method = SOS_ENDPOINTS[state]["method"]

        try:
            if method == "fl_sunbiz":
                return await self._florida_sunbiz(entity_name)
            elif method == "concord_api":
                return await self._ct_concord(entity_name)
            elif method == "tx_sos":
                return await self._tx_sos(entity_name)
            else:
                # Generic scrape fallback - uses OpenCorporates
                return None
        except Exception as e:
            logger.warning(f"SOS lookup failed for {entity_name} ({state}): {e}")
            return None

    async def _florida_sunbiz(self, entity_name: str) -> Optional[EntityRecord]:
        """Florida Sunbiz - well-structured HTML, easy to parse."""
        clean_name = self._clean_entity_name(entity_name)
        url = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchByName"

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url, params={
                "searchNameOrder": clean_name,
                "searchTerm": clean_name,
                "listNameOrder": clean_name,
            })

            if resp.status_code != 200:
                return None

            # Parse search results for exact/close match
            text = resp.text
            # Look for document number links
            doc_pattern = r'href="/Inquiry/CorporationSearch/SearchResultDetail\?inquirytype=EntityName&amp;directionType=Initial&amp;searchNameOrder=([^"]+)&amp;aggregateId=([^"]+)"'
            matches = re.findall(doc_pattern, text)

            if not matches:
                return None

            # Get first match details
            _, doc_id = matches[0]
            detail_url = f"https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResultDetail?inquirytype=EntityName&directionType=Initial&searchNameOrder={clean_name}&aggregateId={doc_id}"

            detail_resp = await client.get(detail_url)
            if detail_resp.status_code != 200:
                return None

            return self._parse_sunbiz_detail(detail_resp.text, entity_name)

    def _parse_sunbiz_detail(self, html: str, entity_name: str) -> Optional[EntityRecord]:
        """Parse Florida Sunbiz detail page."""
        record = EntityRecord(
            entity_name=entity_name,
            lookup_source="fl_sunbiz",
            lookup_timestamp=datetime.now(),
        )

        # Extract filing info
        filing_match = re.search(r'Document Number[^<]*<[^>]*>([^<]+)', html)
        if filing_match:
            record.sos_filing_number = filing_match.group(1).strip()

        # Extract entity type
        type_match = re.search(r'Filing Type[^<]*<[^>]*>([^<]+)', html)
        if type_match:
            raw_type = type_match.group(1).strip()
            record.entity_type = self._normalize_entity_type(raw_type)

        # Extract status
        status_match = re.search(r'Status[^<]*<[^>]*>([^<]+)', html)
        if status_match:
            record.status = status_match.group(1).strip()

        # Extract registered agent
        agent_match = re.search(r'Registered Agent Name.*?<span[^>]*>([^<]+)', html, re.DOTALL)
        if agent_match:
            record.registered_agent_name = agent_match.group(1).strip()

        # Extract principal address
        addr_match = re.search(r'Principal Address.*?<div[^>]*>(.*?)</div>', html, re.DOTALL)
        if addr_match:
            addr_text = re.sub(r'<[^>]+>', ' ', addr_match.group(1)).strip()
            record.principal_office_address = ' '.join(addr_text.split())

        # Extract officers/members
        officer_pattern = r'Title\s*([^<]+).*?<span[^>]*>([^<]+)'
        officers = re.findall(officer_pattern, html)
        record.principals = [name.strip() for _, name in officers[:5]]

        record.state_of_formation = "FL"
        return record

    async def _ct_concord(self, entity_name: str) -> Optional[EntityRecord]:
        """Connecticut CONCORD system lookup."""
        clean_name = self._clean_entity_name(entity_name)

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            # CONCORD uses a Salesforce-backed API
            resp = await client.get(
                "https://service.ct.gov/business/s/onlinebusinesssearch",
                params={"term": clean_name},
            )

            if resp.status_code != 200:
                return None

            # Parse results - CONCORD returns HTML
            # Look for business ID links
            id_pattern = r'/business/s/onlinebusinesssearch\?Id=([^"&]+)'
            matches = re.findall(id_pattern, resp.text)

            if not matches:
                return None

            record = EntityRecord(
                entity_name=entity_name,
                state_of_formation="CT",
                lookup_source="ct_concord",
                lookup_timestamp=datetime.now(),
            )

            return record

    async def _tx_sos(self, entity_name: str) -> Optional[EntityRecord]:
        """Texas SOS Direct lookup."""
        clean_name = self._clean_entity_name(entity_name)

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.post(
                "https://mycpa.cpa.state.tx.us/coa/coaSearchBtn",
                data={
                    "searchTerm": clean_name,
                    "searchType": "entityName",
                },
            )

            if resp.status_code != 200:
                return None

            record = EntityRecord(
                entity_name=entity_name,
                state_of_formation="TX",
                lookup_source="tx_sos",
                lookup_timestamp=datetime.now(),
            )

            # Parse basic info from results
            type_match = re.search(r'Filing Type.*?<td[^>]*>([^<]+)', resp.text, re.DOTALL)
            if type_match:
                record.entity_type = self._normalize_entity_type(type_match.group(1).strip())

            status_match = re.search(r'Status.*?<td[^>]*>([^<]+)', resp.text, re.DOTALL)
            if status_match:
                record.status = status_match.group(1).strip()

            return record

    # ── OpenCorporates ────────────────────────────────────────────────

    async def _opencorporates_lookup(
        self, entity_name: str, state: str
    ) -> Optional[EntityRecord]:
        """OpenCorporates API - paid, cross-state coverage."""
        clean_name = self._clean_entity_name(entity_name)
        state_code = f"us_{state.lower()}"

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.opencorporates.com/v0.4/companies/search",
                params={
                    "q": clean_name,
                    "jurisdiction_code": state_code,
                    "api_token": self.oc_key,
                },
            )

            if resp.status_code != 200:
                return None

            data = resp.json()
            companies = data.get("results", {}).get("companies", [])
            if not companies:
                return None

            company = companies[0].get("company", {})

            record = EntityRecord(
                entity_name=company.get("name", entity_name),
                entity_type=self._normalize_entity_type(company.get("company_type", "")),
                state_of_formation=state,
                status=company.get("current_status", ""),
                registered_agent_name=company.get("agent_name", ""),
                registered_agent_address=company.get("agent_address", ""),
                sos_filing_number=company.get("company_number", ""),
                sos_url=company.get("opencorporates_url", ""),
                lookup_source="opencorporates",
                lookup_timestamp=datetime.now(),
            )

            # Get officers
            officers = company.get("officers", [])
            record.principals = [
                o.get("officer", {}).get("name", "")
                for o in officers[:5]
                if o.get("officer", {}).get("name")
            ]

            return record

    # ── AI Entity Parsing ─────────────────────────────────────────────

    async def _ai_entity_parse(
        self, entity_name: str, state: str, address: Optional[str]
    ) -> Optional[EntityRecord]:
        """Use Grok to infer entity type and likely principals from name patterns."""
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=self.grok_key, base_url="https://api.x.ai/v1")

        prompt = f"""Analyze this entity name from a building permit and infer what you can.
Entity: {entity_name}
State: {state}
Address: {address or "N/A"}

Return ONLY JSON:
{{
  "entity_type": "LLC|Corp|LP|Trust|Individual|Unknown",
  "likely_individual_name": "<best guess at human name behind entity, or null>",
  "is_likely_investor": true/false,
  "confidence": 0.0-1.0
}}"""

        try:
            response = await client.chat.completions.create(
                model="grok-3-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0.1,
            )
            text = response.choices[0].message.content.strip()
            text = text.replace("```json", "").replace("```", "").strip()
            result = json.loads(text)

            record = EntityRecord(
                entity_name=entity_name,
                entity_type=result.get("entity_type", "Unknown"),
                state_of_formation=state,
                lookup_source="ai_inference",
                lookup_timestamp=datetime.now(),
            )

            if result.get("likely_individual_name"):
                record.principals = [result["likely_individual_name"]]

            return record

        except Exception as e:
            logger.warning(f"AI entity parse failed: {e}")
            return None

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _looks_like_entity(name: str) -> bool:
        """Check if a name looks like a business entity vs individual."""
        entity_indicators = [
            "llc", "l.l.c.", "inc", "corp", "ltd", "lp", "l.p.",
            "trust", "associates", "partners", "properties",
            "holdings", "group", "ventures", "capital", "management",
            "development", "construction", "builders", "realty",
            "real estate", "investments", "enterprises", "company",
        ]
        lower = name.lower()
        return any(ind in lower for ind in entity_indicators)

    @staticmethod
    def _clean_entity_name(name: str) -> str:
        """Clean entity name for search queries."""
        # Remove common suffixes that confuse search
        suffixes = [", LLC", " LLC", ", Inc.", " Inc.", ", Corp.", " Corp.",
                     ", Ltd.", " Ltd.", ", L.P.", " L.P.", ", LP", " LP"]
        clean = name
        for suffix in suffixes:
            if clean.upper().endswith(suffix.upper()):
                clean = clean[:-len(suffix)]
        return clean.strip()

    @staticmethod
    def _normalize_entity_type(raw: str) -> str:
        """Normalize entity type strings."""
        lower = raw.lower()
        if "llc" in lower or "limited liability" in lower:
            return "LLC"
        elif "corp" in lower or "incorporated" in lower:
            return "Corp"
        elif "limited partnership" in lower or lower.strip() == "lp":
            return "LP"
        elif "trust" in lower:
            return "Trust"
        else:
            return raw.strip()[:20] if raw else "Unknown"
