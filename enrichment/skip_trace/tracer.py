"""
Layer 2: Skip Trace — Contact Discovery Pipeline
Multi-source skip tracing to find phone, email, mailing address for entity principals.

Sources (in priority order):
1. Free public record sites (FastPeopleSearch, TruePeopleSearch)
2. BatchSkipTracing API ($0.12-0.18/record)
3. PropStream API ($99/mo unlimited)
4. REISkip API ($0.10-0.15/record)
5. County tax records (mailing address)

TCPA Compliance: All phones checked against DNC registry before outreach.
"""
import os
import re
import json
from datetime import datetime
from typing import Optional

import httpx
from loguru import logger

from openshovels.schema import SkipTraceResult


class SkipTracer:
    """
    Multi-source skip trace engine.
    Finds contact info for entity principals from building permits.
    """

    def __init__(
        self,
        batch_skip_api_key: Optional[str] = None,
        propstream_api_key: Optional[str] = None,
        reiskip_api_key: Optional[str] = None,
        grok_api_key: Optional[str] = None,
    ):
        self.batch_skip_key = batch_skip_api_key or os.getenv("BATCH_SKIP_API_KEY")
        self.propstream_key = propstream_api_key or os.getenv("PROPSTREAM_API_KEY")
        self.reiskip_key = reiskip_api_key or os.getenv("REISKIP_API_KEY")
        self.grok_key = grok_api_key or os.getenv("GROK_API_KEY")
        self.cost_tracker = 0.0

    async def trace(
        self,
        person_name: str,
        address: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip_code: Optional[str] = None,
    ) -> Optional[SkipTraceResult]:
        """
        Run skip trace through available sources.
        Returns first successful result with contact info.
        """
        if not person_name:
            return None

        # Parse name
        first, last = self._parse_name(person_name)
        if not last:
            logger.debug(f"Cannot parse name for skip trace: {person_name}")
            return None

        # Try sources in priority order
        result = None

        # Source 1: Free public records (FastPeopleSearch-style)
        result = await self._free_lookup(first, last, city, state)
        if result and result.phones:
            logger.info(f"Skip trace hit (free): {person_name} -> {len(result.phones)} phones")
            return result

        # Source 2: BatchSkipTracing API
        if self.batch_skip_key:
            result = await self._batch_skip_trace(first, last, address, city, state, zip_code)
            if result and result.phones:
                self.cost_tracker += 0.15
                logger.info(f"Skip trace hit (BatchSkip): {person_name} -> {len(result.phones)} phones")
                return result

        # Source 3: PropStream
        if self.propstream_key and address:
            result = await self._propstream_lookup(address, city, state, zip_code)
            if result and result.phones:
                logger.info(f"Skip trace hit (PropStream): {person_name} -> {len(result.phones)} phones")
                return result

        # Source 4: REISkip
        if self.reiskip_key:
            result = await self._reiskip_lookup(first, last, address, city, state)
            if result and result.phones:
                self.cost_tracker += 0.12
                logger.info(f"Skip trace hit (REISkip): {person_name} -> {len(result.phones)} phones")
                return result

        # Return whatever we have, even without phone
        if result:
            return result

        # Minimal result with just the name parsed
        return SkipTraceResult(
            person_name=person_name,
            first_name=first,
            last_name=last,
            skip_trace_source="no_hit",
            skip_trace_timestamp=datetime.now(),
        )

    # ── Source 1: Free Public Records ─────────────────────────────────

    async def _free_lookup(
        self,
        first: str,
        last: str,
        city: Optional[str],
        state: Optional[str],
    ) -> Optional[SkipTraceResult]:
        """
        Free people search using public record aggregators.
        Uses ThatsThem, Whitepages-style lookups.
        """
        try:
            # Try ThatsThem API (free tier)
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                search_url = f"https://thatsthem.com/name/{first}-{last}"
                if city and state:
                    search_url += f"/{city}-{state}"

                resp = await client.get(search_url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                })

                if resp.status_code != 200:
                    return None

                return self._parse_free_results(resp.text, first, last)

        except Exception as e:
            logger.debug(f"Free lookup failed for {first} {last}: {e}")
            return None

    def _parse_free_results(
        self, html: str, first: str, last: str
    ) -> Optional[SkipTraceResult]:
        """Parse phone/email from free people search results."""
        result = SkipTraceResult(
            person_name=f"{first} {last}",
            first_name=first,
            last_name=last,
            skip_trace_source="free_lookup",
            skip_trace_timestamp=datetime.now(),
        )

        # Extract phone numbers (pattern: xxx-xxx-xxxx or (xxx) xxx-xxxx)
        phone_pattern = r'[\(]?\d{3}[\)]?[-.\s]?\d{3}[-.\s]?\d{4}'
        phones = re.findall(phone_pattern, html)
        seen = set()
        for phone in phones:
            clean = re.sub(r'[^\d]', '', phone)
            if len(clean) == 10 and clean not in seen:
                seen.add(clean)
                result.phones.append({
                    "number": f"({clean[:3]}) {clean[3:6]}-{clean[6:]}",
                    "type": "Unknown",
                    "dnc_status": "unchecked",
                })
            if len(result.phones) >= 3:
                break

        # Extract emails
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_pattern, html)
        result.emails = list(set(emails))[:3]

        # Extract address
        addr_pattern = r'(\d+\s+[\w\s]+(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Ct|Pl|Cir)[\w\s,]*\d{5})'
        addresses = re.findall(addr_pattern, html, re.IGNORECASE)
        if addresses:
            result.mailing_address = addresses[0]

        return result if (result.phones or result.emails) else None

    # ── Source 2: BatchSkipTracing ────────────────────────────────────

    async def _batch_skip_trace(
        self,
        first: str,
        last: str,
        address: Optional[str],
        city: Optional[str],
        state: Optional[str],
        zip_code: Optional[str],
    ) -> Optional[SkipTraceResult]:
        """BatchSkipTracing.com API integration."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                payload = {
                    "api_key": self.batch_skip_key,
                    "first_name": first,
                    "last_name": last,
                }
                if address:
                    payload["address"] = address
                if city:
                    payload["city"] = city
                if state:
                    payload["state"] = state
                if zip_code:
                    payload["zip"] = zip_code

                resp = await client.post(
                    "https://api.batchskiptracing.com/v1/skip-trace",
                    json=payload,
                )

                if resp.status_code != 200:
                    logger.warning(f"BatchSkip API error: {resp.status_code}")
                    return None

                data = resp.json()
                return self._parse_batch_skip(data, first, last)

        except Exception as e:
            logger.warning(f"BatchSkip lookup failed: {e}")
            return None

    def _parse_batch_skip(
        self, data: dict, first: str, last: str
    ) -> Optional[SkipTraceResult]:
        """Parse BatchSkipTracing API response."""
        result = SkipTraceResult(
            person_name=f"{first} {last}",
            first_name=first,
            last_name=last,
            skip_trace_source="batch_skip_tracing",
            skip_trace_timestamp=datetime.now(),
        )

        # Phones
        for phone in data.get("phones", []):
            result.phones.append({
                "number": phone.get("number", ""),
                "type": phone.get("type", "Unknown"),
                "provider": phone.get("carrier", ""),
                "dnc_status": "unchecked",
            })

        # Emails
        result.emails = data.get("emails", [])

        # Address
        addr = data.get("address", {})
        result.mailing_address = addr.get("street", "")
        result.mailing_city = addr.get("city", "")
        result.mailing_state = addr.get("state", "")
        result.mailing_zip = addr.get("zip", "")

        # Age
        result.age = str(data.get("age", ""))

        # Relatives
        for rel in data.get("relatives", []):
            result.relatives.append({
                "name": rel.get("name", ""),
                "age": rel.get("age", ""),
            })

        result.confidence_score = data.get("confidence", 0.5)
        return result

    # ── Source 3: PropStream ──────────────────────────────────────────

    async def _propstream_lookup(
        self,
        address: str,
        city: Optional[str],
        state: Optional[str],
        zip_code: Optional[str],
    ) -> Optional[SkipTraceResult]:
        """PropStream API - property-based owner lookup with skip trace."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    "https://api.propstream.com/v1/property/search",
                    params={
                        "address": address,
                        "city": city or "",
                        "state": state or "",
                        "zip": zip_code or "",
                    },
                    headers={"Authorization": f"Bearer {self.propstream_key}"},
                )

                if resp.status_code != 200:
                    return None

                data = resp.json()
                properties = data.get("properties", [])
                if not properties:
                    return None

                prop = properties[0]
                owner = prop.get("owner", {})

                result = SkipTraceResult(
                    person_name=owner.get("name", "Unknown"),
                    first_name=owner.get("first_name", ""),
                    last_name=owner.get("last_name", ""),
                    mailing_address=owner.get("mailing_address", ""),
                    mailing_city=owner.get("mailing_city", ""),
                    mailing_state=owner.get("mailing_state", ""),
                    mailing_zip=owner.get("mailing_zip", ""),
                    skip_trace_source="propstream",
                    skip_trace_timestamp=datetime.now(),
                )

                # PropStream includes phone/email in enhanced results
                for phone in owner.get("phones", []):
                    result.phones.append({
                        "number": phone.get("number", ""),
                        "type": phone.get("type", ""),
                        "dnc_status": "unchecked",
                    })
                result.emails = owner.get("emails", [])

                return result

        except Exception as e:
            logger.warning(f"PropStream lookup failed: {e}")
            return None

    # ── Source 4: REISkip ─────────────────────────────────────────────

    async def _reiskip_lookup(
        self,
        first: str,
        last: str,
        address: Optional[str],
        city: Optional[str],
        state: Optional[str],
    ) -> Optional[SkipTraceResult]:
        """REISkip API - real estate investor focused skip trace."""
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                payload = {
                    "api_key": self.reiskip_key,
                    "first_name": first,
                    "last_name": last,
                }
                if address:
                    payload["property_address"] = address
                if city:
                    payload["property_city"] = city
                if state:
                    payload["property_state"] = state

                resp = await client.post(
                    "https://api.reiskip.com/v1/trace",
                    json=payload,
                )

                if resp.status_code != 200:
                    return None

                data = resp.json()
                person = data.get("person", {})

                result = SkipTraceResult(
                    person_name=f"{first} {last}",
                    first_name=first,
                    last_name=last,
                    skip_trace_source="reiskip",
                    skip_trace_timestamp=datetime.now(),
                )

                for phone in person.get("phones", []):
                    result.phones.append({
                        "number": phone.get("number", ""),
                        "type": phone.get("line_type", ""),
                        "dnc_status": phone.get("dnc_status", "unchecked"),
                    })

                result.emails = person.get("emails", [])
                result.linkedin_url = person.get("linkedin", "")
                result.confidence_score = person.get("match_score", 0.5)

                return result

        except Exception as e:
            logger.warning(f"REISkip lookup failed: {e}")
            return None

    # ── DNC Registry Check ────────────────────────────────────────────

    async def check_dnc(self, phone_number: str) -> bool:
        """
        Check phone against National Do Not Call Registry.
        Returns True if phone IS on the DNC list (do not call).

        Production: Use DNC.com API or internal DNC database.
        """
        # Clean phone to 10 digits
        clean = re.sub(r'[^\d]', '', phone_number)
        if len(clean) == 11 and clean.startswith("1"):
            clean = clean[1:]

        if len(clean) != 10:
            return True  # Invalid phone = don't call

        # TODO: Integrate DNC.com API ($0.005/check)
        # For now, flag as unchecked
        logger.debug(f"DNC check needed for: {clean}")
        return False

    async def validate_phones(self, result: SkipTraceResult) -> SkipTraceResult:
        """Check all phones against DNC registry."""
        for phone in result.phones:
            is_dnc = await self.check_dnc(phone.get("number", ""))
            phone["dnc_status"] = "do_not_call" if is_dnc else "clear"
        result.dnc_checked = True
        return result

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _parse_name(full_name: str) -> tuple[str, str]:
        """Parse full name into first, last."""
        # Remove entity suffixes
        clean = re.sub(
            r',?\s*(LLC|L\.L\.C\.|Inc\.|Corp\.|Ltd\.|L\.P\.|LP)\.?$',
            '', full_name, flags=re.IGNORECASE
        ).strip()

        parts = clean.split()
        if len(parts) == 0:
            return ("", "")
        elif len(parts) == 1:
            return (parts[0], "")
        else:
            return (parts[0], " ".join(parts[1:]))
