"""
GHL Integration — Push scored leads into GoHighLevel CRM
Uses Private Integration Token (API v2) for authentication.
Handles: contacts, opportunities, tags, custom fields, webhooks.
"""
import os
from typing import Optional

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from lead_engine.scoring import ScoredLead


class GHLClient:
    """
    GoHighLevel API v2 client using Private Integration Token.

    Setup:
    1. GHL Settings → Other Settings → Private Integrations
    2. Create integration: "TILT Lead Engine"
    3. Select scopes: contacts.write, opportunities.write, tags.write,
       custom-fields.write, conversations.write, calendars.write
    4. Copy token → GHL_PRIVATE_TOKEN in .env
    """

    BASE_URL = "https://services.leadconnectorhq.com"
    API_VERSION = "2021-07-28"

    def __init__(
        self,
        token: Optional[str] = None,
        location_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
    ):
        self.token = token or os.getenv("GHL_PRIVATE_TOKEN")
        self.location_id = location_id or os.getenv("GHL_LOCATION_ID")
        self.pipeline_id = pipeline_id or os.getenv("GHL_PIPELINE_ID")

        if not self.token:
            raise ValueError(
                "GHL_PRIVATE_TOKEN required. Create at: "
                "GHL Settings → Other Settings → Private Integrations"
            )

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Version": self.API_VERSION,
        }

        # Stage IDs — set after creating pipeline in GHL
        self.stage_ids = {
            "permit_detected": os.getenv("GHL_STAGE_PERMIT_DETECTED", ""),
            "qualified": os.getenv("GHL_STAGE_QUALIFIED", ""),
            "contacted": os.getenv("GHL_STAGE_CONTACTED", ""),
            "engaged": os.getenv("GHL_STAGE_ENGAGED", ""),
            "application": os.getenv("GHL_STAGE_APPLICATION", ""),
            "underwriting": os.getenv("GHL_STAGE_UNDERWRITING", ""),
            "approved": os.getenv("GHL_STAGE_APPROVED", ""),
            "funded": os.getenv("GHL_STAGE_FUNDED", ""),
        }

    # ========================================================================
    # CONTACTS
    # ========================================================================

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def upsert_contact(self, lead: ScoredLead) -> dict:
        """
        Create or update a contact in GHL.
        Uses upsert to avoid duplicates (matches on email or phone).
        """
        permit = lead.permit

        # Parse owner name into first/last
        owner = permit.owner_entity or permit.owner_name or ""
        name_parts = owner.split() if owner else ["Unknown"]
        first_name = name_parts[0] if name_parts else "Unknown"
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Build tier tag
        tier_tags = {
            1: "T1-HOT",
            2: "T2-WARM",
            3: "T3-NURTURE",
            4: "T4-MONITOR",
        }

        # Build source tag
        source_tag = f"source-openshovels-{permit.jurisdiction}"

        # Project type tag
        project_tag = f"project-{permit.ai_project_classification.value}" if permit.ai_project_classification else "project-unknown"

        payload = {
            "locationId": self.location_id,
            "firstName": first_name,
            "lastName": last_name,
            "companyName": permit.owner_entity or "",
            "address1": permit.address,
            "city": permit.city,
            "state": permit.state,
            "postalCode": permit.zip_code or "",
            "tags": [
                tier_tags.get(lead.tier, "T4-MONITOR"),
                source_tag,
                project_tag,
                "lead-engine-auto",
                f"tier-{lead.tier}",
            ],
            "customFields": self._build_custom_fields(lead),
        }

        # Overlay skip trace contact info if available
        enrichment = permit.enrichment
        if enrichment and enrichment.skip_trace:
            st = enrichment.skip_trace

            # Use skip trace name (actual person, not entity)
            if st.first_name:
                payload["firstName"] = st.first_name
            if st.last_name:
                payload["lastName"] = st.last_name

            # Phone — use first non-DNC phone
            clear_phones = [
                p for p in st.phones
                if p.get("dnc_status") != "do_not_call"
            ]
            if clear_phones:
                payload["phone"] = clear_phones[0].get("number", "")

            # Email
            if st.emails:
                payload["email"] = st.emails[0]

            # Mailing address (investor's actual address, not property)
            if st.mailing_address:
                payload["address1"] = st.mailing_address
                if st.mailing_city:
                    payload["city"] = st.mailing_city
                if st.mailing_state:
                    payload["state"] = st.mailing_state
                if st.mailing_zip:
                    payload["postalCode"] = st.mailing_zip

            # Add enrichment-specific tags
            payload["tags"].append("has-contact-info")
            if st.dnc_checked:
                payload["tags"].append("dnc-verified")
        elif enrichment and enrichment.entity and enrichment.entity.registered_agent_name:
            # At least use the registered agent name if no skip trace
            agent = enrichment.entity.registered_agent_name
            agent_parts = agent.split()
            if agent_parts:
                payload["firstName"] = agent_parts[0]
                payload["lastName"] = " ".join(agent_parts[1:]) if len(agent_parts) > 1 else ""
            payload["tags"].append("needs-skip-trace")

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.BASE_URL}/contacts/upsert",
                json=payload,
                headers=self.headers,
            )

            if resp.status_code == 200 or resp.status_code == 201:
                data = resp.json()
                contact_id = data.get("contact", {}).get("id")
                logger.info(
                    f"GHL contact upserted: {contact_id} | "
                    f"{permit.owner_entity} | T{lead.tier} | "
                    f"Score: {lead.composite_score}"
                )
                return data
            elif resp.status_code == 429:
                logger.warning("GHL rate limited — will retry")
                raise httpx.HTTPError("Rate limited")
            else:
                logger.error(
                    f"GHL contact upsert failed: {resp.status_code} | "
                    f"{resp.text[:200]}"
                )
                resp.raise_for_status()

    # ========================================================================
    # OPPORTUNITIES
    # ========================================================================

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def create_opportunity(
        self,
        contact_id: str,
        lead: ScoredLead,
    ) -> dict:
        """Create a deal in the TILT Construction Lending pipeline."""
        permit = lead.permit

        # Determine starting stage based on tier
        if lead.tier <= 2:
            stage_id = self.stage_ids["qualified"]
        else:
            stage_id = self.stage_ids["permit_detected"]

        # Calculate monetary value
        value = permit.ai_value_estimated or permit.job_value
        monetary_value = float(value) if value else 0

        # Build opportunity name
        project_type = (
            permit.ai_project_classification.value.replace("_", " ").title()
            if permit.ai_project_classification
            else "Construction"
        )
        units = permit.unit_count or permit.ai_unit_count_estimated or "?"
        name = f"{permit.owner_entity or 'Unknown'} — {units}-Unit {project_type} {permit.city}"

        payload = {
            "pipelineId": self.pipeline_id,
            "locationId": self.location_id,
            "name": name,
            "stageId": stage_id,
            "status": "open",
            "contactId": contact_id,
            "monetaryValue": monetary_value,
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.BASE_URL}/opportunities/",
                json=payload,
                headers=self.headers,
            )

            if resp.status_code in (200, 201):
                data = resp.json()
                opp_id = data.get("opportunity", {}).get("id")
                logger.info(
                    f"GHL opportunity created: {opp_id} | "
                    f"{name} | ${monetary_value:,.0f} | "
                    f"Stage: {'qualified' if lead.tier <= 2 else 'permit_detected'}"
                )
                return data
            elif resp.status_code == 429:
                logger.warning("GHL rate limited — will retry")
                raise httpx.HTTPError("Rate limited")
            else:
                logger.error(
                    f"GHL opportunity create failed: {resp.status_code} | "
                    f"{resp.text[:200]}"
                )
                resp.raise_for_status()

    # ========================================================================
    # TAGS
    # ========================================================================

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5))
    async def apply_tags(self, contact_id: str, tags: list[str]) -> dict:
        """Apply tags to trigger GHL workflows."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self.BASE_URL}/contacts/{contact_id}/tags",
                json={"tags": tags},
                headers=self.headers,
            )
            if resp.status_code in (200, 201):
                return resp.json()
            else:
                logger.warning(f"GHL tag apply failed: {resp.status_code}")
                return {}

    # ========================================================================
    # SEARCH (dedup check)
    # ========================================================================

    async def search_contact(self, query: str) -> Optional[dict]:
        """Search for existing contact to prevent duplicates."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{self.BASE_URL}/contacts/search",
                params={"locationId": self.location_id, "query": query},
                headers=self.headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                contacts = data.get("contacts", [])
                return contacts[0] if contacts else None
            return None

    # ========================================================================
    # HELPERS
    # ========================================================================

    def _build_custom_fields(self, lead: ScoredLead) -> list[dict]:
        """Map scored lead data + enrichment to GHL custom fields."""
        permit = lead.permit
        scores = lead.scores

        fields = [
            # Lead Intelligence
            {"key": "composite_score", "value": str(lead.composite_score)},
            {"key": "lead_tier", "value": f"T{lead.tier}-{lead.tier_name}"},
            {"key": "lead_source_detail", "value": f"OpenShovels-{permit.jurisdiction}"},
            {"key": "scoring_timestamp", "value": lead.scored_at},

            # Project Details
            {"key": "project_type", "value": permit.ai_project_classification.value if permit.ai_project_classification else "unknown"},
            {"key": "unit_count", "value": str(permit.unit_count or permit.ai_unit_count_estimated or "")},
            {"key": "property_address", "value": permit.address},
            {"key": "estimated_project_value", "value": str(permit.ai_value_estimated or permit.job_value or "")},
            {"key": "owner_occupied", "value": "Non-Owner-Occupied" if permit.ai_is_investor_noo else "Unknown"},

            # Permit Data
            {"key": "permit_number", "value": permit.permit_id},
            {"key": "permit_type", "value": permit.permit_type.value},
            {"key": "permit_status", "value": permit.permit_status.value},
            {"key": "permit_value", "value": str(permit.job_value or "")},
            {"key": "permit_date", "value": str(permit.issued_date or permit.filed_date or "")},
            {"key": "permit_expiration", "value": str(permit.expiration_date or "")},

            # Borrower Profile
            {"key": "entity_name", "value": permit.owner_entity or ""},

            # Scoring Components
            {"key": "score_project_fit", "value": str(scores.project_fit)},
            {"key": "score_borrower", "value": str(scores.borrower_profile)},
            {"key": "score_market", "value": str(scores.market_strength)},
            {"key": "score_timing", "value": str(scores.timing_signal)},
            {"key": "score_conversion", "value": str(scores.conversion_prob)},
        ]

        # === Enrichment Data ===
        enrichment = permit.enrichment
        if enrichment:
            # Entity Resolution
            if enrichment.entity:
                ent = enrichment.entity
                fields.extend([
                    {"key": "entity_type", "value": ent.entity_type or ""},
                    {"key": "entity_status", "value": ent.status or ""},
                    {"key": "registered_agent", "value": ent.registered_agent_name or ""},
                    {"key": "registered_agent_address", "value": ent.registered_agent_address or ""},
                    {"key": "principal_office", "value": ent.principal_office_address or ""},
                    {"key": "entity_principals", "value": ", ".join(ent.principals[:3]) if ent.principals else ""},
                    {"key": "sos_filing_number", "value": ent.sos_filing_number or ""},
                    {"key": "entity_state", "value": ent.state_of_formation or ""},
                    {"key": "other_properties_count", "value": str(ent.other_properties_count or "")},
                    {"key": "entity_lookup_source", "value": ent.lookup_source or ""},
                ])

            # Skip Trace — Contact Info
            if enrichment.skip_trace:
                st = enrichment.skip_trace
                # Primary phone (first non-DNC)
                clear_phones = [
                    p for p in st.phones
                    if p.get("dnc_status") != "do_not_call"
                ]
                if clear_phones:
                    fields.append({"key": "skip_phone_1", "value": clear_phones[0].get("number", "")})
                    fields.append({"key": "skip_phone_1_type", "value": clear_phones[0].get("type", "")})
                if len(clear_phones) > 1:
                    fields.append({"key": "skip_phone_2", "value": clear_phones[1].get("number", "")})
                if len(clear_phones) > 2:
                    fields.append({"key": "skip_phone_3", "value": clear_phones[2].get("number", "")})

                # Emails
                if st.emails:
                    fields.append({"key": "skip_email_1", "value": st.emails[0]})
                if len(st.emails) > 1:
                    fields.append({"key": "skip_email_2", "value": st.emails[1]})

                # Mailing address
                if st.mailing_address:
                    mailing = st.mailing_address
                    if st.mailing_city:
                        mailing += f", {st.mailing_city}"
                    if st.mailing_state:
                        mailing += f", {st.mailing_state}"
                    if st.mailing_zip:
                        mailing += f" {st.mailing_zip}"
                    fields.append({"key": "skip_mailing_address", "value": mailing})

                fields.extend([
                    {"key": "skip_person_name", "value": st.person_name or ""},
                    {"key": "skip_confidence", "value": str(st.confidence_score or "")},
                    {"key": "skip_linkedin", "value": st.linkedin_url or ""},
                    {"key": "skip_dnc_checked", "value": "Yes" if st.dnc_checked else "No"},
                    {"key": "skip_source", "value": st.skip_trace_source or ""},
                ])

            # Property Intelligence
            if enrichment.property_intel:
                prop = enrichment.property_intel
                fields.extend([
                    {"key": "prop_owner_name", "value": prop.owner_name or ""},
                    {"key": "prop_assessed_value", "value": str(prop.assessed_value or "")},
                    {"key": "prop_market_value", "value": str(prop.market_value or "")},
                    {"key": "prop_last_sale_price", "value": str(prop.last_sale_price or "")},
                    {"key": "prop_last_sale_date", "value": str(prop.last_sale_date or "")},
                    {"key": "prop_mortgage_amount", "value": str(prop.mortgage_amount or "")},
                    {"key": "prop_mortgage_lender", "value": prop.mortgage_lender or ""},
                    {"key": "prop_equity_estimate", "value": str(prop.equity_estimate or "")},
                    {"key": "prop_year_built", "value": str(prop.year_built or "")},
                    {"key": "prop_zoning", "value": prop.zoning or ""},
                    {"key": "prop_rental_estimate", "value": str(prop.rental_estimate or "")},
                    {"key": "prop_arv_estimate", "value": str(prop.arv_estimate or "")},
                    {"key": "prop_noo_mailing", "value": prop.owner_mailing_address or ""},
                ])

            # News Intelligence
            if enrichment.news_intel:
                news = enrichment.news_intel
                flags = []
                if news.has_zoning_issues:
                    flags.append("ZONING")
                if news.has_community_opposition:
                    flags.append("OPPOSITION")
                if news.has_tax_incentives:
                    flags.append("INCENTIVES")
                if news.has_litigation:
                    flags.append("LITIGATION")

                fields.extend([
                    {"key": "news_flags", "value": ", ".join(flags) if flags else "None"},
                    {"key": "news_hit_count", "value": str(news.total_hits)},
                    {"key": "news_ai_summary", "value": (news.ai_summary or "")[:500]},
                ])

            # Contractor Intelligence
            if enrichment.contractor:
                gc = enrichment.contractor
                fields.extend([
                    {"key": "contractor_permits_active", "value": str(gc.active_permits_count)},
                    {"key": "contractor_total_value", "value": str(gc.total_permit_value or "")},
                    {"key": "contractor_referral_potential", "value": gc.referral_potential or ""},
                    {"key": "contractor_jurisdictions", "value": ", ".join(gc.jurisdictions_active[:5])},
                ])

            # Enrichment meta
            fields.extend([
                {"key": "enrichment_layers", "value": ", ".join(enrichment.enrichment_layers_completed)},
                {"key": "enrichment_cost", "value": f"${enrichment.enrichment_cost_usd:.4f}"},
                {"key": "has_contact_info", "value": "Yes" if enrichment.has_contact_info else "No"},
                {"key": "noo_confirmed", "value": "Yes" if enrichment.noo_confirmed else "No"},
            ])

        # Filter out empty values
        return [f for f in fields if f["value"]]


# ============================================================================
# MAIN PIPELINE: Score → Push → Tag
# ============================================================================

async def push_leads_to_ghl(leads: list[ScoredLead]) -> dict:
    """
    Push a batch of scored leads into GoHighLevel.
    Returns summary stats.
    """
    ghl = GHLClient()
    stats = {"contacts_created": 0, "opportunities_created": 0, "errors": 0}

    for lead in leads:
        try:
            # 1. Upsert contact
            contact_result = await ghl.upsert_contact(lead)
            contact_id = contact_result.get("contact", {}).get("id")
            if not contact_id:
                stats["errors"] += 1
                continue
            stats["contacts_created"] += 1

            # 2. Create opportunity (only for T1-T3)
            if lead.tier <= 3:
                await ghl.create_opportunity(contact_id, lead)
                stats["opportunities_created"] += 1

            # 3. Apply workflow-triggering tags
            tier_tags = {
                1: ["T1-HOT"],
                2: ["T2-WARM"],
                3: ["T3-NURTURE"],
                4: ["T4-MONITOR"],
            }
            await ghl.apply_tags(contact_id, tier_tags.get(lead.tier, []))

        except Exception as e:
            stats["errors"] += 1
            logger.error(
                f"GHL push failed for {lead.permit.permit_id}: {e}"
            )

    logger.info(
        f"GHL push complete: {stats['contacts_created']} contacts, "
        f"{stats['opportunities_created']} opportunities, "
        f"{stats['errors']} errors"
    )
    return stats
