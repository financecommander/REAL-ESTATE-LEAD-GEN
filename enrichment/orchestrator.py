"""
TILT Enrichment Orchestrator
Runs all 5 enrichment layers on qualified permits:
  1. Entity Resolution (SOS lookup)
  2. Skip Trace (contact discovery)
  3. Property Intelligence (ATTOM)
  4. News Intel (Google News + court records)
  5. Contractor Reverse Lookup

Each layer is independent and fail-safe.
Cost tracking built in for per-lead economics.
"""
import asyncio
from datetime import datetime
from typing import Optional

from loguru import logger

from openshovels.schema import StandardPermit, EnrichmentBundle
from enrichment.entity_resolution.resolver import EntityResolver
from enrichment.skip_trace.tracer import SkipTracer
from enrichment.property_data.attom import PropertyIntel
from enrichment.news_intel.search import NewsIntel
from enrichment.contractor_intel.reverse_lookup import ContractorIntel


class EnrichmentOrchestrator:
    """
    Master orchestrator for the 5-layer enrichment pipeline.

    Usage:
        orch = EnrichmentOrchestrator()
        enriched_permits = await orch.enrich_batch(qualified_permits)
    """

    def __init__(self):
        self.entity_resolver = EntityResolver()
        self.skip_tracer = SkipTracer()
        self.property_intel = PropertyIntel()
        self.news_intel = NewsIntel()
        self.contractor_intel = ContractorIntel()
        self.total_cost = 0.0

    async def enrich_permit(
        self,
        permit: StandardPermit,
        layers: Optional[list[str]] = None,
    ) -> StandardPermit:
        """
        Run enrichment layers on a single permit.

        Args:
            permit: Qualified StandardPermit
            layers: Optional list of layers to run. Default: all.
                    Options: "entity", "skip_trace", "property", "news", "contractor"
        """
        if layers is None:
            layers = ["entity", "skip_trace", "property", "news", "contractor"]

        bundle = EnrichmentBundle(
            permit_id=permit.permit_id,
            enrichment_timestamp=datetime.now(),
        )

        entity_name = permit.owner_entity or permit.owner_name
        cost = 0.0

        # ── Layer 1: Entity Resolution ────────────────────────────────
        if "entity" in layers and entity_name:
            try:
                entity = await self.entity_resolver.resolve(
                    entity_name=entity_name,
                    state=permit.state,
                    address=permit.address,
                )
                if entity:
                    bundle.entity = entity
                    bundle.enrichment_layers_completed.append("entity")
                    logger.info(
                        f"[{permit.permit_id}] Entity resolved: {entity.entity_name} "
                        f"→ {entity.entity_type} | Agent: {entity.registered_agent_name}"
                    )
            except Exception as e:
                logger.warning(f"[{permit.permit_id}] Entity resolution failed: {e}")

        # ── Layer 2: Skip Trace ───────────────────────────────────────
        if "skip_trace" in layers:
            # Determine best person to skip trace
            trace_name = None
            if bundle.entity and bundle.entity.registered_agent_name:
                trace_name = bundle.entity.registered_agent_name
            elif bundle.entity and bundle.entity.principals:
                trace_name = bundle.entity.principals[0]
            elif entity_name and not self.entity_resolver._looks_like_entity(entity_name):
                trace_name = entity_name

            if trace_name:
                try:
                    skip_result = await self.skip_tracer.trace(
                        person_name=trace_name,
                        address=permit.address,
                        city=permit.city,
                        state=permit.state,
                        zip_code=permit.zip_code,
                    )
                    if skip_result:
                        # DNC check on all phones
                        skip_result = await self.skip_tracer.validate_phones(skip_result)
                        bundle.skip_trace = skip_result
                        bundle.enrichment_layers_completed.append("skip_trace")
                        phone_count = len(skip_result.phones)
                        email_count = len(skip_result.emails)
                        logger.info(
                            f"[{permit.permit_id}] Skip trace: {trace_name} "
                            f"→ {phone_count} phones, {email_count} emails"
                        )
                except Exception as e:
                    logger.warning(f"[{permit.permit_id}] Skip trace failed: {e}")

        # ── Layer 3: Property Intelligence ────────────────────────────
        if "property" in layers and permit.address:
            try:
                prop = await self.property_intel.lookup(
                    address=permit.address,
                    city=permit.city,
                    state=permit.state,
                    zip_code=permit.zip_code,
                )
                if prop:
                    bundle.property_intel = prop
                    bundle.enrichment_layers_completed.append("property")
                    logger.info(
                        f"[{permit.permit_id}] Property intel: "
                        f"Market: ${float(prop.market_value or 0):,.0f} | "
                        f"Mortgage: ${float(prop.mortgage_amount or 0):,.0f} | "
                        f"NOO: {prop.owner_mailing_address is not None}"
                    )
            except Exception as e:
                logger.warning(f"[{permit.permit_id}] Property intel failed: {e}")

        # ── Layer 4: News Intel ───────────────────────────────────────
        if "news" in layers:
            try:
                news = await self.news_intel.search(
                    entity_name=entity_name,
                    address=permit.address,
                    city=permit.city,
                    state=permit.state,
                )
                if news and news.total_hits > 0:
                    bundle.news_intel = news
                    bundle.enrichment_layers_completed.append("news")
                    flags = []
                    if news.has_zoning_issues: flags.append("ZONING")
                    if news.has_community_opposition: flags.append("OPPOSITION")
                    if news.has_tax_incentives: flags.append("INCENTIVES")
                    if news.has_litigation: flags.append("LITIGATION")
                    logger.info(
                        f"[{permit.permit_id}] News: {news.total_hits} hits | "
                        f"Flags: {', '.join(flags) if flags else 'none'}"
                    )
            except Exception as e:
                logger.warning(f"[{permit.permit_id}] News intel failed: {e}")

        # ── Layer 5: Contractor Lookup ────────────────────────────────
        if "contractor" in layers and permit.contractor_name:
            try:
                contractor = self.contractor_intel.lookup(permit.contractor_name)
                if contractor:
                    bundle.contractor = contractor
                    bundle.enrichment_layers_completed.append("contractor")
                    logger.info(
                        f"[{permit.permit_id}] Contractor: {contractor.contractor_name} "
                        f"→ {contractor.active_permits_count} permits | "
                        f"Referral: {contractor.referral_potential}"
                    )
            except Exception as e:
                logger.warning(f"[{permit.permit_id}] Contractor lookup failed: {e}")

        # ── Cost Tracking ─────────────────────────────────────────────
        cost = (
            self.entity_resolver.cost_tracker
            + self.skip_tracer.cost_tracker
            + self.property_intel.cost_tracker
            + self.news_intel.cost_tracker
        )
        bundle.enrichment_cost_usd = round(cost, 4)
        self.total_cost += cost

        permit.enrichment = bundle
        return permit

    async def enrich_batch(
        self,
        permits: list[StandardPermit],
        concurrency: int = 3,
        layers: Optional[list[str]] = None,
    ) -> list[StandardPermit]:
        """
        Enrich a batch of permits with rate limiting.

        Args:
            permits: List of qualified StandardPermits
            concurrency: Max concurrent enrichment tasks
            layers: Optional subset of layers to run
        """
        # First, index all permits for contractor reverse lookup
        self.contractor_intel.index_permits(permits)

        semaphore = asyncio.Semaphore(concurrency)

        async def _enrich_limited(p):
            async with semaphore:
                return await self.enrich_permit(p, layers=layers)

        tasks = [_enrich_limited(p) for p in permits]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched = []
        errors = 0
        for result in results:
            if isinstance(result, StandardPermit):
                enriched.append(result)
            elif isinstance(result, Exception):
                errors += 1
                logger.error(f"Enrichment error: {result}")

        # Stats
        with_contact = sum(
            1 for p in enriched
            if p.enrichment and p.enrichment.has_contact_info
        )

        logger.info(
            f"Enrichment complete: {len(enriched)}/{len(permits)} permits | "
            f"{with_contact} with contact info | "
            f"{errors} errors | "
            f"Total cost: ${self.total_cost:.2f}"
        )

        return enriched

    def get_stats(self) -> dict:
        """Get enrichment run statistics."""
        return {
            "total_cost": round(self.total_cost, 4),
            "entity_cost": round(self.entity_resolver.cost_tracker, 4),
            "skip_trace_cost": round(self.skip_tracer.cost_tracker, 4),
            "property_cost": round(self.property_intel.cost_tracker, 4),
            "news_cost": round(self.news_intel.cost_tracker, 4),
        }
