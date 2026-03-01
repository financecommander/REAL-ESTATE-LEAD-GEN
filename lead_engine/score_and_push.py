"""
TILT Construction Lead Engine — Main Orchestrator
Runs the full pipeline: Ingest → Enrich → Qualify → Score → Push to GHL

Usage:
    # One-time run
    python lead_engine/score_and_push.py

    # Run specific jurisdiction
    python lead_engine/score_and_push.py --jurisdiction ct_hartford

    # Dry run (score but don't push to GHL)
    python lead_engine/score_and_push.py --dry-run

    # Scheduled (every 15 minutes)
    python lead_engine/score_and_push.py --scheduled
"""
import asyncio
import os
import sys
import argparse
from datetime import datetime, timedelta

from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv("config/.env")

from openshovels.pipeline.enrich import PermitEnricher
from lead_engine.qualify import filter_qualified
from lead_engine.scoring import score_batch, ScoredLead
from enrichment.orchestrator import EnrichmentOrchestrator
from ghl_integration.client import push_leads_to_ghl

console = Console()


# === Jurisdiction Registry ===
JURISDICTION_SCRAPERS = {
    "ct_hartford": "openshovels.jurisdictions.ct_hartford.scraper.HartfordScraper",
    # Add as you build more scrapers:
    # "ri_providence": "openshovels.jurisdictions.ri_providence.scraper.ProvidenceScraper",
    # "ma_boston": "openshovels.jurisdictions.ma_boston.scraper.BostonScraper",
    # "ny_nyc": "openshovels.jurisdictions.ny_nyc.scraper.NYCScraper",
    # "nj_newark": "openshovels.jurisdictions.nj_newark.scraper.NewarkScraper",
}


def load_scraper(jurisdiction: str):
    """Dynamically load a jurisdiction scraper class."""
    module_path = JURISDICTION_SCRAPERS.get(jurisdiction)
    if not module_path:
        raise ValueError(
            f"Unknown jurisdiction: {jurisdiction}. "
            f"Available: {list(JURISDICTION_SCRAPERS.keys())}"
        )
    module_name, class_name = module_path.rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_name)
    return getattr(module, class_name)()


async def run_pipeline(
    jurisdictions: list[str],
    since: datetime = None,
    dry_run: bool = False,
    limit: int = None,
    skip_enrichment: bool = False,
    enrichment_layers: list[str] = None,
    enrichment_concurrency: int = 3,
) -> dict:
    """
    Full pipeline execution.

    1. INGEST — Scrape permits from each jurisdiction
    2. ENRICH (AI) — AI classification via Grok (cheap, bulk)
    3. QUALIFY — Filter to TILT lending criteria
    4. SCORE — 5-dimension composite scoring
    5. ENRICH (DEEP) — Entity resolution, skip trace, property, news, contractor
    6. PUSH — Send enriched leads to GoHighLevel CRM
    """
    stats = {
        "jurisdictions": len(jurisdictions),
        "raw_permits": 0,
        "enriched": 0,
        "qualified": 0,
        "scored": 0,
        "t1_hot": 0,
        "t2_warm": 0,
        "t3_nurture": 0,
        "t4_monitor": 0,
        "deep_enriched": 0,
        "contacts_found": 0,
        "entities_resolved": 0,
        "properties_looked_up": 0,
        "news_hits": 0,
        "enrichment_cost": 0.0,
        "ghl_contacts": 0,
        "ghl_opportunities": 0,
        "errors": 0,
    }

    all_scored: list[ScoredLead] = []

    # Initialize AI enricher
    grok_key = os.getenv("GROK_API_KEY")
    enricher = None
    if grok_key:
        enricher = PermitEnricher(api_key=grok_key, model=os.getenv("GROK_MODEL", "grok-3-mini"))
    else:
        logger.warning("GROK_API_KEY not set — skipping AI enrichment")

    for jurisdiction in jurisdictions:
        console.rule(f"[bold]{jurisdiction.upper()}")

        # --- STEP 1: INGEST ---
        try:
            scraper = load_scraper(jurisdiction)
            batch = await scraper.run(since=since, limit=limit)
            stats["raw_permits"] += batch.record_count
            logger.info(f"INGEST: {batch.record_count} raw permits from {jurisdiction}")
        except Exception as e:
            logger.error(f"INGEST FAILED for {jurisdiction}: {e}")
            stats["errors"] += 1
            continue

        if not batch.permits:
            logger.info(f"No permits found for {jurisdiction} — skipping")
            continue

        # --- STEP 2: ENRICH ---
        permits = batch.permits
        if enricher:
            permits = await enricher.enrich_batch(permits, concurrency=5)
            stats["enriched"] += len(permits)
            logger.info(f"ENRICH: {len(permits)} permits AI-classified")
        else:
            stats["enriched"] += len(permits)

        # --- STEP 3: QUALIFY ---
        qualified = filter_qualified(permits)
        stats["qualified"] += len(qualified)
        logger.info(f"QUALIFY: {len(qualified)}/{len(permits)} meet TILT criteria")

        if not qualified:
            logger.info(f"No qualified permits in {jurisdiction}")
            continue

        # --- STEP 4: SCORE ---
        scored = score_batch(qualified)
        stats["scored"] += len(scored)
        for s in scored:
            stats[f"t{s.tier}_{'hot' if s.tier==1 else 'warm' if s.tier==2 else 'nurture' if s.tier==3 else 'monitor'}"] += 1
        all_scored.extend(scored)

    # --- STEP 5: DEEP ENRICHMENT (Entity, Skip Trace, Property, News, Contractor) ---
    if all_scored and not skip_enrichment:
        console.rule("[bold blue]DEEP ENRICHMENT — Contact Discovery & Intelligence")

        # Only deep-enrich T1-T3 leads (T4 not worth the cost)
        leads_to_enrich = [s for s in all_scored if s.tier <= 3]
        t4_leads = [s for s in all_scored if s.tier > 3]

        if leads_to_enrich:
            logger.info(
                f"Deep enriching {len(leads_to_enrich)} T1-T3 leads "
                f"(skipping {len(t4_leads)} T4 leads to save cost)"
            )

            enrichment_orch = EnrichmentOrchestrator()

            # Extract permits from scored leads for enrichment
            permits_to_enrich = [s.permit for s in leads_to_enrich]

            try:
                enriched_permits = await enrichment_orch.enrich_batch(
                    permits=permits_to_enrich,
                    concurrency=enrichment_concurrency,
                    layers=enrichment_layers,
                )

                # Map enriched permits back onto scored leads
                enriched_map = {p.permit_id: p for p in enriched_permits}
                for lead in leads_to_enrich:
                    if lead.permit.permit_id in enriched_map:
                        lead.permit = enriched_map[lead.permit.permit_id]

                # Collect stats
                for p in enriched_permits:
                    if p.enrichment:
                        stats["deep_enriched"] += 1
                        if p.enrichment.has_contact_info:
                            stats["contacts_found"] += 1
                        if "entity" in p.enrichment.enrichment_layers_completed:
                            stats["entities_resolved"] += 1
                        if "property" in p.enrichment.enrichment_layers_completed:
                            stats["properties_looked_up"] += 1
                        if "news" in p.enrichment.enrichment_layers_completed:
                            news = p.enrichment.news_intel
                            if news:
                                stats["news_hits"] += news.total_hits

                orch_stats = enrichment_orch.get_stats()
                stats["enrichment_cost"] = orch_stats["total_cost"]

                logger.info(
                    f"DEEP ENRICHMENT: {stats['deep_enriched']}/{len(leads_to_enrich)} enriched | "
                    f"{stats['contacts_found']} with contact info | "
                    f"${stats['enrichment_cost']:.2f} total cost"
                )

            except Exception as e:
                logger.error(f"Deep enrichment failed: {e}")
                stats["errors"] += 1
        else:
            logger.info("No T1-T3 leads to deep enrich")
    elif skip_enrichment:
        console.print("[yellow]SKIP — Deep enrichment disabled[/yellow]")

    # --- STEP 6: PUSH TO GHL ---
    if all_scored and not dry_run:
        console.rule("[bold green]PUSHING TO GOHIGHLEVEL")
        ghl_stats = await push_leads_to_ghl(all_scored)
        stats["ghl_contacts"] = ghl_stats["contacts_created"]
        stats["ghl_opportunities"] = ghl_stats["opportunities_created"]
        stats["errors"] += ghl_stats["errors"]
    elif dry_run:
        console.print("[yellow]DRY RUN — skipping GHL push[/yellow]")

    # --- SUMMARY ---
    print_summary(stats, all_scored)
    return stats


def print_summary(stats: dict, scored: list[ScoredLead]):
    """Pretty-print pipeline results."""
    console.print()
    console.rule("[bold]PIPELINE SUMMARY")

    # Stats table
    table = Table(title="Run Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")

    table.add_row("Jurisdictions", str(stats["jurisdictions"]))
    table.add_row("Raw Permits", str(stats["raw_permits"]))
    table.add_row("AI Enriched", str(stats["enriched"]))
    table.add_row("TILT Qualified", str(stats["qualified"]))
    table.add_row("Scored", str(stats["scored"]))
    table.add_row("T1 HOT 🔥", str(stats["t1_hot"]))
    table.add_row("T2 WARM 🟠", str(stats["t2_warm"]))
    table.add_row("T3 NURTURE 📧", str(stats["t3_nurture"]))
    table.add_row("T4 MONITOR 👁", str(stats["t4_monitor"]))
    table.add_row("", "")
    table.add_row("[bold]Deep Enrichment[/bold]", "")
    table.add_row("  Leads Enriched", str(stats["deep_enriched"]))
    table.add_row("  Contacts Found 📞", str(stats["contacts_found"]))
    table.add_row("  Entities Resolved 🏢", str(stats["entities_resolved"]))
    table.add_row("  Properties Looked Up 🏠", str(stats["properties_looked_up"]))
    table.add_row("  News Hits 📰", str(stats["news_hits"]))
    table.add_row("  Enrichment Cost", f"${stats['enrichment_cost']:.2f}")
    table.add_row("", "")
    table.add_row("→ GHL Contacts", str(stats["ghl_contacts"]))
    table.add_row("→ GHL Opportunities", str(stats["ghl_opportunities"]))
    table.add_row("Errors", str(stats["errors"]))

    console.print(table)

    # Top leads table — now with contact info
    if scored:
        top_table = Table(title="Top Leads (T1-T2)")
        top_table.add_column("Score", style="bold red", justify="center")
        top_table.add_column("Tier", justify="center")
        top_table.add_column("Entity", style="white")
        top_table.add_column("Project", style="cyan")
        top_table.add_column("City", style="green")
        top_table.add_column("Value", style="yellow", justify="right")
        top_table.add_column("Contact", style="magenta")
        top_table.add_column("SLA", style="dim")

        for lead in scored[:10]:
            if lead.tier <= 2:
                value = lead.permit.ai_value_estimated or lead.permit.job_value
                # Contact info from enrichment
                contact = "—"
                if lead.permit.enrichment and lead.permit.enrichment.has_contact_info:
                    st = lead.permit.enrichment.skip_trace
                    phone_count = len(st.phones) if st else 0
                    email_count = len(st.emails) if st else 0
                    contact = f"📞{phone_count} ✉{email_count}"
                elif lead.permit.enrichment and lead.permit.enrichment.entity:
                    ent = lead.permit.enrichment.entity
                    if ent.registered_agent_name:
                        contact = f"🏢 {ent.registered_agent_name[:20]}"

                top_table.add_row(
                    str(lead.composite_score),
                    f"T{lead.tier}",
                    lead.permit.owner_entity or lead.permit.owner_name or "—",
                    lead.permit.ai_project_classification.value if lead.permit.ai_project_classification else "—",
                    lead.permit.city,
                    f"${float(value):,.0f}" if value else "—",
                    contact,
                    lead.sla,
                )

        console.print(top_table)


def main():
    parser = argparse.ArgumentParser(description="TILT Construction Lead Engine")
    parser.add_argument(
        "--jurisdiction", "-j",
        help="Run specific jurisdiction (default: all configured)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Score leads but don't push to GHL",
    )
    parser.add_argument(
        "--since",
        help="Only fetch permits since date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of permits per jurisdiction",
    )
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Run on schedule (every 15 minutes)",
    )
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip deep enrichment (entity, skip trace, property, news, contractor)",
    )
    parser.add_argument(
        "--enrich-layers",
        help="Comma-separated enrichment layers to run (default: all). "
             "Options: entity,skip_trace,property,news,contractor",
    )
    parser.add_argument(
        "--enrich-concurrency",
        type=int,
        default=3,
        help="Max concurrent enrichment tasks (default: 3)",
    )

    args = parser.parse_args()

    # Determine jurisdictions
    if args.jurisdiction:
        jurisdictions = [args.jurisdiction]
    else:
        target = os.getenv("TARGET_JURISDICTIONS", "ct_hartford")
        jurisdictions = [j.strip() for j in target.split(",")]

    # Parse since date
    since = None
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d")
    else:
        since = datetime.utcnow() - timedelta(days=7)  # Default: last 7 days

    # Parse enrichment layers
    enrich_layers = None
    if args.enrich_layers:
        enrich_layers = [l.strip() for l in args.enrich_layers.split(",")]

    if args.scheduled:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        scheduler = AsyncIOScheduler()
        interval = int(os.getenv("SCORING_INTERVAL_SECONDS", "900"))

        async def scheduled_run():
            await run_pipeline(
                jurisdictions=jurisdictions,
                since=datetime.utcnow() - timedelta(seconds=interval * 2),
                dry_run=args.dry_run,
                limit=args.limit,
                skip_enrichment=args.skip_enrichment,
                enrichment_layers=enrich_layers,
                enrichment_concurrency=args.enrich_concurrency,
            )

        scheduler.add_job(scheduled_run, "interval", seconds=interval)
        scheduler.start()
        logger.info(f"Scheduled mode: running every {interval}s")
        try:
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            scheduler.shutdown()
    else:
        asyncio.run(
            run_pipeline(
                jurisdictions=jurisdictions,
                since=since,
                dry_run=args.dry_run,
                limit=args.limit,
                skip_enrichment=args.skip_enrichment,
                enrichment_layers=enrich_layers,
                enrichment_concurrency=args.enrich_concurrency,
            )
        )


if __name__ == "__main__":
    main()
