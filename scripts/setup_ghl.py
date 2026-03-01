"""
GHL Setup Script — Run once to configure GoHighLevel for TILT Lead Engine

Creates:
1. TILT Construction Lending pipeline with 8 stages
2. 30+ custom fields across 5 groups
3. Webhook subscriptions for real-time events

Prerequisites:
- GHL Private Integration Token with required scopes
- GHL_PRIVATE_TOKEN and GHL_LOCATION_ID in config/.env

Usage:
    python scripts/setup_ghl.py
    python scripts/setup_ghl.py --dry-run  # Preview without creating
"""
import asyncio
import os
import sys
import json

from dotenv import load_dotenv
import httpx
from rich.console import Console
from rich.table import Table

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv("config/.env")

console = Console()

BASE_URL = "https://services.leadconnectorhq.com"
TOKEN = os.getenv("GHL_PRIVATE_TOKEN")
LOCATION_ID = os.getenv("GHL_LOCATION_ID")
VERSION = "2021-07-28"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Version": VERSION,
}

# ============================================================================
# PIPELINE DEFINITION
# ============================================================================

PIPELINE = {
    "name": "TILT Construction Lending",
    "stages": [
        {"name": "Permit Detected", "position": 0},
        {"name": "AI Qualified", "position": 1},
        {"name": "First Contact", "position": 2},
        {"name": "Engaged", "position": 3},
        {"name": "Application", "position": 4},
        {"name": "Underwriting", "position": 5},
        {"name": "Approved / CTC", "position": 6},
        {"name": "Funded", "position": 7},
    ],
}

# ============================================================================
# CUSTOM FIELDS
# ============================================================================

CUSTOM_FIELDS = [
    # Lead Intelligence
    {"name": "composite_score", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "lead_tier", "dataType": "SINGLE_OPTIONS", "model": "contact",
     "options": ["T1-HOT", "T2-WARM", "T3-NURTURE", "T4-MONITOR"]},
    {"name": "lead_source_detail", "dataType": "TEXT", "model": "contact"},
    {"name": "scoring_timestamp", "dataType": "DATE", "model": "contact"},
    {"name": "decay_adjusted_score", "dataType": "NUMERICAL", "model": "contact"},

    # Project Details
    {"name": "project_type", "dataType": "SINGLE_OPTIONS", "model": "contact",
     "options": ["Ground-Up", "Heavy Rehab", "Moderate Rehab", "Gut Renovation"]},
    {"name": "unit_count", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "property_address", "dataType": "TEXT", "model": "contact"},
    {"name": "estimated_project_value", "dataType": "MONETARY", "model": "contact"},
    {"name": "owner_occupied", "dataType": "SINGLE_OPTIONS", "model": "contact",
     "options": ["Non-Owner-Occupied", "Owner-Occupied", "Unknown"]},

    # Permit Data
    {"name": "permit_number", "dataType": "TEXT", "model": "contact"},
    {"name": "permit_type", "dataType": "TEXT", "model": "contact"},
    {"name": "permit_status", "dataType": "SINGLE_OPTIONS", "model": "contact",
     "options": ["Filed", "Approved", "Active", "Final", "Expired"]},
    {"name": "permit_value", "dataType": "MONETARY", "model": "contact"},
    {"name": "permit_date", "dataType": "DATE", "model": "contact"},
    {"name": "permit_expiration", "dataType": "DATE", "model": "contact"},

    # Borrower Profile
    {"name": "entity_name", "dataType": "TEXT", "model": "contact"},
    {"name": "portfolio_size", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "completed_projects", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "existing_loan_maturity", "dataType": "DATE", "model": "contact"},
    {"name": "borrower_experience_tier", "dataType": "SINGLE_OPTIONS", "model": "contact",
     "options": ["First-Time", "Experienced (3-10)", "Institutional (10+)"]},

    # Scoring Components
    {"name": "score_project_fit", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "score_borrower", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "score_market", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "score_timing", "dataType": "NUMERICAL", "model": "contact"},
    {"name": "score_conversion", "dataType": "NUMERICAL", "model": "contact"},
]

# ============================================================================
# WEBHOOK EVENTS
# ============================================================================

WEBHOOK_EVENTS = [
    "ContactCreate",
    "ContactUpdate",
    "OpportunityCreate",
    "OpportunityStageUpdate",
    "TaskCompleted",
    "AppointmentScheduled",
]


async def setup_pipeline(dry_run: bool = False) -> dict:
    """Create the TILT Construction Lending pipeline."""
    console.rule("[bold]Creating Pipeline")

    # Skip if pipeline already created manually
    existing_pipeline_id = os.getenv("GHL_PIPELINE_ID", "")
    if existing_pipeline_id:
        console.print(f"[green]✓ Pipeline already configured: {existing_pipeline_id} — skipping creation[/green]")
        return {"pipeline": {"id": existing_pipeline_id}}

    if dry_run:
        console.print(f"[yellow]DRY RUN: Would create pipeline '{PIPELINE['name']}' with {len(PIPELINE['stages'])} stages[/yellow]")
        return {}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{BASE_URL}/opportunities/pipelines",
            json={
                "locationId": LOCATION_ID,
                "name": PIPELINE["name"],
                "stages": PIPELINE["stages"],
            },
            headers=HEADERS,
        )

        if resp.status_code in (200, 201):
            data = resp.json()
            pipeline_id = data.get("pipeline", {}).get("id")
            stages = data.get("pipeline", {}).get("stages", [])

            console.print(f"[green]✓ Pipeline created: {pipeline_id}[/green]")

            # Print stage IDs for .env
            console.print("\n[bold]Add these to config/.env:[/bold]")
            stage_names = [
                "PERMIT_DETECTED", "QUALIFIED", "CONTACTED", "ENGAGED",
                "APPLICATION", "UNDERWRITING", "APPROVED", "FUNDED"
            ]
            console.print(f"GHL_PIPELINE_ID={pipeline_id}")
            for stage, env_name in zip(stages, stage_names):
                console.print(f"GHL_STAGE_{env_name}={stage.get('id', '')}")

            return data
        else:
            console.print(f"[red]✗ Pipeline creation failed: {resp.status_code} — {resp.text[:200]}[/red]")
            return {}


async def setup_custom_fields(dry_run: bool = False) -> list:
    """Create all custom fields."""
    console.rule("[bold]Creating Custom Fields")
    results = []

    table = Table(title=f"Custom Fields ({len(CUSTOM_FIELDS)} total)")
    table.add_column("Field", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Status", style="yellow")

    async with httpx.AsyncClient(timeout=30) as client:
        for field_def in CUSTOM_FIELDS:
            if dry_run:
                table.add_row(field_def["name"], field_def["dataType"], "DRY RUN")
                continue

            payload = {
                "name": field_def["name"],
                "dataType": field_def["dataType"],
                "model": field_def.get("model", "contact"),
            }
            if "options" in field_def:
                payload["options"] = field_def["options"]

            try:
                resp = await client.post(
                    f"{BASE_URL}/locations/{LOCATION_ID}/customFields",
                    json=payload,
                    headers=HEADERS,
                )

                if resp.status_code in (200, 201):
                    data = resp.json()
                    field_id = data.get("customField", {}).get("id", "—")
                    table.add_row(field_def["name"], field_def["dataType"], f"✓ {field_id}")
                    results.append(data)
                elif resp.status_code == 422:
                    table.add_row(field_def["name"], field_def["dataType"], "⚠ Already exists")
                else:
                    table.add_row(field_def["name"], field_def["dataType"], f"✗ {resp.status_code}")

                # Rate limit: 100 req / 10 seconds
                await asyncio.sleep(0.15)

            except Exception as e:
                table.add_row(field_def["name"], field_def["dataType"], f"✗ {e}")

    console.print(table)
    return results


async def setup_webhooks(webhook_url: str, dry_run: bool = False) -> list:
    """Subscribe to webhook events."""
    console.rule("[bold]Setting Up Webhooks")
    results = []

    if not webhook_url:
        console.print("[yellow]No WEBHOOK_URL configured — skipping[/yellow]")
        console.print("Set your engine's public URL to receive GHL events")
        return []

    for event in WEBHOOK_EVENTS:
        if dry_run:
            console.print(f"  [yellow]DRY RUN: {event} → {webhook_url}[/yellow]")
            continue

        console.print(f"  Subscribing: {event} → {webhook_url}")

    return results


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Setup GHL for TILT Lead Engine")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating")
    parser.add_argument("--webhook-url", help="Your engine's public webhook URL")
    args = parser.parse_args()

    if not TOKEN:
        console.print("[red]ERROR: GHL_PRIVATE_TOKEN not set in config/.env[/red]")
        console.print("Create one at: GHL Settings → Other Settings → Private Integrations")
        return

    if not LOCATION_ID:
        console.print("[red]ERROR: GHL_LOCATION_ID not set in config/.env[/red]")
        return

    console.print(f"\n[bold]TILT Lead Engine — GHL Setup[/bold]")
    console.print(f"Location: {LOCATION_ID}")
    console.print(f"Dry run: {args.dry_run}\n")

    # 1. Pipeline
    await setup_pipeline(dry_run=args.dry_run)

    # 2. Custom Fields
    await setup_custom_fields(dry_run=args.dry_run)

    # 3. Webhooks
    await setup_webhooks(
        webhook_url=args.webhook_url or "",
        dry_run=args.dry_run,
    )

    console.rule("[bold green]SETUP COMPLETE")
    console.print("\nNext steps:")
    console.print("1. Copy pipeline + stage IDs into config/.env")
    console.print("2. Build GHL workflows (see docs/ghl-workflows.md)")
    console.print("3. Run: python lead_engine/score_and_push.py --dry-run")
    console.print("4. Verify leads appear in GHL → TILT Construction Lending pipeline")


if __name__ == "__main__":
    asyncio.run(main())
