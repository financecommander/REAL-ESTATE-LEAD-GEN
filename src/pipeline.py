"""End-to-end pipeline: Open Permits → Scoring → GoHighLevel.

Usage (CLI):
    python -m src.pipeline

Or from Python:
    from src.pipeline import Pipeline
    pipeline = Pipeline()
    results = pipeline.run()
"""

import logging
import os
import sys
from typing import Any

from dotenv import load_dotenv

from src.ghl.client import GHLClient, GHLClientError
from src.permits.fetcher import PermitFetcher, PermitFetchError
from src.scoring.scorer import LeadScorer, ScoredLead

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class PipelineResult:
    """Summary of a single pipeline run."""

    def __init__(
        self,
        permits_fetched: int,
        leads_scored: int,
        leads_qualified: int,
        leads_pushed: int,
        ghl_responses: list[dict[str, Any]],
        errors: list[str],
    ) -> None:
        self.permits_fetched = permits_fetched
        self.leads_scored = leads_scored
        self.leads_qualified = leads_qualified
        self.leads_pushed = leads_pushed
        self.ghl_responses = ghl_responses
        self.errors = errors

    def __repr__(self) -> str:
        return (
            f"PipelineResult("
            f"permits_fetched={self.permits_fetched}, "
            f"leads_qualified={self.leads_qualified}, "
            f"leads_pushed={self.leads_pushed}, "
            f"errors={len(self.errors)})"
        )


class Pipeline:
    """Orchestrates the full Open Permits → Scoring → GHL pipeline.

    Parameters
    ----------
    fetcher : PermitFetcher, optional
        Permit data source.  Instantiated from environment variables when
        omitted.
    scorer : LeadScorer, optional
        Lead scoring engine.  Instantiated with defaults when omitted.
    ghl_client : GHLClient, optional
        GoHighLevel API client.  Instantiated from environment variables when
        omitted.  If ``GHL_API_KEY`` or ``GHL_LOCATION_ID`` are missing the
        pipeline will log a warning and skip the push step instead of raising.
    """

    def __init__(
        self,
        fetcher: PermitFetcher | None = None,
        scorer: LeadScorer | None = None,
        ghl_client: GHLClient | None = None,
    ) -> None:
        self.fetcher = fetcher or PermitFetcher()
        self.scorer = scorer or LeadScorer()
        self._ghl_client = ghl_client  # may be None until first run

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, status_filter: str = "open") -> PipelineResult:
        """Execute one full pipeline run.

        Args:
            status_filter: Permit status to filter on (passed through to the
                fetcher).

        Returns:
            A ``PipelineResult`` summarising what happened.
        """
        errors: list[str] = []
        ghl_responses: list[dict[str, Any]] = []
        qualified: list[ScoredLead] = []

        # ---- Step 1: Fetch permits ------------------------------------
        logger.info("Step 1/3 – Fetching open permits …")
        try:
            permits = self.fetcher.fetch(status_filter=status_filter)
        except PermitFetchError as exc:
            msg = f"Permit fetch failed: {exc}"
            logger.error(msg)
            errors.append(msg)
            return PipelineResult(
                permits_fetched=0,
                leads_scored=0,
                leads_qualified=0,
                leads_pushed=0,
                ghl_responses=[],
                errors=errors,
            )

        logger.info("  → %d permits retrieved", len(permits))

        # ---- Step 2: Score permits ------------------------------------
        logger.info("Step 2/3 – Scoring leads …")
        all_scored = self.scorer.score(permits)
        qualified = [lead for lead in all_scored if lead.is_qualified]
        logger.info(
            "  → %d scored, %d qualified", len(all_scored), len(qualified)
        )

        # ---- Step 3: Push to GHL -------------------------------------
        logger.info("Step 3/3 – Pushing qualified leads to GoHighLevel …")
        ghl = self._get_ghl_client()
        if ghl is None:
            msg = "GHL_API_KEY or GHL_LOCATION_ID not configured – skipping push step."
            logger.warning(msg)
            errors.append(msg)
        else:
            try:
                ghl_responses = ghl.push_leads(qualified)
            except GHLClientError as exc:
                msg = f"GHL push failed: {exc}"
                logger.error(msg)
                errors.append(msg)

        result = PipelineResult(
            permits_fetched=len(permits),
            leads_scored=len(all_scored),
            leads_qualified=len(qualified),
            leads_pushed=len(ghl_responses),
            ghl_responses=ghl_responses,
            errors=errors,
        )
        logger.info("Pipeline complete: %r", result)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_ghl_client(self) -> GHLClient | None:
        if self._ghl_client is not None:
            return self._ghl_client
        api_key = os.getenv("GHL_API_KEY", "")
        location_id = os.getenv("GHL_LOCATION_ID", "")
        if not api_key or not location_id:
            return None
        try:
            self._ghl_client = GHLClient(
                api_key=api_key, location_id=location_id
            )
        except GHLClientError:
            return None
        return self._ghl_client


if __name__ == "__main__":
    pipeline = Pipeline()
    result = pipeline.run()
    if result.errors:
        logger.error("Pipeline finished with errors: %s", result.errors)
        sys.exit(1)
