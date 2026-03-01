"""
TILT Lead Scoring Engine
5-dimension weighted composite scoring model.
Produces score 0-100 and tier assignment T1-T4.
"""
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from loguru import logger

from openshovels.schema import (
    StandardPermit, ProjectClassification, PropertyType, PermitStatus
)


@dataclass
class ScoreComponents:
    project_fit: float = 0.0       # 0-30 (weight: 30%)
    borrower_profile: float = 0.0  # 0-25 (weight: 25%)
    market_strength: float = 0.0   # 0-20 (weight: 20%)
    timing_signal: float = 0.0     # 0-15 (weight: 15%)
    conversion_prob: float = 0.0   # 0-10 (weight: 10%)

    @property
    def composite(self) -> float:
        return round(
            self.project_fit +
            self.borrower_profile +
            self.market_strength +
            self.timing_signal +
            self.conversion_prob,
            1
        )

    @property
    def tier(self) -> int:
        score = self.composite
        if score >= 80:
            return 1  # HOT
        elif score >= 60:
            return 2  # WARM
        elif score >= 40:
            return 3  # NURTURE
        else:
            return 4  # MONITOR

    @property
    def tier_name(self) -> str:
        return {1: "HOT", 2: "WARM", 3: "NURTURE", 4: "MONITOR"}[self.tier]

    @property
    def sla(self) -> str:
        return {1: "1 hour", 2: "4 hours", 3: "48 hours", 4: "Weekly batch"}[self.tier]


@dataclass
class ScoredLead:
    """A fully scored lead ready for GHL push."""
    permit: StandardPermit
    scores: ScoreComponents
    composite_score: float = 0.0
    tier: int = 4
    tier_name: str = "MONITOR"
    sla: str = "Weekly batch"
    scored_at: str = ""

    def __post_init__(self):
        self.composite_score = self.scores.composite
        self.tier = self.scores.tier
        self.tier_name = self.scores.tier_name
        self.sla = self.scores.sla
        self.scored_at = date.today().isoformat()


# === Market Data (would come from CoStar/Arbor in production) ===
# Placeholder — replace with API calls or cached data
MARKET_SCORES = {
    # city: (rent_growth_pct, vacancy_pct, permit_pipeline_trend)
    "Hartford": (3.2, 4.8, "declining"),
    "New Haven": (2.8, 5.1, "stable"),
    "Stamford": (4.1, 3.2, "declining"),
    "Bridgeport": (2.1, 6.5, "stable"),
    "Providence": (3.5, 4.2, "declining"),
    "Warwick": (2.4, 5.5, "stable"),
    "Boston": (4.8, 2.9, "increasing"),
    "Worcester": (3.9, 3.8, "declining"),
    "Springfield": (1.8, 7.2, "stable"),
    # Defaults for unknown markets
    "_default": (2.5, 5.0, "stable"),
}


def score_project_fit(permit: StandardPermit) -> float:
    """
    Score 0-30: How well does this project match TILT criteria?
    """
    score = 0.0

    # Unit count (0-8 points)
    units = permit.unit_count or permit.ai_unit_count_estimated
    if units:
        if 1 <= units <= 4:
            score += 8.0  # Small multifamily sweet spot
        elif 5 <= units <= 10:
            score += 7.0  # Mid multifamily (LARGE tier)
        elif 11 <= units <= 50:
            score += 6.0  # Larger multifamily (INSTITUTIONAL tier)
        elif units > 50:
            score += 2.0  # Very large — possible but outside core

    # Project classification (0-8 points)
    classification = permit.ai_project_classification
    if classification == ProjectClassification.GROUND_UP:
        score += 8.0
    elif classification == ProjectClassification.HEAVY_REHAB:
        score += 7.0
    elif classification == ProjectClassification.GUT_RENOVATION:
        score += 6.0
    elif classification == ProjectClassification.MODERATE_REHAB:
        score += 3.0

    # Value range (0-8 points) — all tiers $2M-$100M qualify
    value = permit.ai_value_estimated or permit.job_value
    if value:
        val = float(value)
        if 5_000_000 <= val <= 15_000_000:
            score += 8.0  # CORE — sweet spot
        elif 2_000_000 <= val < 5_000_000:
            score += 7.0  # SMALL — bread and butter rehab
        elif 15_000_000 < val <= 50_000_000:
            score += 7.0  # LARGE — bigger multifamily/mixed
        elif 50_000_000 < val <= 100_000_000:
            score += 6.0  # INSTITUTIONAL — fewer but huge fees
        elif 1_000_000 <= val < 2_000_000:
            score += 2.0  # Below minimum but worth watching

    # NOO confirmation (0-3 points)
    if permit.ai_is_investor_noo is True:
        score += 3.0
    elif permit.ai_is_investor_noo is None:
        score += 1.0  # Unknown — partial credit

    # Permit status (0-3 points)
    if permit.permit_status == PermitStatus.APPROVED:
        score += 3.0
    elif permit.permit_status == PermitStatus.ACTIVE:
        score += 2.5
    elif permit.permit_status == PermitStatus.FILED:
        score += 1.5

    return min(score, 30.0)


def score_borrower_profile(permit: StandardPermit) -> float:
    """
    Score 0-25: Borrower experience and entity structure.
    In production, enriched with ATTOM property ownership data.
    """
    score = 0.0

    # Entity structure (0-8 points)
    entity = (permit.owner_entity or permit.owner_name or "").lower()
    if any(kw in entity for kw in ["llc", "inc", "corp", "lp", "trust", "ventures", "capital", "holdings", "development", "properties", "group", "builders"]):
        score += 8.0  # Entity = investor
    elif entity:
        score += 3.0  # Individual name — might be investor

    # Known portfolio (0-8 points) — requires enrichment
    # Placeholder: would query ATTOM for # of properties owned by entity
    # For now, give partial credit if entity looks institutional
    if any(kw in entity for kw in ["development", "capital", "holdings", "group", "ventures"]):
        score += 6.0  # Likely experienced
    elif any(kw in entity for kw in ["llc", "inc", "properties"]):
        score += 3.0  # Likely investor, unknown experience

    # Contractor relationship (0-5 points)
    if permit.contractor_name:
        score += 3.0  # Has contractor = more serious
    if permit.architect_name:
        score += 2.0  # Has architect = professional project

    # Geographic concentration (0-4 points)
    # Placeholder: would check if owner has other permits in same area
    score += 2.0  # Baseline

    return min(score, 25.0)


def score_market_strength(permit: StandardPermit) -> float:
    """
    Score 0-20: Location fundamentals.
    """
    score = 0.0
    city = permit.city
    market = MARKET_SCORES.get(city, MARKET_SCORES["_default"])
    rent_growth, vacancy, pipeline = market

    # Rent growth (0-7 points)
    if rent_growth >= 4.0:
        score += 7.0
    elif rent_growth >= 3.0:
        score += 5.0
    elif rent_growth >= 2.0:
        score += 3.0
    else:
        score += 1.0

    # Vacancy (0-7 points) — lower = better
    if vacancy <= 3.0:
        score += 7.0
    elif vacancy <= 5.0:
        score += 5.0
    elif vacancy <= 7.0:
        score += 3.0
    else:
        score += 1.0

    # Permit pipeline trend (0-6 points)
    if pipeline == "declining":
        score += 6.0  # Less competition
    elif pipeline == "stable":
        score += 3.0
    else:
        score += 1.0  # Increasing = more competition

    return min(score, 20.0)


def score_timing_signal(permit: StandardPermit) -> float:
    """
    Score 0-15: Urgency indicators.
    """
    score = 0.0
    today = date.today()

    # Permit expiration urgency (0-8 points)
    if permit.expiration_date:
        days_until_expiry = (permit.expiration_date - today).days
        if days_until_expiry < 30:
            score += 8.0  # Critical urgency
        elif days_until_expiry < 60:
            score += 6.0
        elif days_until_expiry < 90:
            score += 4.0
        elif days_until_expiry < 180:
            score += 2.0

    # Recency of permit (0-4 points)
    permit_date = permit.issued_date or permit.filed_date
    if permit_date:
        age_days = (today - permit_date).days
        if age_days <= 7:
            score += 4.0   # Brand new
        elif age_days <= 30:
            score += 3.0
        elif age_days <= 90:
            score += 2.0
        elif age_days <= 180:
            score += 1.0

    # Seasonal bonus (0-3 points)
    month = today.month
    if month in (2, 3, 4, 5):  # Spring permits → summer build
        score += 3.0
    elif month in (6, 7, 8):
        score += 2.0
    elif month in (9, 10, 11):
        score += 1.0

    return min(score, 15.0)


def score_conversion_probability(permit: StandardPermit) -> float:
    """
    Score 0-10: How likely is this lead to convert?
    """
    score = 3.0  # Baseline for permit-triggered leads

    # AI confidence boost
    if permit.ai_confidence and permit.ai_confidence >= 0.8:
        score += 2.0
    elif permit.ai_confidence and permit.ai_confidence >= 0.6:
        score += 1.0

    # Data completeness boost
    fields_present = sum([
        bool(permit.owner_name or permit.owner_entity),
        bool(permit.contractor_name),
        bool(permit.job_value or permit.ai_value_estimated),
        bool(permit.unit_count or permit.ai_unit_count_estimated),
        bool(permit.ai_is_investor_noo is not None),
    ])
    score += min(fields_present, 5) * 1.0  # Up to 5 points for completeness

    return min(score, 10.0)


def score_lead(permit: StandardPermit) -> ScoredLead:
    """Full composite scoring for a single permit."""
    components = ScoreComponents(
        project_fit=score_project_fit(permit),
        borrower_profile=score_borrower_profile(permit),
        market_strength=score_market_strength(permit),
        timing_signal=score_timing_signal(permit),
        conversion_prob=score_conversion_probability(permit),
    )
    return ScoredLead(permit=permit, scores=components)


def score_batch(permits: list[StandardPermit]) -> list[ScoredLead]:
    """Score a batch and sort by composite score descending."""
    scored = [score_lead(p) for p in permits]
    scored.sort(key=lambda x: x.composite_score, reverse=True)

    # Log tier distribution
    tiers = {1: 0, 2: 0, 3: 0, 4: 0}
    for s in scored:
        tiers[s.tier] += 1
    logger.info(
        f"Scored {len(scored)} leads: "
        f"T1={tiers[1]} T2={tiers[2]} T3={tiers[3]} T4={tiers[4]}"
    )
    return scored
