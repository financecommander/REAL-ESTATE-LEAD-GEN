"""
TILT Qualification Filter
Determines which permits qualify for TILT construction lending.

Deal Size Tiers:
  SMALL:         $2M - $5M    (heavy rehab, small multifamily)
  CORE:          $5M - $15M   (ground-up 1-4 unit, mid-market)
  LARGE:         $15M - $50M  (large multifamily, mixed-use)
  INSTITUTIONAL: $50M - $100M (institutional scale)

All tiers qualify — scoring differentiates priority.
"""
from decimal import Decimal
from enum import Enum
from typing import Optional

from loguru import logger

from openshovels.schema import (
    StandardPermit, ProjectClassification, PropertyType
)


# === Deal Size Tiers ===
class DealTier(str, Enum):
    SMALL = "small"              # $2M - $5M
    CORE = "core"                # $5M - $15M
    LARGE = "large"              # $15M - $50M
    INSTITUTIONAL = "institutional"  # $50M - $100M
    UNKNOWN = "unknown"


# === TILT Lending Criteria ===
MIN_UNITS = 1
MAX_UNITS = 4
MIN_VALUE = Decimal("2000000")    # $2M floor
MAX_VALUE = Decimal("100000000")  # $100M ceiling

QUALIFYING_CLASSIFICATIONS = {
    ProjectClassification.GROUND_UP,
    ProjectClassification.HEAVY_REHAB,
    ProjectClassification.GUT_RENOVATION,
    ProjectClassification.MODERATE_REHAB,  # Include at $2M+ these are real deals
}

QUALIFYING_PROPERTY_TYPES = {
    PropertyType.SINGLE_FAMILY,
    PropertyType.DUPLEX,
    PropertyType.TRIPLEX,
    PropertyType.FOURPLEX,
    PropertyType.MIXED_USE,
    PropertyType.MULTI_5PLUS,  # Now qualifying — larger deals welcome
}


def classify_deal_tier(value: Optional[Decimal]) -> DealTier:
    """Classify a deal into a size tier based on estimated value."""
    if value is None:
        return DealTier.UNKNOWN
    val = float(value)
    if val < 2_000_000:
        return DealTier.UNKNOWN  # Below minimum
    elif val < 5_000_000:
        return DealTier.SMALL
    elif val < 15_000_000:
        return DealTier.CORE
    elif val < 50_000_000:
        return DealTier.LARGE
    elif val <= 100_000_000:
        return DealTier.INSTITUTIONAL
    else:
        return DealTier.UNKNOWN  # Above ceiling


def qualify_permit(permit: StandardPermit) -> StandardPermit:
    """
    Apply TILT lending criteria to a permit.
    Sets tilt_qualified, tilt_disqualify_reason, and deal tier tag.
    """
    reasons = []

    # --- Unit Count Check (relaxed for larger deals) ---
    units = permit.unit_count or permit.ai_unit_count_estimated
    value = permit.ai_value_estimated or permit.job_value
    deal_tier = classify_deal_tier(value)

    if units is not None:
        # For SMALL/CORE: enforce 1-4 unit limit
        if deal_tier in (DealTier.SMALL, DealTier.CORE, DealTier.UNKNOWN):
            if units < MIN_UNITS or units > MAX_UNITS:
                reasons.append(f"unit_count={units} (need {MIN_UNITS}-{MAX_UNITS} for {deal_tier.value} tier)")
        # For LARGE/INSTITUTIONAL: allow up to 50 units
        elif deal_tier in (DealTier.LARGE, DealTier.INSTITUTIONAL):
            if units > 50:
                reasons.append(f"unit_count={units} (max 50 for {deal_tier.value} tier)")

    # --- Property Type Check (relaxed for larger deals) ---
    if deal_tier in (DealTier.SMALL, DealTier.CORE, DealTier.UNKNOWN):
        if permit.property_type == PropertyType.COMMERCIAL:
            reasons.append("property_type=commercial (need residential/mixed-use)")
    # LARGE and INSTITUTIONAL can include commercial with residential component

    # --- Project Classification Check ---
    classification = permit.ai_project_classification
    if classification and classification not in QUALIFYING_CLASSIFICATIONS:
        reasons.append(
            f"classification={classification.value} "
            f"(need ground_up/heavy_rehab/gut_renovation/moderate_rehab)"
        )

    # --- Value Check ---
    if value is not None:
        if value < MIN_VALUE:
            reasons.append(f"value=${value:,.0f} (need >=${MIN_VALUE:,.0f})")
        elif value > MAX_VALUE:
            reasons.append(f"value=${value:,.0f} (need <=${MAX_VALUE:,.0f})")

    # --- NOO Check ---
    if permit.ai_is_investor_noo is False:
        reasons.append("owner_occupied=true (need NOO)")

    # --- Set Qualification ---
    if reasons:
        permit.tilt_qualified = False
        permit.tilt_disqualify_reason = "; ".join(reasons)
    else:
        if permit.ai_confidence and permit.ai_confidence < 0.5:
            permit.tilt_qualified = None
            permit.tilt_disqualify_reason = f"low_ai_confidence={permit.ai_confidence}"
        else:
            permit.tilt_qualified = True
            permit.tilt_disqualify_reason = None

    # Tag with deal tier for GHL routing
    if permit.tilt_qualified and deal_tier != DealTier.UNKNOWN:
        permit.ai_tags = list(set(permit.ai_tags + [f"deal-{deal_tier.value}"]))

    return permit


def filter_qualified(permits: list[StandardPermit]) -> list[StandardPermit]:
    """Filter a list of permits to only TILT-qualified ones."""
    qualified = []
    tier_counts = {t: 0 for t in DealTier}

    for permit in permits:
        permit = qualify_permit(permit)
        if permit.tilt_qualified is True:
            qualified.append(permit)
            value = permit.ai_value_estimated or permit.job_value
            tier = classify_deal_tier(value)
            tier_counts[tier] += 1

    tier_summary = " | ".join(
        f"{t.value}={c}" for t, c in tier_counts.items() if c > 0
    )

    logger.info(
        f"TILT filter: {len(qualified)}/{len(permits)} qualified "
        f"({len(qualified)/max(len(permits),1)*100:.1f}%) "
        f"[{tier_summary or 'none'}]"
    )
    return qualified
