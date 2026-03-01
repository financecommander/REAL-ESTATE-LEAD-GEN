"""Score and qualify building-permit leads.

Each permit record is converted to a ``ScoredLead`` with a numeric score
in the range 0–100.  Only leads at or above ``MIN_LEAD_SCORE`` (default 50)
are considered *qualified*.

Scoring rules
-------------
| Category              | Points |
|-----------------------|--------|
| High-value permit     |     30 |
| Medium-value permit   |     15 |
| New-construction type |     25 |
| Major-reno type       |     15 |
| Residential property  |     20 |
| Commercial property   |     10 |
| Recent permit (≤30 d) |     25 |
| Permit ≤ 90 days old  |     10 |

Maximum total: 100 points.
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Permit-value thresholds (USD)
HIGH_VALUE_THRESHOLD = 50_000
MEDIUM_VALUE_THRESHOLD = 10_000

# Permit type keywords mapped to point values
_NEW_CONSTRUCTION_KEYWORDS = {"new", "construction", "addition"}
_MAJOR_RENO_KEYWORDS = {"renovation", "remodel", "alteration"}

# Property-type keywords
_RESIDENTIAL_KEYWORDS = {"residential", "single family", "duplex", "condo", "townhouse"}
_COMMERCIAL_KEYWORDS = {"commercial", "office", "retail", "industrial", "multi-family"}

DEFAULT_MIN_SCORE = 50


@dataclass
class ScoredLead:
    """A permit record enriched with a lead quality score."""

    permit: dict[str, Any]
    score: int
    reasons: list[str] = field(default_factory=list)

    @property
    def is_qualified(self) -> bool:
        """Return True when the score meets the minimum threshold."""
        min_score = int(os.getenv("MIN_LEAD_SCORE", str(DEFAULT_MIN_SCORE)))
        return self.score >= min_score


class LeadScorer:
    """Scores permit records and returns qualified leads."""

    def score(self, permits: list[dict[str, Any]]) -> list[ScoredLead]:
        """Score every permit record and return all ``ScoredLead`` objects."""
        results = [self._score_one(p) for p in permits]
        qualified = [r for r in results if r.is_qualified]
        logger.info(
            "Scored %d permits → %d qualified leads", len(results), len(qualified)
        )
        return results

    def qualified(self, permits: list[dict[str, Any]]) -> list[ScoredLead]:
        """Return only the qualified scored leads."""
        return [lead for lead in self.score(permits) if lead.is_qualified]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _score_one(self, permit: dict[str, Any]) -> ScoredLead:
        score = 0
        reasons: list[str] = []

        score += self._score_permit_value(permit, reasons)
        score += self._score_permit_type(permit, reasons)
        score += self._score_property_type(permit, reasons)
        score += self._score_recency(permit, reasons)

        return ScoredLead(permit=permit, score=min(score, 100), reasons=reasons)

    # -- value -----------------------------------------------------------

    @staticmethod
    def _score_permit_value(
        permit: dict[str, Any], reasons: list[str]
    ) -> int:
        raw = permit.get("total_valuation") or permit.get("valuation") or 0
        try:
            value = float(str(raw).replace(",", "").replace("$", ""))
        except ValueError:
            value = 0.0

        if value >= HIGH_VALUE_THRESHOLD:
            reasons.append(f"High-value permit (${value:,.0f})")
            return 30
        if value >= MEDIUM_VALUE_THRESHOLD:
            reasons.append(f"Medium-value permit (${value:,.0f})")
            return 15
        return 0

    # -- type ------------------------------------------------------------

    @staticmethod
    def _score_permit_type(
        permit: dict[str, Any], reasons: list[str]
    ) -> int:
        permit_type = (
            permit.get("permit_type_desc")
            or permit.get("permit_class_mapped")
            or permit.get("work_description")
            or ""
        ).lower()

        if any(kw in permit_type for kw in _NEW_CONSTRUCTION_KEYWORDS):
            reasons.append("New construction permit")
            return 25
        if any(kw in permit_type for kw in _MAJOR_RENO_KEYWORDS):
            reasons.append("Major renovation permit")
            return 15
        return 0

    # -- property type ---------------------------------------------------

    @staticmethod
    def _score_property_type(
        permit: dict[str, Any], reasons: list[str]
    ) -> int:
        prop_type = (
            permit.get("property_use_code_desc")
            or permit.get("building_use")
            or permit.get("permit_class_mapped")
            or ""
        ).lower()

        if any(kw in prop_type for kw in _RESIDENTIAL_KEYWORDS):
            reasons.append("Residential property")
            return 20
        if any(kw in prop_type for kw in _COMMERCIAL_KEYWORDS):
            reasons.append("Commercial property")
            return 10
        return 0

    # -- recency ---------------------------------------------------------

    @staticmethod
    def _score_recency(permit: dict[str, Any], reasons: list[str]) -> int:
        issued_raw = (
            permit.get("issue_date")
            or permit.get("issued_date")
            or permit.get("applied_date")
            or ""
        )
        if not issued_raw:
            return 0

        try:
            # Handle ISO-8601 with or without timezone info
            issued_str = str(issued_raw).split("T")[0]
            issued = datetime.strptime(issued_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return 0

        age_days = (datetime.now(tz=timezone.utc) - issued).days
        if age_days <= 30:
            reasons.append(f"Very recent permit ({age_days}d ago)")
            return 25
        if age_days <= 90:
            reasons.append(f"Recent permit ({age_days}d ago)")
            return 10
        return 0
