"""
Layer 5: Contractor Intelligence
Reverse lookup: find all other permits by the same contractor.
Identifies referral potential and pipeline intelligence.
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from loguru import logger

from openshovels.schema import ContractorProfile, StandardPermit


class ContractorIntel:
    """
    Contractor reverse lookup engine.

    Given a contractor name from a permit, finds:
    - All other active permits by that contractor
    - Total pipeline value
    - Other entity clients (potential borrowers)
    - Referral potential scoring
    """

    def __init__(self):
        # In-memory permit index by contractor name
        # In production this would be a database query
        self._contractor_index: dict[str, list[dict]] = {}

    def index_permits(self, permits: list[StandardPermit]):
        """Build contractor index from a batch of permits."""
        for permit in permits:
            if not permit.contractor_name:
                continue

            key = permit.contractor_name.lower().strip()
            if key not in self._contractor_index:
                self._contractor_index[key] = []

            self._contractor_index[key].append({
                "permit_id": permit.permit_id,
                "jurisdiction": permit.jurisdiction,
                "address": permit.address,
                "city": permit.city,
                "state": permit.state,
                "owner_entity": permit.owner_entity or permit.owner_name or "",
                "job_value": float(permit.job_value) if permit.job_value else 0,
                "ai_value": float(permit.ai_value_estimated) if permit.ai_value_estimated else 0,
                "project_type": permit.ai_project_classification.value if permit.ai_project_classification else "unknown",
                "permit_status": permit.permit_status.value,
                "filed_date": str(permit.filed_date) if permit.filed_date else "",
                "issued_date": str(permit.issued_date) if permit.issued_date else "",
            })

        logger.info(
            f"Indexed {sum(len(v) for v in self._contractor_index.values())} permits "
            f"across {len(self._contractor_index)} contractors"
        )

    def lookup(self, contractor_name: str) -> Optional[ContractorProfile]:
        """
        Look up a contractor's full profile from the permit index.
        """
        if not contractor_name:
            return None

        key = contractor_name.lower().strip()
        permits = self._contractor_index.get(key, [])

        if not permits:
            # Try fuzzy match
            for idx_key in self._contractor_index:
                if key in idx_key or idx_key in key:
                    permits = self._contractor_index[idx_key]
                    break

        if not permits:
            return None

        # Calculate stats
        total_value = sum(
            max(p.get("ai_value", 0), p.get("job_value", 0))
            for p in permits
        )

        # Unique jurisdictions
        jurisdictions = list(set(p["jurisdiction"] for p in permits))

        # Other clients (unique entity names)
        clients = list(set(
            p["owner_entity"] for p in permits
            if p["owner_entity"]
        ))

        # Recent projects (most recent first)
        recent = sorted(
            permits,
            key=lambda p: p.get("issued_date", p.get("filed_date", "")),
            reverse=True,
        )[:10]

        # Referral potential
        referral = self._score_referral_potential(permits, clients)

        return ContractorProfile(
            contractor_name=contractor_name,
            active_permits_count=len(permits),
            total_permit_value=Decimal(str(total_value)) if total_value else None,
            jurisdictions_active=jurisdictions,
            recent_projects=[{
                "address": p["address"],
                "city": p["city"],
                "value": max(p.get("ai_value", 0), p.get("job_value", 0)),
                "type": p["project_type"],
                "date": p.get("issued_date") or p.get("filed_date", ""),
                "owner": p["owner_entity"],
            } for p in recent],
            other_clients=clients[:20],
            referral_potential=referral,
            lookup_timestamp=datetime.now(),
        )

    def _score_referral_potential(
        self, permits: list[dict], clients: list[str]
    ) -> str:
        """
        Score contractor's referral potential based on activity.

        HIGH: 5+ active permits, $5M+ pipeline, 3+ unique clients
        MEDIUM: 2-4 permits, $1-5M pipeline, 2+ clients
        LOW: 1 permit or <$1M pipeline
        """
        total_value = sum(
            max(p.get("ai_value", 0), p.get("job_value", 0))
            for p in permits
        )

        if len(permits) >= 5 and total_value >= 5_000_000 and len(clients) >= 3:
            return "high"
        elif len(permits) >= 2 and total_value >= 1_000_000 and len(clients) >= 2:
            return "medium"
        else:
            return "low"

    def get_top_contractors(self, min_permits: int = 3) -> list[ContractorProfile]:
        """Get top contractors by permit volume for referral targeting."""
        profiles = []
        for name_key, permits in self._contractor_index.items():
            if len(permits) >= min_permits:
                profile = self.lookup(permits[0].get("contractor_name", name_key))
                if profile:
                    profiles.append(profile)

        # Sort by permit count descending
        profiles.sort(key=lambda p: p.active_permits_count, reverse=True)
        return profiles[:50]
