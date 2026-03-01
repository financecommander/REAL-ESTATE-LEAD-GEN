"""
OpenShovels — AI Enrichment Pipeline
Uses Grok ($0.20/M tokens) for permit classification/tagging.
Uses Claude for compliance-critical lead scoring (via lead_engine).
"""
import json
from decimal import Decimal
from typing import Optional

from loguru import logger
from openai import AsyncOpenAI

from openshovels.schema import (
    StandardPermit, ProjectClassification, PropertyType
)


# Grok via OpenAI-compatible API
GROK_BASE_URL = "https://api.x.ai/v1"

CLASSIFICATION_PROMPT = """You are a building permit classifier for a construction lending company.
Analyze the permit and return ONLY a JSON object (no markdown, no explanation).

Permit data:
- Description: {description}
- Type: {permit_type}
- Address: {address}, {city}, {state}
- Stated value: {job_value}
- Owner: {owner_name}
- Unit count (if stated): {unit_count}

Return JSON:
{{
  "project_classification": "ground_up|heavy_rehab|moderate_rehab|gut_renovation|cosmetic|systems_only|not_construction",
  "estimated_unit_count": <int or null>,
  "estimated_value_usd": <int or null>,
  "is_investor_noo": <true|false|null>,
  "confidence": <0.0-1.0>,
  "tags": ["tag1", "tag2"],
  "reasoning": "<one sentence>"
}}

Rules:
- "ground_up" = new building from scratch or after demolition
- "heavy_rehab" = structural changes, >50% of building affected, >$500K
- "is_investor_noo" = true if owner name looks like LLC/Corp/Trust, or if address differs from owner
- Include tags like: "multifamily", "luxury", "affordable", "mixed_use", "commercial_component"
"""


class PermitEnricher:
    """AI-powered permit classification and enrichment."""

    def __init__(self, api_key: str, model: str = "grok-3-mini"):
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=GROK_BASE_URL,
        )
        self.model = model

    async def enrich_permit(self, permit: StandardPermit) -> StandardPermit:
        """Classify and enrich a single permit using AI."""
        try:
            prompt = CLASSIFICATION_PROMPT.format(
                description=permit.description or "N/A",
                permit_type=permit.permit_type.value,
                address=permit.address,
                city=permit.city,
                state=permit.state,
                job_value=str(permit.job_value) if permit.job_value else "N/A",
                owner_name=permit.owner_name or "N/A",
                unit_count=permit.unit_count or "N/A",
            )

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.1,
            )

            text = response.choices[0].message.content.strip()
            # Strip markdown fences if present
            text = text.replace("```json", "").replace("```", "").strip()
            result = json.loads(text)

            # Apply AI classifications
            permit.ai_project_classification = self._map_classification(
                result.get("project_classification", "not_construction")
            )
            permit.ai_unit_count_estimated = result.get("estimated_unit_count")
            permit.ai_value_estimated = (
                Decimal(str(result["estimated_value_usd"]))
                if result.get("estimated_value_usd")
                else None
            )
            permit.ai_is_investor_noo = result.get("is_investor_noo")
            permit.ai_confidence = result.get("confidence", 0.0)
            permit.ai_tags = result.get("tags", [])
            permit.source = "ai_enriched"

            # Update unit count if AI found it and we didn't have it
            if not permit.unit_count and permit.ai_unit_count_estimated:
                permit.unit_count = permit.ai_unit_count_estimated

            # Update property type based on unit count
            if permit.ai_unit_count_estimated:
                permit.property_type = self._unit_count_to_type(
                    permit.ai_unit_count_estimated
                )

        except json.JSONDecodeError as e:
            logger.warning(f"AI JSON parse error for {permit.permit_id}: {e}")
        except Exception as e:
            logger.warning(f"AI enrichment failed for {permit.permit_id}: {e}")

        return permit

    async def enrich_batch(
        self,
        permits: list[StandardPermit],
        concurrency: int = 5,
    ) -> list[StandardPermit]:
        """Enrich a batch of permits with rate limiting."""
        import asyncio

        semaphore = asyncio.Semaphore(concurrency)
        enriched = []

        async def _enrich_with_limit(p):
            async with semaphore:
                return await self.enrich_permit(p)

        tasks = [_enrich_with_limit(p) for p in permits]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, StandardPermit):
                enriched.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Batch enrichment error: {result}")

        logger.info(f"Enriched {len(enriched)}/{len(permits)} permits")
        return enriched

    @staticmethod
    def _map_classification(value: str) -> ProjectClassification:
        mapping = {
            "ground_up": ProjectClassification.GROUND_UP,
            "heavy_rehab": ProjectClassification.HEAVY_REHAB,
            "moderate_rehab": ProjectClassification.MODERATE_REHAB,
            "gut_renovation": ProjectClassification.GUT_RENOVATION,
            "cosmetic": ProjectClassification.COSMETIC,
            "systems_only": ProjectClassification.SYSTEMS_ONLY,
        }
        return mapping.get(value, ProjectClassification.NOT_CONSTRUCTION)

    @staticmethod
    def _unit_count_to_type(units: int) -> PropertyType:
        if units == 1:
            return PropertyType.SINGLE_FAMILY
        elif units == 2:
            return PropertyType.DUPLEX
        elif units == 3:
            return PropertyType.TRIPLEX
        elif units == 4:
            return PropertyType.FOURPLEX
        elif units >= 5:
            return PropertyType.MULTI_5PLUS
        return PropertyType.UNKNOWN
