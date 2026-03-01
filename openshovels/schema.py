"""
OpenShovels — Standardized Permit Schema v2
Extended with enrichment data models for entity resolution, skip trace,
property intelligence, news intel, and contractor reverse lookup.
"""
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


# ── Permit Enums ──────────────────────────────────────────────────────

class PermitType(str, Enum):
    NEW_CONSTRUCTION = "new_construction"
    DEMOLITION = "demolition"
    RENOVATION = "renovation"
    ADDITION = "addition"
    MECHANICAL = "mechanical"
    ELECTRICAL = "electrical"
    PLUMBING = "plumbing"
    OTHER = "other"


class PermitStatus(str, Enum):
    FILED = "filed"
    APPROVED = "approved"
    ACTIVE = "active"
    FINAL = "final"
    EXPIRED = "expired"
    REVOKED = "revoked"


class PropertyType(str, Enum):
    SINGLE_FAMILY = "single_family"
    DUPLEX = "duplex"
    TRIPLEX = "triplex"
    FOURPLEX = "fourplex"
    MULTI_5PLUS = "multi_5plus"
    MIXED_USE = "mixed_use"
    COMMERCIAL = "commercial"
    UNKNOWN = "unknown"


class ProjectClassification(str, Enum):
    GROUND_UP = "ground_up"
    HEAVY_REHAB = "heavy_rehab"
    MODERATE_REHAB = "moderate_rehab"
    GUT_RENOVATION = "gut_renovation"
    COSMETIC = "cosmetic"
    SYSTEMS_ONLY = "systems_only"
    NOT_CONSTRUCTION = "not_construction"


class DataSource(str, Enum):
    JURISDICTION_RAW = "jurisdiction_raw"
    OPEN_DATA_PORTAL = "open_data_portal"
    AI_ENRICHED = "ai_enriched"
    MANUAL_ENTRY = "manual_entry"


# ── Enrichment Data Models ────────────────────────────────────────────

class EntityRecord(BaseModel):
    """Result of Secretary of State / OpenCorporates lookup."""
    entity_name: str
    entity_type: Optional[str] = None
    state_of_formation: Optional[str] = None
    formation_date: Optional[date] = None
    status: Optional[str] = None
    registered_agent_name: Optional[str] = None
    registered_agent_address: Optional[str] = None
    principal_office_address: Optional[str] = None
    principals: list[str] = Field(default_factory=list)
    related_entities: list[str] = Field(default_factory=list)
    sos_filing_number: Optional[str] = None
    sos_url: Optional[str] = None
    other_properties_count: Optional[int] = None
    lookup_source: Optional[str] = None
    lookup_timestamp: Optional[datetime] = None


class SkipTraceResult(BaseModel):
    """Contact info from skip trace services."""
    person_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phones: list[dict] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)
    mailing_address: Optional[str] = None
    mailing_city: Optional[str] = None
    mailing_state: Optional[str] = None
    mailing_zip: Optional[str] = None
    age: Optional[str] = None
    relatives: list[dict] = Field(default_factory=list)
    associates: list[dict] = Field(default_factory=list)
    previous_addresses: list[dict] = Field(default_factory=list)
    linkedin_url: Optional[str] = None
    confidence_score: Optional[float] = None
    dnc_checked: bool = False
    skip_trace_source: Optional[str] = None
    skip_trace_timestamp: Optional[datetime] = None


class PropertyIntelligence(BaseModel):
    """Property data from ATTOM/assessor/Zillow."""
    property_address: str
    owner_name: Optional[str] = None
    owner_mailing_address: Optional[str] = None
    assessed_value: Optional[Decimal] = None
    market_value: Optional[Decimal] = None
    last_sale_date: Optional[date] = None
    last_sale_price: Optional[Decimal] = None
    mortgage_amount: Optional[Decimal] = None
    mortgage_lender: Optional[str] = None
    equity_estimate: Optional[Decimal] = None
    year_built: Optional[int] = None
    lot_size_sqft: Optional[int] = None
    building_sqft: Optional[int] = None
    zoning: Optional[str] = None
    tax_amount: Optional[Decimal] = None
    rental_estimate: Optional[Decimal] = None
    arv_estimate: Optional[Decimal] = None
    neighborhood_median_price: Optional[Decimal] = None
    days_on_market_avg: Optional[int] = None
    data_source: Optional[str] = None
    lookup_timestamp: Optional[datetime] = None


class NewsHit(BaseModel):
    """A single news article or public record hit."""
    title: str
    source: str
    url: Optional[str] = None
    published_date: Optional[date] = None
    snippet: Optional[str] = None
    sentiment: Optional[str] = None
    category: Optional[str] = None


class NewsIntelligence(BaseModel):
    """Aggregated news and public record search results."""
    search_query: str
    total_hits: int = 0
    articles: list[NewsHit] = Field(default_factory=list)
    has_zoning_issues: bool = False
    has_community_opposition: bool = False
    has_tax_incentives: bool = False
    has_litigation: bool = False
    court_cases: list[dict] = Field(default_factory=list)
    ai_summary: Optional[str] = None
    search_timestamp: Optional[datetime] = None


class ContractorProfile(BaseModel):
    """Contractor intelligence from reverse permit lookup."""
    contractor_name: str
    contractor_license: Optional[str] = None
    active_permits_count: int = 0
    total_permit_value: Optional[Decimal] = None
    jurisdictions_active: list[str] = Field(default_factory=list)
    recent_projects: list[dict] = Field(default_factory=list)
    other_clients: list[str] = Field(default_factory=list)
    referral_potential: Optional[str] = None
    lookup_timestamp: Optional[datetime] = None


class EnrichmentBundle(BaseModel):
    """All enrichment data for a single permit/lead."""
    permit_id: str
    entity: Optional[EntityRecord] = None
    skip_trace: Optional[SkipTraceResult] = None
    property_intel: Optional[PropertyIntelligence] = None
    news_intel: Optional[NewsIntelligence] = None
    contractor: Optional[ContractorProfile] = None
    enrichment_cost_usd: float = 0.0
    enrichment_timestamp: Optional[datetime] = None
    enrichment_layers_completed: list[str] = Field(default_factory=list)

    @property
    def has_contact_info(self) -> bool:
        if not self.skip_trace:
            return False
        return bool(self.skip_trace.phones or self.skip_trace.emails)

    @property
    def noo_confirmed(self) -> bool:
        if self.property_intel and self.property_intel.owner_mailing_address:
            return True
        if self.entity and self.entity.entity_type in ("LLC", "Corp", "LP", "Trust"):
            return True
        return False


# ── Standard Permit v2 ────────────────────────────────────────────────

class StandardPermit(BaseModel):
    """Canonical permit record — all jurisdictions normalize to this."""
    permit_id: str = Field(description="Jurisdiction-issued permit number")
    jurisdiction: str = Field(description="Jurisdiction code")
    source: DataSource = DataSource.JURISDICTION_RAW

    filed_date: Optional[date] = None
    issued_date: Optional[date] = None
    expiration_date: Optional[date] = None
    last_inspection_date: Optional[date] = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now())

    address: str
    city: str
    state: str
    zip_code: Optional[str] = None
    county: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    parcel_id: Optional[str] = None

    permit_type: PermitType = PermitType.OTHER
    permit_status: PermitStatus = PermitStatus.FILED
    description: Optional[str] = None
    job_value: Optional[Decimal] = None

    property_type: PropertyType = PropertyType.UNKNOWN
    unit_count: Optional[int] = None
    square_footage: Optional[int] = None
    stories: Optional[int] = None

    owner_name: Optional[str] = None
    owner_entity: Optional[str] = None
    contractor_name: Optional[str] = None
    contractor_license: Optional[str] = None
    architect_name: Optional[str] = None

    ai_project_classification: Optional[ProjectClassification] = None
    ai_unit_count_estimated: Optional[int] = None
    ai_value_estimated: Optional[Decimal] = None
    ai_is_investor_noo: Optional[bool] = None
    ai_confidence: Optional[float] = None
    ai_tags: list[str] = Field(default_factory=list)

    tilt_qualified: Optional[bool] = None
    tilt_disqualify_reason: Optional[str] = None

    enrichment: Optional[EnrichmentBundle] = None

    class Config:
        json_encoders = {
            Decimal: lambda v: float(v),
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat(),
        }


class PermitBatch(BaseModel):
    jurisdiction: str
    ingested_at: datetime = Field(default_factory=lambda: datetime.now())
    record_count: int
    new_records: int
    updated_records: int
    permits: list[StandardPermit]
