"""
Jurisdiction Registry — All Target Markets
New England (CT, MA, RI, NH, ME, VT) + Florida + Texas

Each entry maps to a data source (ArcGIS, Socrata, CKAN, CSV) with
API endpoints, field mappings, and scraper configuration.
"""

JURISDICTIONS = {
    # ═══════════════════════════════════════════════════════════════════
    # CONNECTICUT
    # ═══════════════════════════════════════════════════════════════════
    "ct_hartford": {
        "name": "Hartford, CT",
        "state": "CT",
        "county": "Hartford County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://services1.arcgis.com/XBhYkoXKJCRHbe7M/arcgis/rest/services/Building_Permits_20200101_to_Current/FeatureServer/0/query",
            "geojson": "https://open-data-hartford-hartfordgis.hub.arcgis.com/api/v3/datasets/d595ae995fb049d3ac54919ebf24b1ac/downloads/data?format=geojson",
            "csv": "https://data.hartford.gov/api/download/v1/items/d595ae995fb049d3ac54919ebf24b1ac/csv",
        },
        "status": "active",
        "record_count_approx": 34000,
    },
    "ct_new_haven": {
        "name": "New Haven, CT",
        "state": "CT",
        "county": "New Haven County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://data.newhavenct.gov/api/v2/catalog",
            "search": "https://data.newhavenct.gov/resource/permits.json",
        },
        "status": "pending_endpoint_discovery",
    },
    "ct_bridgeport": {
        "name": "Bridgeport, CT",
        "state": "CT",
        "county": "Fairfield County",
        "platform": "viewpoint",
        "endpoints": {
            "primary": "https://bridgeportct.viewpointcloud.com/",
        },
        "status": "pending_scraper",
        "notes": "ViewPoint Cloud platform — needs API discovery or scraping",
    },
    "ct_stamford": {
        "name": "Stamford, CT",
        "state": "CT",
        "county": "Fairfield County",
        "platform": "html_scrape",
        "endpoints": {
            "primary": "https://www.stamfordct.gov/building-department",
        },
        "status": "pending_scraper",
    },

    # ═══════════════════════════════════════════════════════════════════
    # MASSACHUSETTS
    # ═══════════════════════════════════════════════════════════════════
    "ma_boston": {
        "name": "Boston, MA",
        "state": "MA",
        "county": "Suffolk County",
        "platform": "ckan",
        "endpoints": {
            "primary": "https://data.boston.gov/api/3/action/datastore_search",
            "csv_download": "https://data.boston.gov/dataset/cd1ec3ff-6ebf-4a65-af68-8329eceab740/resource/6ddcd912-32a0-43df-9908-63574f8c7e77/download/tmppcwb6msr.csv",
            "resource_id": "6ddcd912-32a0-43df-9908-63574f8c7e77",
        },
        "field_mapping": {
            "permit_id": "PermitNumber",
            "description": "DESCRIPTION",
            "address": "ADDRESS",
            "city": "CITY",
            "state": "STATE",
            "zip_code": "ZIP",
            "permit_type": "WORKTYPE",
            "permit_status": "STATUS",
            "issued_date": "ISSUED_DATE",
            "expiration_date": "EXPIRATION_DATE",
            "job_value": "DECLARED_VALUATION",
            "owner_name": "APPLICANT",
            "contractor_name": "APPLICANT",  # Boston uses same field
            "occupancy": "OCCUPANCYTYPE",
        },
        "status": "active",
        "record_count_approx": 609000,
    },
    "ma_worcester": {
        "name": "Worcester, MA",
        "state": "MA",
        "county": "Worcester County",
        "platform": "html_scrape",
        "endpoints": {
            "primary": "https://www.worcesterma.gov/inspectional-services",
        },
        "status": "pending_scraper",
    },
    "ma_springfield": {
        "name": "Springfield, MA",
        "state": "MA",
        "county": "Hampden County",
        "platform": "html_scrape",
        "endpoints": {
            "primary": "https://www.springfield-ma.gov/planning/",
        },
        "status": "pending_scraper",
    },

    # ═══════════════════════════════════════════════════════════════════
    # RHODE ISLAND
    # ═══════════════════════════════════════════════════════════════════
    "ri_providence": {
        "name": "Providence, RI",
        "state": "RI",
        "county": "Providence County",
        "platform": "socrata",
        "endpoints": {
            "primary": "https://data.providenceri.gov/resource/permits.json",
            "portal": "https://data.providenceri.gov/",
            "viewpoint": "https://providenceri.viewpointcloud.com/",
        },
        "status": "pending_endpoint_discovery",
        "notes": "Socrata portal exists but need to confirm building permit dataset ID",
    },

    # ═══════════════════════════════════════════════════════════════════
    # NEW HAMPSHIRE
    # ═══════════════════════════════════════════════════════════════════
    "nh_manchester": {
        "name": "Manchester, NH",
        "state": "NH",
        "county": "Hillsborough County",
        "platform": "html_scrape",
        "endpoints": {
            "primary": "https://www.manchesternh.gov/Departments/Building",
        },
        "status": "pending_scraper",
    },

    # ═══════════════════════════════════════════════════════════════════
    # MAINE
    # ═══════════════════════════════════════════════════════════════════
    "me_portland": {
        "name": "Portland, ME",
        "state": "ME",
        "county": "Cumberland County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://portlandme-portal.opendata.arcgis.com/",
            "search": "https://services1.arcgis.com/",
        },
        "status": "pending_endpoint_discovery",
    },

    # ═══════════════════════════════════════════════════════════════════
    # VERMONT
    # ═══════════════════════════════════════════════════════════════════
    "vt_burlington": {
        "name": "Burlington, VT",
        "state": "VT",
        "county": "Chittenden County",
        "platform": "html_scrape",
        "endpoints": {
            "primary": "https://www.burlingtonvt.gov/DPI/Building-Permits",
        },
        "status": "pending_scraper",
    },

    # ═══════════════════════════════════════════════════════════════════
    # FLORIDA
    # ═══════════════════════════════════════════════════════════════════
    "fl_miami_dade": {
        "name": "Miami-Dade County, FL",
        "state": "FL",
        "county": "Miami-Dade County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://gis-mdc.opendata.arcgis.com/datasets/MDC::building-permit/api",
            "feature_server": "https://gisfs.miamidade.gov/arcgis/rest/services/MD_BuildingPermit/FeatureServer/0/query",
            "geojson": "https://opendata.miamidade.gov/api/v3/datasets/building-permit/downloads/data?format=geojson",
        },
        "status": "active",
        "record_count_approx": 500000,
        "notes": "ArcGIS Hub — 3 years of data, updated regularly",
    },
    "fl_broward": {
        "name": "Broward County, FL",
        "state": "FL",
        "county": "Broward County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://gis.broward.org/arcgis/rest/services/",
            "portal": "https://opendata.broward.org/",
        },
        "status": "pending_endpoint_discovery",
    },
    "fl_orlando": {
        "name": "Orlando, FL",
        "state": "FL",
        "county": "Orange County",
        "platform": "socrata",
        "endpoints": {
            "primary": "https://data.cityoforlando.net/resource/permits.json",
            "portal": "https://data.cityoforlando.net/",
        },
        "status": "pending_endpoint_discovery",
    },
    "fl_tampa": {
        "name": "Tampa, FL",
        "state": "FL",
        "county": "Hillsborough County",
        "platform": "socrata",
        "endpoints": {
            "primary": "https://data.tampagov.net/resource/permits.json",
            "portal": "https://data.tampagov.net/",
        },
        "status": "pending_endpoint_discovery",
    },
    "fl_jacksonville": {
        "name": "Jacksonville, FL",
        "state": "FL",
        "county": "Duval County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://maps.coj.net/arcgis/rest/services/",
            "portal": "https://data.coj.net/",
        },
        "status": "pending_endpoint_discovery",
    },

    # ═══════════════════════════════════════════════════════════════════
    # TEXAS
    # ═══════════════════════════════════════════════════════════════════
    "tx_houston": {
        "name": "Houston, TX",
        "state": "TX",
        "county": "Harris County",
        "platform": "socrata",
        "endpoints": {
            "primary": "https://data.houstontx.gov/resource/permits.json",
            "portal": "https://data.houstontx.gov/",
            "sold_permits": "https://www.houstonpermittingcenter.org/",
        },
        "status": "pending_endpoint_discovery",
        "notes": "Monthly XLSX reports also available at houstontx.gov",
    },
    "tx_dallas": {
        "name": "Dallas, TX",
        "state": "TX",
        "county": "Dallas County",
        "platform": "socrata",
        "endpoints": {
            "primary": "https://www.dallasopendata.com/resource/permits.json",
            "portal": "https://www.dallasopendata.com/",
        },
        "status": "pending_endpoint_discovery",
    },
    "tx_austin": {
        "name": "Austin, TX",
        "state": "TX",
        "county": "Travis County",
        "platform": "socrata",
        "endpoints": {
            "primary": "https://data.austintexas.gov/resource/3syk-w9eu.json",
            "portal": "https://data.austintexas.gov/",
        },
        "field_mapping": {
            "permit_id": "permit_number",
            "description": "description",
            "address": "original_address1",
            "permit_type": "permit_type_desc",
            "permit_status": "status_current",
            "issued_date": "issued_date",
            "job_value": "total_job_valuation",
            "contractor_name": "contractor_company_desc",
            "applicant": "applicant_full_name",
        },
        "status": "active",
        "record_count_approx": 400000,
        "notes": "Well-structured Socrata dataset with good field coverage",
    },
    "tx_san_antonio": {
        "name": "San Antonio, TX",
        "state": "TX",
        "county": "Bexar County",
        "platform": "arcgis",
        "endpoints": {
            "primary": "https://data.sanantonio.gov/",
        },
        "status": "pending_endpoint_discovery",
    },
}


def get_active_jurisdictions() -> list[str]:
    """Return jurisdiction codes with active scrapers."""
    return [
        code for code, config in JURISDICTIONS.items()
        if config.get("status") == "active"
    ]


def get_jurisdiction_config(code: str) -> dict:
    """Get configuration for a specific jurisdiction."""
    if code not in JURISDICTIONS:
        raise ValueError(f"Unknown jurisdiction: {code}")
    return JURISDICTIONS[code]


def get_jurisdictions_by_state(state: str) -> list[str]:
    """Get all jurisdiction codes for a given state."""
    return [
        code for code, config in JURISDICTIONS.items()
        if config.get("state") == state.upper()
    ]
