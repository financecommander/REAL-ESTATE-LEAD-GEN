# REAL-ESTATE-LEAD-GEN

An end-to-end real estate lead generation pipeline that:
1. **Fetches** open building permits from any Socrata/SODA open-data API
2. **Scores** each permit as a potential lead (0–100)
3. **Pushes** qualified leads as contacts into GoHighLevel (GHL) CRM

---

## Architecture

```
Open Permits API  →  PermitFetcher
                          │
                          ▼
                     LeadScorer  (score 0–100)
                          │
                          ▼  (qualified leads only)
                      GHLClient  →  GoHighLevel CRM
```

### Modules

| Module | Description |
|--------|-------------|
| `src/permits/fetcher.py` | Fetches open permits from a Socrata/SODA endpoint |
| `src/scoring/scorer.py` | Scores permits on value, type, property class, and recency |
| `src/ghl/client.py` | Upserts qualified leads as contacts in GoHighLevel |
| `src/pipeline.py` | Orchestrates the full pipeline; also a CLI entry-point |

---

## Scoring rules

| Criterion | Points |
|-----------|--------|
| Permit value ≥ $50 000 | 30 |
| Permit value ≥ $10 000 | 15 |
| New-construction permit type | 25 |
| Major-renovation permit type | 15 |
| Residential property | 20 |
| Commercial property | 10 |
| Permit issued ≤ 30 days ago | 25 |
| Permit issued ≤ 90 days ago | 10 |

Only leads with a total score ≥ `MIN_LEAD_SCORE` (default 50) are pushed to GHL.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `GHL_API_KEY` | ✅ | GoHighLevel private/location API key |
| `GHL_LOCATION_ID` | ✅ | GHL sub-account (location) ID |
| `PERMITS_API_URL` | optional | Socrata endpoint (default: City of Austin) |
| `PERMITS_APP_TOKEN` | optional | Socrata app token (avoids throttling) |
| `PERMITS_LIMIT` | optional | Max permits to fetch per run (default: 200) |
| `MIN_LEAD_SCORE` | optional | Minimum score to qualify a lead (default: 50) |

---

## Usage

### Run the full pipeline

```bash
python -m src.pipeline
```

### Use as a library

```python
from src.pipeline import Pipeline

pipeline = Pipeline()
result = pipeline.run()

print(f"Fetched:   {result.permits_fetched} permits")
print(f"Qualified: {result.leads_qualified} leads")
print(f"Pushed:    {result.leads_pushed} to GHL")
```

### Use individual components

```python
from src.permits.fetcher import PermitFetcher
from src.scoring.scorer import LeadScorer
from src.ghl.client import GHLClient

permits = PermitFetcher().fetch()
qualified = LeadScorer().qualified(permits)
GHLClient().push_leads(qualified)
```

---

## Tests

```bash
pytest tests/ -v
```
