# TILT Construction Lead Engine

**Permit Data → AI Scoring → Deep Enrichment → Contact Discovery → GoHighLevel CRM**

6-step pipeline: Ingest → AI Classify → Qualify → Score → Deep Enrich → GHL Push

## Usage
```bash
python lead_engine/score_and_push.py -j ct_hartford --dry-run
python lead_engine/score_and_push.py -j ct_hartford --since 2024-01-01
python lead_engine/score_and_push.py -j ct_hartford --skip-enrichment
python lead_engine/score_and_push.py -j ct_hartford --enrich-layers entity,skip_trace
```

## Calculus Holdings LLC — Confidential
