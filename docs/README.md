# CleverTap Campaign Automation Pipeline

Automated pipeline for running personalized push notification campaigns via CleverTap for Supertails+ Clinic promotions.

## What This Does

The pipeline takes customer cohort data (segmented user lists), applies priority-based exclusion logic, personalizes campaign messages, and triggers push notifications through CleverTap's External Trigger API.

**Key safety feature:** All scripts default to **dry-run mode** — no API calls are made unless you explicitly pass `--live`.

---

## Documentation Index

| File | Description |
|---|---|
| [architecture.md](architecture.md) | System architecture and data flow |
| [pipeline-stages.md](pipeline-stages.md) | Detailed guide for each pipeline stage |
| [configuration.md](configuration.md) | Environment variables and config files |
| [data-formats.md](data-formats.md) | Schema reference for all CSV files |
| [usage.md](usage.md) | CLI reference and usage examples |

---

## Quick Start

### Prerequisites

Install Python dependencies:
```bash
pip install pandas mysql-connector-python google-auth-oauthlib google-auth-httplib2 google-api-python-client requests python-dotenv
```

Set up credentials:
- Copy `.env.example` to `.env` and fill in your credentials (see [configuration.md](configuration.md))
- Place `credentials.json` (Google OAuth) in `secret/`

### Git-Ignored Files

The following are automatically ignored by `.gitignore` and must not be committed:
- `secret/` — OAuth credentials and tokens
- `.env` — Environment variables with API keys
- `outputs/` — Campaign output CSVs and logs
- `data/cohorts/` — User list CSVs
- `data/clinic_mastersheet.csv` — Downloaded from Google Sheets
- `__pycache__/` — Python bytecode

### Run a Dry-Run (Safe)

```bash
# Preview morning campaign for today
python run_campaign.py --slot morning

# Preview both slots for a specific date
python run_campaign.py --slot both --date 22032026
```

### Run a Live Campaign (Authorized Personnel Only)

```bash
python run_campaign.py --slot morning --live
```

---

## Pipeline Overview

```
Google Sheets (campaign config)     MySQL Database (cohort data)
         ↓                                    ↓
  campaign_scripts/01_fetch_clinic_mastersheet.py    fetch_cohorts.py
         ↓                                    ↓
              campaign_scripts/02_generate_priority_exclusions.py
                          ↓
              campaign_scripts/03_prepare_campaign_content.py
                          ↓
                  campaign_scripts/04_trigger_campaign.py
                     ↓          ↓
              (Dry-run:       (Live:
             print payloads)  CleverTap API)
```

Each stage is a standalone script. The orchestrator `run_campaign.py` runs them end-to-end.

---

## Project Structure

```
clevertap-automation-pipeline/
├── fetch_cohorts.py              # Stage 0: Fetch cohorts from MySQL
├── campaign_scripts/
│   ├── 01_fetch_clinic_mastersheet.py   # Stage 1: Fetch config from Google Sheets
│   ├── 02_generate_priority_exclusions.py  # Stage 2: Apply exclusion logic
│   ├── 03_prepare_campaign_content.py   # Stage 3: Personalize content & deeplinks
│   └── 04_trigger_campaign.py           # Stage 4: Trigger CleverTap API
├── run_campaign.py               # Orchestrator: runs all stages
├── utils.py                         # Shared utility functions
├── .env                             # Credentials and config (git-ignored)
├── data/
│   ├── clinic_mastersheet.csv       # Campaign schedule (from Google Sheets)
│   ├── cohort_mapping.csv           # Cohort code → campaign_id + deeplink URL mapping
│   ├── exclusion_mapping.csv        # Exclusion name → exclusion dataset mapping
│   ├── cohorts/                     # User lists per cohort segment
│   └── queries/                     # SQL files for cohort extraction
├── outputs/
│   ├── {DDMMYYYY}_{slot}/           # Per-campaign output CSVs
│   └── log/                         # Dispatch logs
├── secret/                          # Google OAuth credentials (git-ignored)
└── MVP/                             # Proof-of-concept scripts
```
