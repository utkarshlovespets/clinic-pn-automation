# Clinic Push Notification Automation

Automated pipeline for preparing and triggering personalized Supertails Clinic push notifications through CleverTap External Trigger campaigns.

The pipeline is dry-run by default. No CleverTap API calls are made unless `--live` is passed.

## Documentation

| File | Purpose |
|---|---|
| [architecture.md](architecture.md) | System flow and stage responsibilities |
| [configuration.md](configuration.md) | Environment variables and Google Sheet setup |
| [data-formats.md](data-formats.md) | Input, intermediate, output, and log schemas |
| [pipeline-stages.md](pipeline-stages.md) | Stage-by-stage behavior and commands |
| [usage.md](usage.md) | Common CLI workflows and troubleshooting |

## Quick Start

Install dependencies:

```bash
pip install pandas mysql-connector-python google-auth-oauthlib google-auth-httplib2 google-api-python-client requests python-dotenv
```

Set up local secrets:

- Create `.env` in the project root.
- Put Google OAuth credentials at `secrets/credentials.json`.
- Run Stage 1 once to create the cached Google token.

Dry-run today's inferred slot:

```bash
python run_campaign.py
```

Dry-run a specific date and slot:

```bash
python run_campaign.py --date 04052026 --slot morning
```

Live run, authorized use only:

```bash
python run_campaign.py --date 04052026 --slot morning --live
```

## Current Data Sources

Stage 1 reads four tabs from the Google Sheet identified by `SPREADSHEET_ID`:

| Sheet tab | Local CSV |
|---|---|
| `Clinic_PN_Automation` | `data/clinic_mastersheet.csv` |
| `Cohort_Mapping` | `data/cohort_mapping.csv` |
| `Exclusion_Mapping` | `data/exclusion_mapping.csv` |
| `Image_Mapping` | `data/image_mapping.csv` |

Campaign rows are mapped by `Campaign ID`, not by the mastersheet `Cohort Name`. `cohort_mapping.csv` must contain `cohort_code`.
Rows with `Image` use `cohort_mapping.csv.img_campaign_id` and resolve `image_url` from `Image_Mapping`.

## Project Layout

```text
clinic-pn-automation/
|-- run_campaign.py
|-- generate_summaries_archive.py
|-- utils.py
|-- campaign_scripts/
|   |-- 00_fetch_cohorts.py
|   |-- 01_fetch_clinic_mastersheet.py
|   |-- 02_generate_priority_exclusions.py
|   |-- 03_prepare_campaign_content.py
|   `-- 04_trigger_campaign.py
|-- data/
|   |-- clinic_mastersheet.csv
|   |-- cohort_mapping.csv
|   |-- exclusion_mapping.csv
|   `-- cohorts/
|-- outputs/
|   |-- {DDMMYYYY}_{slot}/
|   `-- log/
|-- docs/
`-- secrets/
```

## Git-Ignored Runtime Files

Do not commit credentials, downloaded user data, generated outputs, or local environment files:

- `.env`
- `secrets/`
- `data/cohorts/`
- `outputs/`
- Python cache folders
