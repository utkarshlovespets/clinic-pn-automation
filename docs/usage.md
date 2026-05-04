# Usage Guide

## Prerequisites

Install dependencies:

```bash
pip install pandas mysql-connector-python google-auth-oauthlib google-auth-httplib2 google-api-python-client requests python-dotenv
```

Create `.env`, add Google and CleverTap credentials, and place OAuth credentials at `secrets/credentials.json`.

## Full Pipeline

Dry-run current IST auto-slot:

```bash
python run_campaign.py
```

Dry-run a specific date and slot:

```bash
python run_campaign.py --date 04052026 --slot morning
```

Dry-run both slots:

```bash
python run_campaign.py --date 04052026 --slot both
```

Live run, authorized use only:

```bash
python run_campaign.py --date 04052026 --slot morning --live
```

Filter Stage 4 to specific generated cohort names:

```bash
python run_campaign.py --date 04052026 --slot morning --cohorts Clinic_WTFLD Clinic_Grooming_Repl_xxREP
```

Increase orchestrated Stage 4 parallelism:

```bash
python run_campaign.py --date 04052026 --slot morning --live --max-workers 50
```

## What `run_campaign.py` Does

1. Fetches `Clinic_PN_Automation`, `Cohort_Mapping`, and `Exclusion_Mapping` from Google Sheets.
2. In live mode only, refreshes cohort CSVs through Stage 1b.
3. Generates priority-filtered audience CSVs.
4. Adds personalized title/body and deeplinks.
5. Dry-runs or live-triggers CleverTap campaigns.
6. Sends a Slack status notification when Slack env vars are configured.

## Individual Stages

Fetch Google Sheet config:

```bash
python campaign_scripts/01_fetch_clinic_mastersheet.py
```

Generate priority files:

```bash
python campaign_scripts/02_generate_priority_exclusions.py --date 04052026 --slot morning
```

Prepare content:

```bash
python campaign_scripts/03_prepare_campaign_content.py --output-dir outputs/04052026_morning
```

Trigger dry-run:

```bash
python campaign_scripts/04_trigger_campaign.py --output-dir outputs/04052026_morning
```

Trigger live:

```bash
python campaign_scripts/04_trigger_campaign.py --output-dir outputs/04052026_morning --live
```

Generate summaries archive:

```bash
python generate_summaries_archive.py
```

## Files To Inspect Before Live

Stage 2:

```text
outputs/04052026_morning/campaign_meta.csv
outputs/log/summary/04052026_morning.csv
```

Stage 3:

```text
outputs/04052026_morning/01_Clinic_WTFLD.csv
outputs/04052026_morning/02_Clinic_Grooming_Repl_xxREP.csv
```

Dry-run log:

```text
outputs/log/dry_run/04052026_morning_campaign_log.csv
```

Live log:

```text
outputs/log/live/04052026_morning_campaign_log.csv
```

## Common Checks

### Missing Cohort CSV

Check:

- `data/cohort_mapping.csv.cohort_dataset`
- `data/exclusion_mapping.csv.Dataset`
- Files under `data/cohorts/`

The filenames must match exactly.

### Campaign ID Not Found

Check that `Clinic_PN_Automation.Campaign ID` matches `Cohort_Mapping.campaign_id`.

### Mapping Column Error

`data/cohort_mapping.csv` must contain `cohort_code`.

### Missing Title Or Content

`run_campaign.py` aborts when selected rows for the run date/slot have blank `Title` or `Content`. Fill those cells in `Clinic_PN_Automation` and rerun Stage 1.

### Google Auth Fails

Delete `secrets/token.json` and rerun Stage 1. A browser auth flow will recreate it.

### CleverTap 401

Check `CLEVERTAP_ACCOUNT_ID`, `CLEVERTAP_PASSCODE`, and `CLEVERTAP_REGION` in `.env`.

## Live-Run Safety

Dry-run is the default. `--live` is required for API calls. Live mode includes countdown prompts before triggering. Press `Ctrl+C` during a countdown to abort before sends start.
