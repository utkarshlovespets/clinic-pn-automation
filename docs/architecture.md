# Architecture

The project is a file-based campaign pipeline. Each stage reads CSV inputs, writes CSV outputs, and can be inspected or rerun independently.

## Data Flow

```text
Google Sheet: Clinic_PN_Automation
Google Sheet: Cohort_Mapping
Google Sheet: Exclusion_Mapping
Google Sheet: Image_Mapping
          |
          v
Stage 1: campaign_scripts/01_fetch_clinic_mastersheet.py
          |
          |-- data/clinic_mastersheet.csv
          |-- data/cohort_mapping.csv
          |-- data/exclusion_mapping.csv
          `-- data/image_mapping.csv

data/cohorts/*.csv
          |
          v
Stage 2: campaign_scripts/02_generate_priority_exclusions.py
          |
          |-- outputs/{DDMMYYYY}_{slot}/NN_{cohort_code}.csv
          |-- outputs/{DDMMYYYY}_{slot}/summary.csv
          `-- outputs/log/summary/{DDMMYYYY}_{slot}.csv

Stage 3: campaign_scripts/03_prepare_campaign_content.py
          |
          v
outputs/{DDMMYYYY}_{slot}/NN_{cohort_code}.csv
with title, body, campaign_id, deeplink, and image columns

Stage 4: campaign_scripts/04_trigger_campaign.py
          |
          |-- dry-run: print payload previews, no API calls
          |-- live: POST to CleverTap
          `-- outputs/log/{dry_run|live}/{DDMMYYYY}_{slot}_campaign_log.csv
```

## Orchestration

`run_campaign.py` runs Stages 1 through 4 in order. It handles:

- Date and slot selection.
- Auto-slot mode when run with no flags or only `--live`.
- Fetching all four Google Sheet tabs.
- Passing `cohort_mapping.csv` and `exclusion_mapping.csv` into Stage 2.
- Passing `cohort_mapping.csv` and `image_mapping.csv` into Stage 3 for campaign IDs, deeplinks, and image URLs.
- Dry-run by default, live mode only with `--live`.
- Slack completion notification when configured.

## Stage Responsibilities

| Stage | Script | Responsibility |
|---|---|---|
| Stage 1 | `01_fetch_clinic_mastersheet.py` | Fetch mastersheet, cohort mapping, exclusion mapping, and image mapping from Google Sheets |
| Stage 1b | `00_fetch_cohorts.py` | Live-only cohort refresh from configured data source |
| Stage 2 | `02_generate_priority_exclusions.py` | Build prioritized audience CSVs and apply priority/exclusion filtering |
| Stage 3 | `03_prepare_campaign_content.py` | Resolve title/body personalization and build deeplinks |
| Stage 4 | `04_trigger_campaign.py` | Dry-run or live trigger CleverTap campaigns |

## Mapping Model

Campaign audiences are matched by `Campaign ID` in `data/clinic_mastersheet.csv` to `campaign_id` in `data/cohort_mapping.csv`.

`cohort_code` is the automation-facing cohort identifier used for output filenames and normalized lookups. It is required in the mapping file.

Explicit exclusions are separate. Stage 2 combines default values from `data/cohort_mapping.csv.exclusion` with the mastersheet `Exclusion` cell. Each value can match an `exclusion_name` in `data/exclusion_mapping.csv` or a cohort name/code in `data/cohort_mapping.csv`.

Image sends are selected per mastersheet row. If `Clinic_PN_Automation.Image` is non-blank, Stage 3 resolves it through `data/image_mapping.csv` and uses `cohort_mapping.csv.img_campaign_id` as the effective campaign ID.

## Safety

| Mechanism | Purpose |
|---|---|
| Dry-run default | Prevents accidental CleverTap sends |
| `--live` flag | Required before API calls are made |
| Countdown | Gives a final abort window before live sends |
| Campaign logs | Records attempted sends in dry-run and live folders |
| CSV checkpoints | Makes each stage inspectable before continuing |
