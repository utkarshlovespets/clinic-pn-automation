# Pipeline Stages

## Stage 1: Fetch Google Sheet Configuration

**Script:** `campaign_scripts/01_fetch_clinic_mastersheet.py`

Fetches three tabs from the spreadsheet identified by `SPREADSHEET_ID`:

| Tab | Output |
|---|---|
| `Clinic_PN_Automation` | `data/clinic_mastersheet.csv` |
| `Cohort_Mapping` | `data/cohort_mapping.csv` |
| `Exclusion_Mapping` | `data/exclusion_mapping.csv` |

Command:

```bash
python campaign_scripts/01_fetch_clinic_mastersheet.py
```

Useful options:

```bash
python campaign_scripts/01_fetch_clinic_mastersheet.py --skip-mapping-fetch
python campaign_scripts/01_fetch_clinic_mastersheet.py --output data/clinic_mastersheet.csv --cohort-mapping-output data/cohort_mapping.csv --exclusion-mapping-output data/exclusion_mapping.csv
```

The script also normalizes old `xx%` discount placeholders in mastersheet title/content cells to `10%`.

## Stage 1b: Fetch Cohorts

**Script:** `campaign_scripts/00_fetch_cohorts.py`

When running through `run_campaign.py`, this stage runs only in live mode. It refreshes files in `data/cohorts/`.

You can also maintain `data/cohorts/*.csv` manually as long as filenames match:

- `Cohort_Mapping.cohort_dataset`
- `Exclusion_Mapping.Dataset`

## Stage 2: Generate Priority Exclusions

**Script:** `campaign_scripts/02_generate_priority_exclusions.py`

Reads:

- `data/clinic_mastersheet.csv`
- `data/cohort_mapping.csv`
- `data/exclusion_mapping.csv`
- `data/cohorts/*.csv`

Command:

```bash
python campaign_scripts/02_generate_priority_exclusions.py --date 04052026 --slot morning
```

Behavior:

- Filters mastersheet rows by date and slot.
- Deduplicates repeated campaign IDs within the same date/slot, keeping the first row.
- Maps each row by `Campaign ID` to `cohort_mapping.csv.campaign_id`.
- Loads the mapped `cohort_dataset` audience.
- Applies priority exclusion so a user targeted by a higher-priority row is removed from lower-priority rows.
- Applies default exclusions from `cohort_mapping.csv.exclusion`.
- Applies explicit exclusions from the mastersheet `Exclusion` column.
- Exclusion names can resolve through `exclusion_mapping.csv`, `cohort_mapping.csv.cohort_code`, or `cohort_mapping.csv.cohort_name`.

Outputs:

- `outputs/{DDMMYYYY}_{slot}/NN_{cohort_code}.csv`
- `outputs/{DDMMYYYY}_{slot}/campaign_meta.csv`
- `outputs/{DDMMYYYY}_{slot}/summary.csv`
- `outputs/log/summary/{DDMMYYYY}_{slot}.csv`

Important: `cohort_code` is required in `cohort_mapping.csv`.

## Stage 3: Prepare Campaign Content

**Script:** `campaign_scripts/03_prepare_campaign_content.py`

Reads Stage 2 outputs and `data/cohort_mapping.csv`.

Command:

```bash
python campaign_scripts/03_prepare_campaign_content.py --output-dir outputs/04052026_morning
```

Behavior:

- Resolves `{your pet}`, `{your pet's}`, and `{pet parent}` placeholders per user.
- Resolves campaign ID and URL templates from `cohort_mapping.csv`.
- Builds `android_deeplink` and `ios_deeplink`.
- Rewrites the priority CSVs in place with added columns.

Deeplink replacement:

| Token | Example replacement |
|---|---|
| `{date}` | `04May` |
| `{priority}` | `1M`, `2M`, `1E`, `2E` |

## Stage 4: Trigger Campaign

**Script:** `campaign_scripts/04_trigger_campaign.py`

Reads enriched priority CSVs and triggers CleverTap External Trigger campaigns.

Dry-run:

```bash
python campaign_scripts/04_trigger_campaign.py --output-dir outputs/04052026_morning
```

Live:

```bash
python campaign_scripts/04_trigger_campaign.py --output-dir outputs/04052026_morning --live
```

Behavior:

- Dry-run prints representative payloads and writes dry-run logs.
- Live mode waits through a 10-second countdown, then sends API requests.
- Users are grouped by identical title/body/deeplink/campaign combinations.
- Emails are sent in batches of up to 1000.
- Standalone Stage 4 defaults to `--max-workers 200`.

Logs:

```text
outputs/log/dry_run/{DDMMYYYY}_{slot}_campaign_log.csv
outputs/log/live/{DDMMYYYY}_{slot}_campaign_log.csv
```

## Orchestrator

**Script:** `run_campaign.py`

Runs the full pipeline.

```bash
python run_campaign.py --date 04052026 --slot morning
python run_campaign.py --date 04052026 --slot morning --live
```

Current defaults:

| Option | Default |
|---|---|
| `--slot` | `both`, unless auto-slot mode is active |
| `--cohort-map` | `data/cohort_mapping.csv` |
| `--exclusion-map` | `data/exclusion_mapping.csv` |
| `--deeplink-map` | defaults to `--cohort-map` |
| `--output-dir` | `outputs` |
| `--max-workers` | `10` |

Auto-slot mode is active when `run_campaign.py` is called with no flags or only `--live`. It uses IST time: before 14:00 is morning, 14:00 and later is evening.

Before running later stages, the orchestrator validates that selected mastersheet rows have both title and content.
