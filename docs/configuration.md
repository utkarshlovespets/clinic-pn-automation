# Configuration

## Environment Variables

Create `.env` in the project root.

```env
SPREADSHEET_ID=<google-sheet-id>
GOOGLE_CREDENTIALS_FILE=credentials.json
GOOGLE_TOKEN_FILE=token.json

CLEVERTAP_ACCOUNT_ID=<clevertap-account-id>
CLEVERTAP_PASSCODE=<clevertap-passcode>
CLEVERTAP_REGION=in1
CLEVERTAP_CAMPAIGN_ID=<optional-fallback-campaign-id>

MYSQL_HOST=<optional-host>
MYSQL_PORT=3306
MYSQL_USER=<optional-user>
MYSQL_PASSWORD=<optional-password>

DEFAULT_SLACK_API_URL=https://slack.com/api/chat.postMessage
DEFAULT_SLACK_CHANNEL=<optional-channel-id>
SLACK_API_TOKEN=<optional-token>
```

| Variable | Used by | Notes |
|---|---|---|
| `SPREADSHEET_ID` | Stage 1 | Google Sheet containing all three required tabs |
| `GOOGLE_CREDENTIALS_FILE` | Stage 1 | OAuth client JSON, resolved relative to project root or `secrets/` |
| `GOOGLE_TOKEN_FILE` | Stage 1 | Cached OAuth token, created automatically |
| `CLEVERTAP_ACCOUNT_ID` | Stage 4 | CleverTap API account ID |
| `CLEVERTAP_PASSCODE` | Stage 4 | CleverTap API passcode |
| `CLEVERTAP_REGION` | Stage 4 | Region such as `in1`, `us1`, or `eu1` |
| `CLEVERTAP_CAMPAIGN_ID` | Stage 4 | Optional fallback only when enriched CSV campaign ID is blank |
| `MYSQL_*` | Stage 1b | Only needed when refreshing cohort CSVs from database |
| `DEFAULT_SLACK_*`, `SLACK_API_TOKEN` | Orchestrator | Optional pipeline status notification |

## Google Sheets

The spreadsheet must contain these tabs exactly:

| Tab | Purpose | Local output |
|---|---|---|
| `Clinic_PN_Automation` | Campaign schedule and copy | `data/clinic_mastersheet.csv` |
| `Cohort_Mapping` | Campaign ID to cohort dataset, default exclusions, and deeplink templates | `data/cohort_mapping.csv` |
| `Exclusion_Mapping` | Exclusion name to exclusion dataset | `data/exclusion_mapping.csv` |

Stage 1 reads `A:Z` from each tab unless an explicit `--range` is used for the mastersheet.

## `Clinic_PN_Automation` Schema

| Column | Required | Notes |
|---|---|---|
| `Date` | Yes | `DD/MM/YYYY` |
| `Day` | No | Human reference |
| `Slot` | Yes | `Morning`/`Evening`; matching is case-insensitive |
| `Cohort Name` | Yes | Friendly label for the mastersheet |
| `Campaign ID` | Yes | Must match `campaign_id` in `Cohort_Mapping` |
| `Exclusion` | No | Comma-separated names from `Exclusion_Mapping.Exclusion Name` |
| `Title` | Yes for sending | Push title template |
| `Content` | Yes for sending | Push body template |

Stage 2 can generate files for blank title/content rows, but `run_campaign.py` validates title/body for the selected run date before continuing.

## `Cohort_Mapping` Schema

| Column | Required | Notes |
|---|---|---|
| `cohort_name` | No | Friendly reference only |
| `cohort_code` | Yes | Required automation key |
| `campaign_id` | Yes | CleverTap External Trigger campaign ID |
| `cohort_dataset` | Yes | File under `data/cohorts/` |
| `android_base_url` | Yes for deeplinks | May contain `{date}` and `{priority}` |
| `ios_base_url` | Yes for deeplinks | May contain `{date}` and `{priority}` |
| `exclusion` | No | Default comma-separated exclusions for this cohort. Values can match `Exclusion_Mapping.Exclusion Name`, `cohort_code`, or `cohort_name` |

Example:

```csv
cohort_name,cohort_code,campaign_id,cohort_dataset,android_base_url,ios_base_url,exclusion
Vaccination Due,Clinic_Vaccination_Due,1776770659,vaccination_due.csv,https://supertails.com/pages/supertails-clinic?utm_campaign={date}_MP_{priority}_Clinic_Vaccine_xxN2B,supertails-com/pages/supertails-clinic?utm_campaign={date}_MP_{priority}_Clinic_Vaccine_xxN2B,Appointment Completed
```

## `Exclusion_Mapping` Schema

| Column | Required | Notes |
|---|---|---|
| `Exclusion Name` | Yes | Name used in mastersheet `Exclusion` cells |
| `Dataset` | Yes | File under `data/cohorts/` |

Example:

```csv
Exclusion Name,Dataset
Appointment Completed,appointment_completed.csv
```

## URL Tokens

`android_base_url` and `ios_base_url` may contain:

| Token | Replacement |
|---|---|
| `{date}` | Campaign date as `DDMonth`, such as `04May` |
| `{priority}` | Slot-tagged priority, such as `1M` or `2E` |

## Secrets

Do not commit:

- `.env`
- `secrets/credentials.json`
- `secrets/token.json`
- Downloaded or generated audience/output files
