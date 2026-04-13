# Configuration

---

## Environment Variables (`.env`)

Create a `.env` file in the project root with the following variables:

```env
# ── Google Sheets ──────────────────────────────────────────────────────────────
SPREADSHEET_ID=<your-google-sheets-id>
GOOGLE_CREDENTIALS_FILE=credentials.json
GOOGLE_TOKEN_FILE=token.json

# ── CleverTap ──────────────────────────────────────────────────────────────────
CLEVERTAP_ACCOUNT_ID=<your-account-id>
CLEVERTAP_PASSCODE=<your-passcode>
CLEVERTAP_REGION=in1
CLEVERTAP_CAMPAIGN_ID=<your-campaign-id>

# ── MySQL (only needed for Stage 0) ───────────────────────────────────────────
MYSQL_HOST=<your-mysql-host>
MYSQL_PORT=3306
MYSQL_USER=<your-username>
MYSQL_PASSWORD=<your-password>
```

### Variable Reference

| Variable | Required By | Description |
|---|---|---|
| `SPREADSHEET_ID` | Stage 1 | Google Sheets document ID (from the sheet URL) |
| `GOOGLE_CREDENTIALS_FILE` | Stage 1 | Filename of OAuth credentials JSON (placed in `secret/`) |
| `GOOGLE_TOKEN_FILE` | Stage 1 | Filename of cached OAuth token (auto-created in `secret/`) |
| `CLEVERTAP_ACCOUNT_ID` | Stage 4 | CleverTap account identifier |
| `CLEVERTAP_PASSCODE` | Stage 4 | CleverTap API passcode |
| `CLEVERTAP_REGION` | Stage 4 | CleverTap regional endpoint (e.g., `in1` for India) |
| `CLEVERTAP_CAMPAIGN_ID` | Stage 4 | Campaign ID for the External Trigger |
| `MYSQL_HOST` | Stage 0 | Hostname of the analytics database |
| `MYSQL_PORT` | Stage 0 | MySQL port (default: 3306) |
| `MYSQL_USER` | Stage 0 | MySQL username |
| `MYSQL_PASSWORD` | Stage 0 | MySQL password |
| `REDIS_URL` | Web + Worker | Redis connection URL for RQ job queue (default local: `redis://localhost:6379/0`) |
| `RQ_QUEUE_NAME` | Web + Worker | Queue name used by enqueue API and worker process (default: `clinic-jobs`) |

---

## Google Sheets Setup

### Step 1: Create OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or use an existing one)
3. Enable the **Google Sheets API**
4. Create **OAuth 2.0 credentials** (Desktop app type)
5. Download the credentials JSON file

### Step 2: Place Credentials

```
secret/credentials.json   ← downloaded OAuth credentials
```

The `secret/token.json` file is auto-created on first run when you authenticate in the browser.

### Step 3: Configure Spreadsheet

Set `SPREADSHEET_ID` in `.env` to the ID from your Google Sheets URL:

```
https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit
```

The pipeline reads from the sheet tab named **`Clinic_PN_Automation`** (falls back to the first tab if not found).

---

## CleverTap Setup

1. Log in to your CleverTap dashboard
2. Navigate to **Settings → Passcode** to get your `CLEVERTAP_ACCOUNT_ID` and `CLEVERTAP_PASSCODE`
3. Create or identify an **External Trigger campaign** and note its `CLEVERTAP_CAMPAIGN_ID`
4. Set `CLEVERTAP_REGION` to match your account region:
   - `in1` — India
   - `us1` — United States
   - `eu1` — Europe
   - (See CleverTap docs for other regions)

---

## `data/deeplink_map.csv`

Maps each cohort name to its cohort data file and deeplink URL templates.

### Schema

| Column | Description |
|---|---|
| `Cohort Name` | Exact cohort name as used in the mastersheet |
| `cohort_dataset` | Filename (without path) of the cohort CSV in `data/cohorts/` |
| `android_base_url` | Android deeplink URL template |
| `ios_base_url` | iOS deeplink URL template |

### URL Templates

URLs can contain these substitution tokens:

- `{date}` — replaced with the campaign date in `DDMonth` format (e.g., `25March`)
- `{priority}` — replaced with the cohort's priority number (e.g., `1`, `2`)

### Example Entry

```csv
Cohort Name,cohort_dataset,android_base_url,ios_base_url
Clinic_Vaccination_Due,vaccination_due.csv,https://supertails.com/pages/clinic?utm_campaign={date}_MP_{priority}_Clinic_xxVAC,supertails-com/pages/clinic?utm_campaign={date}_MP_{priority}_Clinic_xxVAC
```

---

## Google Sheets Mastersheet Format

The `Clinic_PN_Automation` tab should have these columns (row 1 = headers):

| Column | Notes |
|---|---|
| `Date` | Format: `DD/MM/YYYY` |
| `Day` | Day abbreviation: Mon, Tue, etc. |
| `Slot` | `morning` or `evening`; blank cells inherit the last non-blank value |
| `Cohort Name` | Must match a `Cohort Name` in `deeplink_map.csv` |
| `Exclusion` | (Optional) Comma-separated cohort names to exclude |
| `Title` | Push notification title; may use template placeholders |
| `Content` | Push notification body; may use template placeholders |

### Slot Inheritance

If multiple rows share the same date and slot, leave `Slot` blank in continuation rows — the parser fills in the last seen value.

---

## Security Notes

The following files are in `.gitignore` and must never be committed:

```
.env
secret/credentials.json
secret/token.json
```

Never hardcode credentials in script files. All credential access goes through `dotenv_values()` from `.env`.

---

## Render Deployment

When hosting on Render, keep the existing CLI scripts and use the web wrapper in [app.py](../app.py) for manual triggers and archive downloads.

### Required Render Environment Variables

| Variable | Purpose |
|---|---|
| `ADMIN_API_KEY` | Protects the web UI and API endpoints |
| `APP_BASE_URL` | Public URL of the Render web service, used by cron helpers |
| `ENABLE_LIVE_RUNS` | Set to `true` only when you are ready to allow live campaign runs |
| `SPREADSHEET_ID` | Google Sheets ID for Stage 1 |
| `GOOGLE_CREDENTIALS_B64` | Base64-encoded Google OAuth credentials JSON, if you want the app to materialize `secrets/credentials.json` on startup |
| `GOOGLE_TOKEN_B64` | Base64-encoded Google OAuth token JSON, if you want the app to materialize `secrets/token.json` on startup |
| `CLEVERTAP_ACCOUNT_ID` | CleverTap account ID for Stage 4 |
| `CLEVERTAP_PASSCODE` | CleverTap passcode for Stage 4 |
| `CLEVERTAP_CAMPAIGN_ID` | CleverTap campaign ID for Stage 4 |
| `CRON_LIVE` | Cron helper default for live mode (`false` initially) |
| `CRON_MAX_WORKERS` | Default worker count used by the cron trigger helper |
| `REDIS_URL` | Redis connection URL injected from Render Redis service |
| `RQ_QUEUE_NAME` | Queue name used by web enqueue APIs and worker service |

### Worker Startup Commands

Use these commands when running outside Render.

Start web API:

```bash
uvicorn app:app --host 0.0.0.0 --port 10000
```

Start worker (separate terminal):

```bash
rq worker --url redis://localhost:6379/0 clinic-jobs
```

With environment variables:

```bash
set REDIS_URL=redis://localhost:6379/0
set RQ_QUEUE_NAME=clinic-jobs
rq worker --url %REDIS_URL% %RQ_QUEUE_NAME%
```

### Render Notes

- The web app writes a runtime `.env` file on Render if one does not already exist.
- The web app also materializes `secrets/credentials.json` and `secrets/token.json` from the base64-encoded Google environment variables above when provided.
- Render cron jobs should call the web API through `render_cron_trigger.py` so campaign logs and downloads stay on the web service disk.
- In Render, keep web and worker services on the same `REDIS_URL` + `RQ_QUEUE_NAME` pair so jobs are picked up correctly.
