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

The pipeline reads from the sheet tab named **`PN_Automation`** (falls back to the first tab if not found).

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

The `PN_Automation` tab should have these columns (row 1 = headers):

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
