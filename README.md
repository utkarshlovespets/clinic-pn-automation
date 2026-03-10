# CleverTap Automation Pipeline

This documentation details the architecture, configuration, and operational procedures for the automated mobile push notification pipeline developed for Supertails.

---

## 1. Project Overview

The system is an **External Trigger** pipeline that automates campaign delivery by bridging Google Sheets with the CleverTap Identity API. It allows complex business logic — specifically waterfall exclusions — to be calculated locally before messages are dispatched.

### Core Goals

* **Centralized Orchestration:** Manage all daily push content and schedules via a Google Sheet control center (`PN_Calendar` worksheet).
* **Waterfall Exclusion Logic:** Prioritize campaigns so a user qualifying for multiple cohorts only receives the highest-priority notification.
* **Auditability:** Generate a `summary.csv` per slot run with per-cohort candidate counts, exclusions, and final recipient counts.

---

## 2. Project Structure

```
clevertap-automation-pipeline/
├── 01_fetch_data.py                  # Step 1: Fetch clinic master sheet from Google Sheets
├── 02_generate_priority_exclusions.py # Step 2: Apply waterfall exclusion logic, write output CSVs
├── 03_schedule_campaign.py           # Step 3: Push audiences to CleverTap API (upcoming)
├── requirements.txt
├── .env                              # Local secrets & config (never commit)
├── data/
│   ├── clinic_mastersheet.csv        # Fetched from Google Sheets by Step 1
│   ├── master_cohort.csv             # Email → cohort tag mapping
│   └── test_users.csv
├── outputs/
│   └── <DDMMYYYY>_<slot>/            # One folder per date+slot run
│       ├── 01_<CohortName>.csv
│       ├── 02_<CohortName>.csv
│       └── summary.csv
└── secret/
    ├── credentials.json              # Google OAuth client credentials
    └── token.json                    # Auto-managed OAuth token
```

---

## 3. Pipeline Steps

### Step 1 — `01_fetch_data.py`

Fetches the `PN_Calendar` worksheet from Google Sheets via the Sheets API v4 and saves it as `data/clinic_mastersheet.csv`.

**Config read from `.env`:**

| Key | Description |
|---|---|
| `SPREADSHEET_ID` | Google Sheets document ID (from the URL) |
| `GOOGLE_CREDENTIALS_FILE` | Path to OAuth credentials JSON (default: `secret/credentials.json`) |
| `GOOGLE_TOKEN_FILE` | Path to OAuth token JSON (default: `secret/token.json`) |

**Hardcoded constant in script:**

| Constant | Value |
|---|---|
| `WORKSHEET_NAME` | `PN_Calendar` |

**Run:**
```bash
python 01_fetch_data.py
```

**Optional CLI overrides:**
```
--spreadsheet-id   Override SPREADSHEET_ID from .env
--range            Explicit A1 range (e.g. 'Sheet1'!A:Z)
--credentials      Path to credentials JSON
--token            Path to token JSON
--output           Output CSV path (default: data/clinic_mastersheet.csv)
```

**Auth flow:** On first run, a browser window opens for OAuth consent. The token is saved to `secret/token.json` and auto-refreshed on subsequent runs.

---

### Step 2 — `02_generate_priority_exclusions.py`

Reads `clinic_mastersheet.csv` and `master_cohort.csv`, then generates per-priority audience CSVs for:
- The **latest date** in the sheet (morning + evening)
- The **following date** (morning + evening)

Slot combos with no data in the sheet are silently skipped.

**Waterfall Exclusion Logic:**

Rows in the sheet are processed top-to-bottom. Row order = priority order. A user matched for cohort N is excluded from all lower-priority cohorts for that slot run.

**Cohort matching:**

Cohort names from the sheet are matched against the `Tags` column in `master_cohort.csv` using normalized alphanumeric comparison. Exact match is preferred; substring match is used as fallback.

**Output per slot** (`outputs/<DDMMYYYY>_<slot>/`):

| File | Contents |
|---|---|
| `01_<CohortName>.csv` | Priority 1 final audience (Email column) |
| `02_<CohortName>.csv` | Priority 2 final audience |
| … | … |
| `summary.csv` | Per-cohort: candidates, excluded, final count, output file |

**Run:**
```bash
python 02_generate_priority_exclusions.py
```

**Optional CLI overrides:**
```
--clinic-csv    Path to clinic master sheet CSV (default: data/clinic_mastersheet.csv)
--cohort-csv    Path to master cohort CSV (default: data/master_cohort.csv)
--output-dir    Base output directory (default: outputs/)
```

---

### Step 3 — `03_schedule_campaign.py`

*(In development)* — Will push the generated audience CSVs to the CleverTap API to trigger mobile push notifications.

---

## 4. Configuration — `.env`

```dotenv
# Google Sheets
SPREADSHEET_ID=<your-spreadsheet-id>
GOOGLE_CREDENTIALS_FILE=credentials.json
GOOGLE_TOKEN_FILE=token.json

# CleverTap API
CLEVERTAP_ACCOUNT_ID=<account-id>
CLEVERTAP_PASSCODE=<passcode>
CLEVERTAP_REGION=in1
```

> `.env` is read directly via `dotenv_values()` — OS-level environment variables do **not** interfere with these values.

---

## 5. Requirements

Install dependencies:
```bash
pip install -r requirements.txt
```

`requirements.txt`:
```
pandas
requests
python-dotenv
google-auth
google-auth-oauthlib
google-api-python-client
```

---

## 6. Running the Full Pipeline

```bash
# Step 1: Fetch latest sheet data
python 01_fetch_data.py

# Step 2: Generate exclusion lists for latest + next day (all slots)
python 02_generate_priority_exclusions.py
```

Step 3 (CleverTap dispatch) will be added once `03_schedule_campaign.py` is complete.

---

## 7. Security Notes

* `secret/`, `.env`, `token.json`, and `credentials.json` must never be committed to version control.
* All exclusion logic runs locally — no customer PII is uploaded to any external server.
* CleverTap sends are Identity-scoped: only the exact email list provided can be messaged; no accidental bulk sends are possible.