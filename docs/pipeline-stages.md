# Pipeline Stages

---

## Stage 0: Fetch Cohorts

**File:** `fetch_cohorts.py`
**Status:** Optional — only run when cohort data needs refreshing from the database.

### What It Does

Connects to the Supertails analytics MySQL database, executes SQL query files from `data/queries/`, and saves results as CSVs in `data/cohorts/`.

### Input

- `data/queries/*.sql` — one SQL file per cohort
- `.env` — MySQL connection credentials (`MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`)

### Output

- `data/cohorts/{cohort_name}.csv` — columns: `email`, `first_name`, `pet_name`

### Usage

```bash
# Run all queries
python fetch_cohorts.py

# Run a single query
python fetch_cohorts.py --query all_rajaji_nagar
```

### SQL Query Pattern

Each SQL file typically:
1. Joins `vw_cx_email` (email data), `vw_cx_pins` (pincode data), and `cx_pet_profile` (pet data)
2. Filters by criteria such as pincode, behavior, or service type
3. Extracts `email`, normalized first name (strips titles like "Dr", "Mr"), and pet name

---

## Stage 1: Fetch Clinic Mastersheet

**File:** `campaign_scripts/01_fetch_clinic_mastersheet.py`

### What It Does

Downloads the campaign schedule from a Google Sheets spreadsheet using the Google Sheets API v4. The mastersheet defines what campaigns run on what dates, in which slot, with what message content.

### Input

- Google Sheets spreadsheet (sheet name: `Clinic_PN_Automation`)
- `.env` — `SPREADSHEET_ID`, `GOOGLE_CREDENTIALS_FILE`, `GOOGLE_TOKEN_FILE`
- `secret/credentials.json` — Google OAuth 2.0 app credentials
- `secret/token.json` — Cached OAuth token (auto-refreshed)

### Output

- `data/clinic_mastersheet.csv`

### Mastersheet Column Schema

| Column | Description |
|---|---|
| `Date` | Campaign date in `DD/MM/YYYY` format |
| `Day` | Day of week (e.g., "Mon", "Tue") |
| `Slot` | `morning` or `evening` (blank rows inherit the last non-blank slot) |
| `Cohort Name` | Friendly label for your reference |
| `Campaign ID` | Identifier matching `campaign_id` in `data/cohort_mapping.csv` |
| `Exclusion` | (Optional) Comma-separated cohort names to exclude from this cohort |
| `Title` | Push notification title template |
| `Content` | Push notification body template |

### Notes

- Falls back to the first sheet if `Clinic_PN_Automation` tab is not found
- Pads rows with empty strings to normalize row lengths

---

## Stage 2: Generate Priority Exclusions

**File:** `campaign_scripts/02_generate_priority_exclusions.py`

### What It Does

The core logic stage. Reads the mastersheet for a given date/slot, loads the corresponding cohort CSVs, applies deduplication and exclusion rules, and writes per-priority output CSVs.

### Date and Slot Propagation

When run via `run_campaign.py`, this stage automatically receives the `--date` and `--slot` arguments from the orchestrator. This ensures all pipeline stages remain aligned to the same date and slot.

- If `--date` is omitted, defaults to today's date
- If `--slot` is omitted, defaults to processing both morning and evening

**Standalone use:** You can also run Stage 2 directly with explicit date/slot:
```bash
python campaign_scripts/02_generate_priority_exclusions.py --date 25032026 --slot morning
```

### Input

- `data/clinic_mastersheet.csv`
- `data/cohort_mapping.csv`
- `data/exclusion_mapping.csv`
- `data/cohorts/*.csv`

### Output (in `outputs/{DDMMYYYY}_{slot}/`)

- `NN_CohortName.csv` — one file per cohort, numbered by priority (01, 02, ...)
- `campaign_meta.csv` — metadata linking priority number to cohort name and message templates
- `summary.csv` — exclusion statistics per cohort

### Exclusion Logic

Cohorts are processed in spreadsheet row order (top = highest priority).

**Priority exclusion:** When processing cohort N, any user already assigned to cohorts 1 through N-1 is removed. This ensures no user receives more than one notification per slot.

**Column-based exclusion:** If the `Exclusion` column names one or more cohorts, members of those cohorts are additionally removed from the current cohort's final list (regardless of priority order).

### Campaign ID Matching

Campaign rows are mapped by `Campaign ID`, not by the mastersheet `Cohort Name`. Cohort codes are still normalized for explicit exclusions and older generated outputs: stripped of apostrophes, lowercased, and non-alphanumeric characters removed.

```
"Clinic_Gut_N2B_Mar'26"  →  "clinicgutn2bmar26"
```

### `campaign_meta.csv` Schema

| Column | Description |
|---|---|
| `priority` | Integer (1 = highest priority) |
| `cohort_name` | Raw cohort name from mastersheet |
| `title` | Title template (may contain placeholders) |
| `body` | Body template (may contain placeholders) |
| `android_base_url` | Android deeplink URL template |
| `ios_base_url` | iOS deeplink URL template |

### `summary.csv` Schema

| Column | Description |
|---|---|
| `priority` | Priority number |
| `cohort_name` | Cohort name |
| `input_candidates` | Total users in raw cohort CSV |
| `excluded_by_priority` | Users removed by priority deduplication |
| `excluded_by_exclusion_col` | Users removed by explicit exclusion column |
| `final_count` | Users remaining after all exclusions |

---

## Stage 3: Prepare Campaign Content

**File:** `campaign_scripts/03_prepare_campaign_content.py`

### What It Does

Reads the per-priority cohort CSVs and the `campaign_meta.csv`, resolves message templates per user, and builds personalized deeplink URLs. Overwrites each cohort CSV with enriched columns.

### Input

- `outputs/{DDMMYYYY}_{slot}/NN_CohortName.csv` (columns: `Email`, `First Name`, `Pet Name`)
- `outputs/{DDMMYYYY}_{slot}/campaign_meta.csv`
- `data/cohort_mapping.csv`

### Output

- Same cohort CSVs, enriched with additional columns: `title`, `body`, `android_deeplink`, `ios_deeplink`

### Template Placeholders

Message templates in the mastersheet can use these placeholders (case-insensitive, single or double braces accepted):

| Placeholder | Replaced With | Fallback |
|---|---|---|
| `{your pet}` | Pet's name | `"your pet"` |
| `{your pet's}` | Pet's name + `'s` | `"your pet's"` |
| `{pet parent}` | Customer's first name | `"pet parent"` |

If the placeholder appears at the start of a sentence, the replacement is capitalized.

**Example:**

Template: `"Drop by for {your pet}'s FREE consultation, {pet parent}!"`
Result (Radha, pet: Remus): `"Drop by for Remus's FREE consultation, Radha!"`
Result (no names): `"Drop by for your pet's FREE consultation, pet parent!"`

### Deeplink URL Construction

Deeplink base URLs from `cohort_mapping.csv` may contain these substitution tokens:

| Token | Replaced With | Example |
|---|---|---|
| `{date}` | Date in `DDMonth` format | `25March` |
| `{priority}` | Slot-tagged priority token | `1M`, `2M`, `1E`, `2E` |

**Priority Token Format:**
- **Morning slot:** `1M`, `2M`, `3M`, ... (prioritized 1 to N per date)
- **Evening slot:** `1E`, `2E`, `3E`, ... (prioritized 1 to N per date)

Stage 3 automatically detects the slot from the output directory path (e.g., `outputs/25032026_morning/`) and applies the correct suffix (`M` or `E`) to each priority number. This allows campaign analytics to distinguish morning vs evening sends purely from the UTM `priority` parameter.

This allows tracking which cohorts were delivered in morning vs evening through the UTM parameter alone, without needing to cross-reference the run date/slot separately.

**Example:**

Template: `https://supertails.com/pages/clinic?utm_campaign={date}_MP_{priority}_Clinic_xxRAJ`

Morning priority 1 result: `https://supertails.com/pages/clinic?utm_campaign=25March_MP_1M_Clinic_xxRAJ`

Evening priority 2 result: `https://supertails.com/pages/clinic?utm_campaign=25March_MP_2E_Clinic_xxRAJ`

---

## Stage 4: Trigger Campaign

**File:** `campaign_scripts/04_trigger_campaign.py`

### What It Does

Sends push notifications by calling the CleverTap External Trigger API. Reads enriched cohort CSVs, groups users by identical content, chunks into batches of 1000, and sends one API request per batch.

### Input

- `outputs/{DDMMYYYY}_{slot}/NN_CohortName.csv` (enriched)
- `.env` — `CLEVERTAP_ACCOUNT_ID`, `CLEVERTAP_PASSCODE`, `CLEVERTAP_REGION`, optional `CLEVERTAP_CAMPAIGN_ID` fallback

### Output

- `outputs/log/{DDMMYYYY}_{slot}_dispatch_log.csv` — one row per send attempt

### API Payload Structure

```json
{
  "to": {
    "email": ["user1@example.com", "user2@example.com"]
  },
  "campaign_id_list": ["<campaign_id_for_this_cohort>"],
  "ExternalTrigger": {
    "title": "Personalized title here",
    "body": "Personalized body here",
    "android_deeplink": "https://...",
    "ios_deeplink": "supertails-com/..."
  }
}
```

Headers: `X-CleverTap-Account-Id`, `X-CleverTap-Passcode`, `Content-Type: application/json`

### Batching Logic

1. Group all users in a cohort by their unique (title, body, android_deeplink, ios_deeplink) tuple
2. For each unique content group, split emails into chunks of 1000
3. Send one POST request per chunk

This minimizes API calls (users with identical content share a single request) while staying within CleverTap's per-request limits.

### Dry-Run vs Live Mode

**Dry-run (default):**
- Prints one sample payload per cohort showing what would be sent
- Prints a tally of how many additional batches would follow
- Makes zero API calls

**Live mode (`--live` flag):**
- Shows a 5-second countdown (abort with Ctrl+C)
- Sends real API requests
- Retries failures up to 3 times (backoff: 2s, 4s)
- Logs every attempt to the dispatch log

### Dispatch Log Schema

| Column | Description |
|---|---|
| `email` | Recipient email address |
| `cohort_name` | Cohort this user belongs to |
| `priority` | Priority number |
| `title` | Notification title sent |
| `body` | Notification body sent |
| `dry_run` | `True` or `False` |
| `timestamp` | ISO 8601 timestamp of the attempt |
| `status` | HTTP status code or error message |

### Parallel Execution

Batches across all cohorts are dispatched in parallel using `ThreadPoolExecutor`. Default worker count: 30. Override with `--max-workers N`.

---

## Orchestrator: Run Campaign

**File:** `run_campaign.py`

Runs Stages 1 through 4 end-to-end for a given date and slot.

### Usage

```bash
python run_campaign.py [OPTIONS]

Options:
  --date DDMMYYYY         Campaign date (default: today)
  --slot {morning,evening,both}
                          Which slot to run (default: morning)
  --live                  Enable live API calls (default: dry-run)
  --max-workers N         Parallel threads for Stage 4 (default: 30)
  --cohorts NAME [NAME ...]
                          Only process these cohort names
```

### Examples

```bash
# Safe preview — today's morning slot
python run_campaign.py --slot morning

# Preview both slots for a past/future date
python run_campaign.py --slot both --date 22032026

# Live run — morning slot (authorized only)
python run_campaign.py --slot morning --live

# Target specific cohorts only
python run_campaign.py --slot morning --cohorts "N2B_All_Bangalore" "Clinic_KN_Mar26"

# High-throughput live run
python run_campaign.py --slot morning --live --max-workers 50
```
