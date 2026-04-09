# Usage Guide

---

## Prerequisites

### Install Dependencies

```bash
pip install pandas mysql-connector-python google-auth-oauthlib google-auth-httplib2 google-api-python-client requests python-dotenv
```

### Set Up Credentials

1. Create `.env` in the project root (see [configuration.md](configuration.md))
2. Place `credentials.json` in `secret/`
3. Run Stage 1 once to authenticate with Google (browser prompt will appear to generate `token.json`)

---

## Running the Full Pipeline

Use `run_campaign.py` to run all stages end-to-end.

### Syntax

```bash
python run_campaign.py [--date DDMMYYYY] [--slot {morning,evening,both}] [--live] [--max-workers N] [--cohorts NAME ...]
```

### Common Scenarios

**Preview today's morning campaign (dry-run):**
```bash
python run_campaign.py --slot morning
```

**Preview both slots for a specific date:**
```bash
python run_campaign.py --slot both --date 22032026
```

**Important:** The `--date` and `--slot` flags are propagated across all pipeline stages. If omitted, all stages default to today's date and both slots.

**Preview only specific cohorts:**
```bash
python run_campaign.py --slot morning --cohorts "N2B_All_Bangalore" "Clinic_Birthday"
```

**Run live campaign (morning slot):**
```bash
python run_campaign.py --slot morning --live
```

When running live, a **10-second countdown** will be displayed before Stage 4 triggers campaigns. Press Ctrl+C during the countdown to abort.

**Run live with higher parallelism:**
```bash
python run_campaign.py --slot morning --live --max-workers 50
```

---

## Running Individual Stages

Each stage can be run independently if needed.

### Stage 0 — Refresh Cohort Data from MySQL

Run all SQL queries:
```bash
python fetch_cohorts.py
```

Run a single query:
```bash
python fetch_cohorts.py --query all_rajaji_nagar
```

### Stage 1 — Fetch Mastersheet from Google Sheets

```bash
python 01_fetch_clinic_mastersheet.py
```

Outputs: `data/clinic_mastersheet.csv`

### Stage 2 — Generate Priority Exclusions

```bash
python 02_generate_priority_exclusions.py --date 25032026 --slot evening
```

Outputs: `outputs/25032026_evening/`

### Stage 3 — Prepare Campaign Content

```bash
python 03_prepare_campaign_content.py --output-dir outputs/25032026_evening
```

Enriches the CSVs in the specified output directory.

### Stage 4 — Trigger Campaign

Dry-run (preview only):
```bash
python 04_trigger_campaign.py --output-dir outputs/25032026_evening
```

Live run:
```bash
python 04_trigger_campaign.py --output-dir outputs/25032026_evening --live
```

---

## Interpreting Dry-Run Output

When running in dry-run mode, Stage 4 prints a sample of what would be sent:

```
[DRY-RUN] Cohort: Rajaji_Nagar_n2b_15km  (Priority 1)
  Would send to 4688 users in 5 batch(es).
  Sample payload:
  {
    "to": {"email": ["user1@example.com", "user2@example.com", ...]},
    "campaign_id_list": [1774333510],
    "ExternalTrigger": {
      "title": "Traffic jam? But vet visits > 🚗",
      "body": "Skip the chaos. Get 25% OFF at Supertails+ Clinic...",
      "android_deeplink": "https://supertails.com/...",
      "ios_deeplink": "supertails-com/..."
    }
  }
  (+4 more batch(es) not shown)
```

Review this output before running with `--live`.

---

## Reviewing Outputs

After running the pipeline, check the output directory:

```
outputs/25032026_evening/
├── 01_Rajaji_Nagar_n2b_15km.csv      ← priority 1 users
├── 02_Clinic_KN_Mar_26.csv            ← priority 2 users
├── 03_Clinic_Birthday.csv
├── campaign_meta.csv                  ← templates per cohort
└── summary.csv                        ← exclusion statistics
```

**Check `summary.csv`** to verify exclusion counts look reasonable before going live.

**Check a cohort CSV** to spot-check personalization:
```bash
head outputs/25032026_evening/01_Rajaji_Nagar_n2b_15km.csv
```

---

## Checking Dispatch Logs

After a live run, dispatch logs are at:

```
outputs/log/25032026_evening_dispatch_log.csv
```

The log contains one row per user with timestamp, status code, and the exact title/body sent. Use this for post-campaign auditing or debugging delivery failures.

---

## Troubleshooting

### Google Sheets authentication fails

Delete `secret/token.json` and re-run Stage 1. A browser window will open for re-authentication.

### Cohort CSV not found

Check that the `cohort_dataset` value in `data/deeplink_map.csv` matches an actual filename in `data/cohorts/`. Cohort name matching is normalized (case-insensitive, special characters stripped), but the filename must exist.

### CleverTap API returns 401

Verify `CLEVERTAP_ACCOUNT_ID` and `CLEVERTAP_PASSCODE` in `.env`. Check that the campaign is not paused or archived in CleverTap.

### Zero users in output CSV

Check `summary.csv` — if `input_candidates` is 0, the cohort CSV is empty or missing. If `excluded_by_priority` equals `input_candidates`, the entire cohort was eliminated by higher-priority cohorts on the same date.

### MySQL connection fails (Stage 0)

Confirm you are on the correct VPN/network that has access to `MYSQL_HOST`. Verify credentials in `.env`.

---

## Abort a Live Run

Press **Ctrl+C** during the 5-second countdown (before any API calls) to abort safely.

If batches have already started, Ctrl+C will interrupt in-flight threads. Check the dispatch log to see which users were successfully sent.
