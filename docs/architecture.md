# Architecture

## System Overview

The pipeline is a linear, CSV-based data pipeline. Each stage reads from files produced by the previous stage and writes its own output files. No shared state is held in memory between stages — all intermediate data lives on disk as CSVs.

This design makes it easy to:
- Inspect data at any stage before proceeding
- Re-run individual stages without re-running the full pipeline
- Debug failures by examining intermediate files

---

## Data Flow

```
┌─────────────────────────────────────────────┐
│                  DATA SOURCES               │
│                                             │
│  Google Sheets          MySQL Database      │
│  (campaign schedule)    (customer data)     │
└────────────┬────────────────────┬───────────┘
             │                    │
             ▼                    ▼
  ┌──────────────────┐  ┌──────────────────────┐
  │ Stage 1          │  │ Stage 0 (optional)   │
  │ Fetch Mastersheet│  │ Fetch Cohorts        │
  └────────┬─────────┘  └──────────┬───────────┘
           │                       │
           ▼                       ▼
  data/clinic_mastersheet.csv   data/cohorts/*.csv
           │                       │
           └───────────┬───────────┘
                       │  + data/deeplink_map.csv
                       ▼
           ┌───────────────────────────┐
           │ Stage 2                   │
           │ Generate Priority         │
           │ Exclusions                │
           └──────────────┬────────────┘
                          │
                          ▼
           outputs/{date}_{slot}/
             ├── 01_Cohort.csv
             ├── 02_Cohort.csv
             ├── campaign_meta.csv
             └── summary.csv
                          │
                          ▼
           ┌───────────────────────────┐
           │ Stage 3                   │
           │ Prepare Campaign Content  │
           │ (personalization +        │
           │  deeplinks injected)      │
           └──────────────┬────────────┘
                          │
                          ▼
           outputs/{date}_{slot}/
             └── NN_Cohort.csv  (enriched with
                                 title, body, deeplinks)
                          │
                          ▼
           ┌───────────────────────────┐
           │ Stage 4                   │
           │ Trigger Campaign          │
           │                           │
           │  Dry-run: print payloads  │
           │  Live: POST to CleverTap  │
           └──────────────┬────────────┘
                          │
                          ▼
           outputs/log/{date}_{slot}_dispatch_log.csv
```

---

## Component Responsibilities

### Orchestrator (`run_campaign.py`)

Imports and runs Stages 1–4 sequentially via `importlib`. Handles:
- Command-line argument parsing (date, slot, cohort filter, live mode, max-workers)
- Safety disclaimers and confirmation prompts before live runs
- Passing context (output directory, date, slot) between stages

### Stage 0 — Fetch Cohorts (`fetch_cohorts.py`)

Optional. Connects to MySQL and runs SQL files from `data/queries/` to produce cohort CSVs in `data/cohorts/`. Only needed when cohort data needs refreshing from the database. Cohort CSVs can also be provided manually.

### Stage 1 — Fetch Mastersheet (`01_fetch_clinic_mastersheet.py`)

Authenticates with Google Sheets via OAuth 2.0 and downloads the campaign schedule. The mastersheet defines which cohorts are targeted on which dates, in which slot, and with what message templates.

### Stage 2 — Priority Exclusions (`02_generate_priority_exclusions.py`)

The core business logic stage. Applies a two-layer exclusion model:

1. **Priority exclusion:** Users in higher-priority cohorts are removed from lower-priority ones, so no user receives duplicate notifications.
2. **Explicit exclusion:** The `Exclusion` column in the mastersheet can name additional cohorts whose members should be removed from a given cohort.

Outputs one CSV per cohort (numbered by priority) plus a `summary.csv` with exclusion statistics.

### Stage 3 — Campaign Content (`03_prepare_campaign_content.py`)

Reads per-user data and resolves template placeholders (`{your pet}`, `{your pet's}`, `{pet parent}`) against actual first names and pet names. Constructs deeplink URLs by substituting `{date}` and `{priority}` into URL templates from `data/deeplink_map.csv`.

### Stage 4 — Trigger Campaign (`04_trigger_campaign.py`)

Groups users by identical (title, body, deeplinks) tuples, chunks each group into batches of up to 1000 emails, and sends one API request per batch. Runs batches in parallel via `ThreadPoolExecutor`. In live mode, retries failed requests up to 3 times with exponential backoff.

---

## Parallelism Model

Stage 4 uses Python's `ThreadPoolExecutor` for parallel HTTP calls to CleverTap. The default worker count is 30 and can be overridden with `--max-workers`. Batching is I/O-bound (network), so threading is appropriate.

Stages 1–3 are single-threaded and sequential.

---

## Output Directory Naming

All intermediate and final outputs for a campaign run are placed under:

```
outputs/{DDMMYYYY}_{morning|evening}/
```

Example: `outputs/25032026_evening/`

This namespacing means multiple campaign runs can coexist without overwriting each other.

---

## Safety Architecture

| Mechanism | Purpose |
|---|---|
| Dry-run default | No API calls unless `--live` is passed |
| 5-second countdown | Allows abort before live execution starts |
| Sample payload preview | Review what will be sent before committing |
| Retry with backoff | Handles transient network failures gracefully |
| Dispatch log | Full audit trail of every send attempt |
| Credential gitignore | Prevents accidental credential commits |
