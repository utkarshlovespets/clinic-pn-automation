"""Trigger CleverTap External Trigger campaigns from priority exclusion outputs.

DISCLAIMER: This script defaults to DRY-RUN mode.
    No actual CleverTap API calls are made unless --live is explicitly passed.
    The --live flag must NEVER be used until the project is fully tested
    and approved by the team.

Expects 03_prepare_campaign_content.py to have already been run so that
each priority CSV contains title and body columns.

Usage (dry-run, safe default):
    python 04_trigger_campaign.py --output-dir outputs/19032026_morning

Usage (dry-run, specific cohorts only):
    python 04_trigger_campaign.py --output-dir outputs/19032026_morning --cohorts "N2B_All_Bangalore" "Clinic_KN_Mar26"

Usage (live -- only when authorised):
    python 04_trigger_campaign.py --output-dir outputs/19032026_morning --live
"""

import argparse
import csv
import json
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from dotenv import dotenv_values

# Allow importing shared modules from project root when running from campaign_scripts/.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# -- Constants -----------------------------------------------------------------

BATCH_SIZE = 1000
MAX_WORKERS_DEFAULT = 50
LIVE_COUNTDOWN_SECONDS = 10
LOG_FIELDS = ["timestamp", "email", "utm_name", "clicked", "title", "body"]

DISCLAIMER_DRY_RUN = """
================================================================================
  [DRY-RUN MODE] No API calls will be made (default).
  Review the payload preview below before running with --live.
  NEVER use --live until the project is fully tested and approved.
================================================================================
"""

DISCLAIMER_LIVE = """
================================================================================
  !! LIVE MODE ACTIVE !! REAL API CALLS WILL BE MADE TO CLEVERTAP !!
  Campaigns will be triggered. This action cannot be undone.
  Press Ctrl+C within {n} seconds to abort...
================================================================================
"""


def extract_utm_name(android_deeplink: str, ios_deeplink: str) -> str:
    """Extract utm_name value from deeplink URL query params.

    Uses utm_campaign as the primary key used in current deeplink templates.
    Falls back to utm_name if present.
    """
    for deeplink in (android_deeplink, ios_deeplink):
        if not deeplink:
            continue
        parsed = urlparse(deeplink)
        query = parse_qs(parsed.query)
        for key in ("utm_campaign", "utm_name"):
            values = query.get(key)
            if values:
                return str(values[0]).strip()
    return ""


# -- Job building --------------------------------------------------------------

def build_jobs(
    output_dir: Path,
    cohorts: Optional[Set[str]] = None,
) -> List[Tuple[str, int, List[str], str, str, str, str, str]]:
    """Build the full list of API call jobs from an output directory.

    Reads title and body directly from enriched priority CSVs (produced by
    03_prepare_campaign_content.py).  For each priority CSV:
      1. Loads Email, title, body.
      2. Optionally filters by cohort name (if cohorts set is provided).
      3. Groups users by identical (title, body) content.
      4. Chunks each group into batches of BATCH_SIZE.

    Args:
        output_dir: Directory containing priority CSVs and campaign_meta.csv.
        cohorts:    Optional set of cohort names to include. If None, all
                    cohorts are processed.

    Returns:
        List of (cohort_name, priority, batch_emails, copy1, copy2,
                 android_dl, ios_dl, output_dir_name) tuples in priority order.

    Raises:
        FileNotFoundError: If campaign_meta.csv is missing.
        ValueError: If required columns are absent from campaign_meta.csv.
    """
    meta_path = output_dir / "campaign_meta.csv"
    if not meta_path.exists():
        raise FileNotFoundError(
            f"campaign_meta.csv not found in {output_dir}. "
            "Run 02_generate_priority_exclusions.py first."
        )

    meta_df = pd.read_csv(meta_path, dtype=str, keep_default_na=False)
    required_meta = {"priority", "cohort_name"}
    missing = required_meta - set(meta_df.columns)
    if missing:
        raise ValueError(f"campaign_meta.csv is missing columns: {sorted(missing)}")

    # Build priority -> cohort_name lookup (fallback when summary.csv is absent).
    cohort_name_lookup: dict = {}
    for _, mrow in meta_df.iterrows():
        try:
            p = int(mrow["priority"])
        except (ValueError, TypeError):
            continue
        cohort_name_lookup[p] = str(mrow["cohort_name"]).strip()

    # Optional file-level mapping from summary.csv.
    # This prevents stale NN_*.csv files (from older runs) from being triggered.
    summary_map = {}
    summary_path = output_dir / "summary.csv"
    if summary_path.exists():
        summary_df = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
        required_summary = {"priority", "cohort_name", "output_file"}
        missing_summary = required_summary - set(summary_df.columns)
        if missing_summary:
            raise ValueError(f"summary.csv is missing columns: {sorted(missing_summary)}")
        for _, srow in summary_df.iterrows():
            output_file = str(srow.get("output_file", "")).strip()
            if not output_file:
                continue
            try:
                summary_priority = int(str(srow.get("priority", "")).strip())
            except (ValueError, TypeError):
                continue
            summary_map[output_file] = {
                "priority": summary_priority,
                "cohort_name": str(srow.get("cohort_name", "")).strip(),
            }

    # Discover priority CSVs.
    if summary_map:
        csv_files = [
            output_dir / output_name
            for output_name in sorted(summary_map)
            if (output_dir / output_name).exists()
        ]
        stale_files = sorted(
            p.name for p in output_dir.glob("[0-9][0-9]_*.csv") if p.name not in summary_map
        )
        if stale_files:
            print(
                f"  [INFO] Skipping {len(stale_files)} stale CSV(s) not present in summary.csv."
            )
    else:
        # Legacy fallback: process whatever NN_*.csv files are present.
        csv_files = sorted(output_dir.glob("[0-9][0-9]_*.csv"))

    if not csv_files:
        print("  [WARNING] No priority CSV files found in output directory.")
        return []

    output_dir_name = output_dir.name
    all_jobs: List[Tuple[str, int, List[str], str, str, str, str, str]] = []

    for csv_path in csv_files:
        file_meta = summary_map.get(csv_path.name)
        if file_meta is not None:
            priority = file_meta["priority"]
            cohort_name = file_meta["cohort_name"]
        else:
            try:
                priority = int(csv_path.stem.split("_")[0])
            except (ValueError, IndexError):
                print(f"  [WARNING] Skipping file with unexpected name: {csv_path.name}")
                continue

            cohort_name = cohort_name_lookup.get(priority, csv_path.stem)

        # Apply cohort filter if specified.
        if cohorts is not None and cohort_name not in cohorts:
            print(f"  [INFO] {csv_path.name}: cohort '{cohort_name}' not in filter -- skipped.")
            continue

        users_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

        if users_df.empty or "Email" not in users_df.columns:
            print(f"  [INFO] {csv_path.name}: 0 users -- skipped.")
            continue

        # Require title / body columns (populated by script 03).
        for col in ("title", "body"):
            if col not in users_df.columns:
                print(
                    f"  [WARNING] {csv_path.name}: missing '{col}' column. "
                    "Run 03_prepare_campaign_content.py first -- skipping."
                )
                break
        else:
            # Deeplinks are per-cohort -- same value for every row in this file.
            android_dl = str(users_df["android_deeplink"].iloc[0]).strip() \
                if "android_deeplink" in users_df.columns else ""
            ios_dl = str(users_df["ios_deeplink"].iloc[0]).strip() \
                if "ios_deeplink" in users_df.columns else ""

            # Group users by identical (title, body) content.
            groups: dict = {}
            for _, urow in users_df.iterrows():
                email = str(urow.get("Email", "")).strip().lower()
                if not email:
                    continue
                copy1 = str(urow.get("title", "")).strip()
                copy2 = str(urow.get("body", "")).strip()
                groups.setdefault((copy1, copy2), []).append(email)

            # Chunk each group into batches of BATCH_SIZE.
            for (copy1, copy2), emails in groups.items():
                for i in range(0, len(emails), BATCH_SIZE):
                    batch = emails[i : i + BATCH_SIZE]
                    all_jobs.append((
                        cohort_name, priority, batch, copy1, copy2,
                        android_dl, ios_dl, output_dir_name,
                    ))

    return all_jobs


# -- API call ------------------------------------------------------------------

def send_request(
    job: Tuple[str, int, List[str], str, str, str, str, str],
    headers: dict,
    url: str,
    campaign_id: int,
    dry_run: bool,
    previewed_cohorts: Optional[Set[str]] = None,
    previewed_lock: Optional[Lock] = None,
) -> str:
    """Send (or mock-send) a single batch API call to CleverTap.

    Args:
        job:               (cohort_name, priority, batch_emails, copy1, copy2,
                            android_deeplink, ios_deeplink, output_dir_name)
        headers:           CleverTap auth headers.
        url:               External Trigger API endpoint.
        campaign_id:       Integer campaign ID from .env.
        dry_run:           If True, print payload instead of making HTTP request.
        previewed_cohorts: Shared set of cohort names whose payload has already
                           been printed (one preview per cohort in dry-run).
        previewed_lock:    Lock protecting previewed_cohorts.

    Returns:
        Human-readable result string.
    """
    cohort_name, priority, emails, copy1, copy2, android_dl, ios_dl, _ = job

    ext_trigger = {"title": copy1, "body": copy2}
    if android_dl:
        ext_trigger["android_deeplink"] = android_dl
    if ios_dl:
        ext_trigger["ios_deeplink"] = ios_dl

    payload = {
        "to": {"email": emails},
        "campaign_id_list": [campaign_id],
        "ExternalTrigger": ext_trigger,
    }

    label = f"[P{priority:02d} | {cohort_name} | {len(emails)} email(s)]"

    if dry_run:
        # Print the full payload only once per cohort; subsequent batches just
        # show a compact summary line so the terminal stays readable.
        first_preview = False
        if previewed_cohorts is not None and previewed_lock is not None:
            with previewed_lock:
                if cohort_name not in previewed_cohorts:
                    previewed_cohorts.add(cohort_name)
                    first_preview = True
        else:
            first_preview = True  # fallback: always print

        if first_preview:
            preview_emails = emails[:3]
            if len(emails) > 3:
                preview_emails = preview_emails + [f"+{len(emails) - 3} more"]
            preview = dict(payload)
            preview["to"] = {"email": preview_emails}
            print(f"\n  {label} DRY-RUN payload:")
            print("  " + json.dumps(preview, ensure_ascii=True, indent=4).replace("\n", "\n  "))
        # Additional batches are silent here; run_parallel prints a tally at the end.
        return f"{label} -> [DRY-RUN] skipped"

    max_attempts = 3
    last_error = ""
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return f"{label} -> OK | {data.get('message', '')}"
                return f"{label} -> CleverTap error | {data.get('error', response.text)}"
            return f"{label} -> HTTP {response.status_code} | {response.text[:200]}"
        except requests.exceptions.RequestException as exc:
            last_error = str(exc)
            if attempt < max_attempts:
                time.sleep(2 * attempt)  # 2s after attempt 1, 4s after attempt 2
    return f"{label} -> ERROR (failed after {max_attempts} attempts) | {last_error}"


# -- Parallel dispatcher -------------------------------------------------------

def run_parallel(
    jobs: List[Tuple],
    headers: dict,
    url: str,
    campaign_id: int,
    dry_run: bool,
    max_workers: int = MAX_WORKERS_DEFAULT,
    log_dir: Optional[Path] = None,
) -> None:
    """Dispatch all jobs in parallel using ThreadPoolExecutor.

    After dispatch, writes a per-slot-directory CSV campaign log to log_dir
    (if provided), separated into dry_run/live subfolders.
    Log columns: timestamp, email, utm_name, clicked, title, body.
    """
    if not jobs:
        print("  No jobs to process.")
        return

    print(f"\nTotal API calls to make: {len(jobs)}")
    if dry_run:
        print("(DRY-RUN: payloads printed below, no HTTP requests)\n")

    log_rows: List[dict] = []

    # Shared state so each cohort's payload / result is printed only once in dry-run.
    previewed_cohorts: Set[str] = set()
    previewed_lock = Lock()
    # Track suppressed result lines per cohort (dry-run only).
    printed_result_cohorts: Set[str] = set()
    suppressed_counts: dict = defaultdict(int)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                send_request, job, headers, url, campaign_id, dry_run,
                previewed_cohorts, previewed_lock,
            ): job
            for job in jobs
        }
        for future in as_completed(futures):
            job = futures[future]
            cohort_name = job[0]
            result_str = future.result()

            if dry_run:
                # Only print one result line per cohort; tally the rest silently.
                if cohort_name not in printed_result_cohorts:
                    printed_result_cohorts.add(cohort_name)
                    print(result_str)
                else:
                    suppressed_counts[cohort_name] += 1
            else:
                print(result_str)

            cohort_name, priority, emails, copy1, copy2, android_dl, ios_dl, output_dir_name = job
            utm_name = extract_utm_name(android_dl, ios_dl)

            ts = datetime.now().isoformat(timespec="seconds")
            for email in emails:
                log_rows.append({
                    "timestamp": ts,
                    "email": email,
                    "utm_name": utm_name,
                    "clicked": "",
                    "title": copy1[:120],
                    "body": copy2[:120],
                    "_output_dir": output_dir_name,
                })

    # After all futures: print a compact tally of suppressed dry-run batches.
    if dry_run and suppressed_counts:
        print()
        for cname, count in sorted(suppressed_counts.items()):
            print(f"  [DRY-RUN] {cname}: +{count} additional batch(es) suppressed (same payload/result)")

    # Write one campaign log CSV per output directory under mode-specific folder.

    if log_rows and log_dir is not None:
        mode_dir = log_dir / ("dry_run" if dry_run else "live")
        mode_dir.mkdir(parents=True, exist_ok=True)
        by_dir: dict = defaultdict(list)
        for row in log_rows:
            by_dir[row["_output_dir"]].append(row)

        for dir_name, rows in by_dir.items():
            log_path = mode_dir / f"{dir_name}_campaign_log.csv"
            file_exists = log_path.exists()
            with open(log_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
                if not file_exists:
                    writer.writeheader()
                writer.writerows(rows)
            print(f"\n  [LOG] Dispatch log written: {log_path}")


# -- Entry point ---------------------------------------------------------------

def main() -> None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    env = dotenv_values(project_root / ".env")

    parser = argparse.ArgumentParser(
        description=(
            "Trigger CleverTap campaigns from priority exclusion outputs. "
            "Defaults to DRY-RUN mode -- no API calls are made without --live. "
            "Requires 03_prepare_campaign_content.py to have been run first."
        )
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Path to a specific slot output directory. If omitted, --date and --slot are used.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date in DDMMYYYY format. Default: today only.",
    )
    parser.add_argument(
        "--slot",
        choices=["morning", "evening", "both"],
        default="both",
        help="Slot(s) to process: morning, evening, or both (default: both).",
    )
    parser.add_argument(
        "--output-base",
        default="outputs",
        help="Base output directory when deriving paths from --date/--slot (default: outputs).",
    )
    parser.add_argument(
        "--cohorts",
        nargs="+",
        default=None,
        metavar="COHORT",
        help=(
            "One or more cohort names to process (default: all). "
            "Example: --cohorts \"N2B_All_Bangalore\" \"Clinic_KN_Mar26\""
        ),
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help=(
            "Enable real API calls to CleverTap. "
            "NEVER use this flag until the project is fully tested and approved."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=MAX_WORKERS_DEFAULT,
        help=f"Number of parallel API calls (default: {MAX_WORKERS_DEFAULT}).",
    )
    args = parser.parse_args()

    dry_run = not args.live

    # -- Print disclaimer ------------------------------------------------------
    if dry_run:
        print(DISCLAIMER_DRY_RUN)
    else:
        print(DISCLAIMER_LIVE.format(n=LIVE_COUNTDOWN_SECONDS))
        for remaining in range(LIVE_COUNTDOWN_SECONDS, 0, -1):
            print(f"  Starting in {remaining}...", end="\r", flush=True)
            time.sleep(1)
        print()

    # -- Load config -----------------------------------------------------------
    region = (env.get("CLEVERTAP_REGION") or "in1").strip()
    account_id = (env.get("CLEVERTAP_ACCOUNT_ID") or "").strip()
    passcode = (env.get("CLEVERTAP_PASSCODE") or "").strip()
    campaign_id_str = (env.get("CLEVERTAP_CAMPAIGN_ID") or "").strip()

    if not account_id or not passcode:
        print(
            "[ERROR] CLEVERTAP_ACCOUNT_ID and CLEVERTAP_PASSCODE must be set in .env"
        )
        sys.exit(1)

    if not campaign_id_str:
        print("[ERROR] CLEVERTAP_CAMPAIGN_ID must be set in .env")
        sys.exit(1)

    try:
        campaign_id = int(campaign_id_str)
    except ValueError:
        print(f"[ERROR] CLEVERTAP_CAMPAIGN_ID must be an integer, got: {campaign_id_str!r}")
        sys.exit(1)

    url = f"https://{region}.api.clevertap.com/2/send/externaltrigger.json"
    headers = {
        "X-CleverTap-Account-Id": account_id,
        "X-CleverTap-Passcode": passcode,
        "Content-Type": "application/json",
    }

    cohorts_filter: Optional[Set[str]] = set(args.cohorts) if args.cohorts else None

    # -- Resolve output directories to process --------------------------------
    if args.output_dir:
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = (project_root / output_dir).resolve()
        output_dirs = [output_dir]
    else:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if args.date:
            try:
                target = datetime.strptime(args.date, "%d%m%Y")
            except ValueError:
                print(f"[ERROR] --date must be in DDMMYYYY format, got: {args.date!r}")
                sys.exit(1)
            dates = [target]
        else:
            dates = [today]
        slots = ["morning", "evening"] if args.slot == "both" else [args.slot]
        base = (project_root / args.output_base).resolve()
        output_dirs = [base / f"{d.strftime('%d%m%Y')}_{s}" for d in dates for s in slots]

    log_dir = (project_root / "outputs" / "log").resolve()
    mode_log_dir = log_dir / ("dry_run" if dry_run else "live")

    print(f"Campaign ID  : {campaign_id}")
    print(f"API endpoint : {url}")
    print(f"Mode         : {'LIVE' if not dry_run else 'DRY-RUN'}")
    print(f"Max workers  : {args.max_workers}")
    if cohorts_filter:
        print(f"Cohort filter: {', '.join(sorted(cohorts_filter))}")
    print(f"Log dir      : {mode_log_dir}")
    print()

    # -- Build jobs ------------------------------------------------------------
    all_jobs = []
    for output_dir in output_dirs:
        if not output_dir.exists():
            print(f"[INFO] Skipping {output_dir.name} -- directory not found.")
            continue
        print(f"Building jobs from: {output_dir.name}")
        try:
            jobs = build_jobs(output_dir, cohorts_filter)
            all_jobs.extend(jobs)
        except (FileNotFoundError, ValueError) as exc:
            print(f"[WARNING] {output_dir.name}: {exc}")

    if not all_jobs:
        print("No jobs to dispatch. Exiting.")
        sys.exit(0)

    # -- Dispatch --------------------------------------------------------------
    run_parallel(all_jobs, headers, url, campaign_id, dry_run, args.max_workers, log_dir)

    print()
    if dry_run:
        print(
            "[OK] Dry-run complete. Review the payloads above, then run with --live "
            "when approved."
        )
    else:
        print("[OK] Live dispatch complete.")


if __name__ == "__main__":
    main()
