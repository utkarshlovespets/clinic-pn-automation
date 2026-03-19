"""Trigger CleverTap External Trigger campaigns from priority exclusion outputs.

DISCLAIMER: This script defaults to DRY-RUN mode.
    No actual CleverTap API calls are made unless --live is explicitly passed.
    The --live flag must NEVER be used until the project is fully tested
    and approved by the team.

Expects 03_prepare_campaign_content.py to have already been run so that
each priority CSV contains title and body columns.

Usage (dry-run, safe default):
    python 04_trigger_campaign.py --output-dir outputs/19032026_morning

Usage (live -- only when authorised):
    python 04_trigger_campaign.py --output-dir outputs/19032026_morning --live
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests
from dotenv import dotenv_values

# -- Constants -----------------------------------------------------------------

BATCH_SIZE = 1000
MAX_WORKERS_DEFAULT = 10
LIVE_COUNTDOWN_SECONDS = 5

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


# -- Job building --------------------------------------------------------------

def build_jobs(
    output_dir: Path,
) -> List[Tuple[str, int, List[str], str, str]]:
    """Build the full list of API call jobs from an output directory.

    Reads title and body directly from enriched priority CSVs (produced by
    03_prepare_campaign_content.py).  For each priority CSV:
      1. Loads Email, title, body.
      2. Groups users by identical (title, body) content.
      3. Chunks each group into batches of BATCH_SIZE.

    The cohort name for labels is derived from campaign_meta.csv.

    Returns:
        List of (cohort_name, priority, batch_emails, copy1, copy2) tuples
        in priority order.

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

    # Build priority -> cohort_name lookup (for labelling only).
    cohort_name_lookup: dict = {}
    for _, mrow in meta_df.iterrows():
        try:
            p = int(mrow["priority"])
        except (ValueError, TypeError):
            continue
        cohort_name_lookup[p] = str(mrow["cohort_name"]).strip()

    # Discover priority CSVs (NN_*.csv, sorted by NN prefix).
    csv_files = sorted(output_dir.glob("[0-9][0-9]_*.csv"))
    if not csv_files:
        print("  [WARNING] No priority CSV files found in output directory.")
        return []

    all_jobs: List[Tuple[str, int, List[str], str, str, str, str]] = []

    for csv_path in csv_files:
        try:
            priority = int(csv_path.stem.split("_")[0])
        except (ValueError, IndexError):
            print(f"  [WARNING] Skipping file with unexpected name: {csv_path.name}")
            continue

        cohort_name = cohort_name_lookup.get(priority, csv_path.stem)

        users_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

        if users_df.empty or "Email" not in users_df.columns:
            print(f"  [INFO] {csv_path.name}: 0 users -- skipped.")
            continue

        # Require title / body columns (populated by script 02b).
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
                    all_jobs.append((cohort_name, priority, batch, copy1, copy2, android_dl, ios_dl))

    return all_jobs


# -- API call ------------------------------------------------------------------

def send_request(
    job: Tuple[str, int, List[str], str, str, str, str],
    headers: dict,
    url: str,
    campaign_id: int,
    dry_run: bool,
) -> str:
    """Send (or mock-send) a single batch API call to CleverTap.

    Args:
        job:         (cohort_name, priority, batch_emails, copy1, copy2,
                      android_deeplink, ios_deeplink)
        headers:     CleverTap auth headers.
        url:         External Trigger API endpoint.
        campaign_id: Integer campaign ID from .env.
        dry_run:     If True, print payload instead of making HTTP request.

    Returns:
        Human-readable result string.
    """
    cohort_name, priority, emails, copy1, copy2, android_dl, ios_dl = job

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
        preview_emails = emails[:3]
        if len(emails) > 3:
            preview_emails = preview_emails + [f"+{len(emails) - 3} more"]
        preview = dict(payload)
        preview["to"] = {"email": preview_emails}
        print(f"\n  {label} DRY-RUN payload:")
        print("  " + json.dumps(preview, ensure_ascii=True, indent=4).replace("\n", "\n  "))
        return f"{label} -> [DRY-RUN] skipped"

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return f"{label} -> OK | {data.get('message', '')}"
            return f"{label} -> CleverTap error | {data.get('error', response.text)}"
        return f"{label} -> HTTP {response.status_code} | {response.text[:200]}"
    except requests.exceptions.RequestException as exc:
        return f"{label} -> Request error | {exc}"


# -- Parallel dispatcher -------------------------------------------------------

def run_parallel(
    jobs: List[Tuple],
    headers: dict,
    url: str,
    campaign_id: int,
    dry_run: bool,
    max_workers: int = MAX_WORKERS_DEFAULT,
) -> None:
    """Dispatch all jobs in parallel using ThreadPoolExecutor."""
    if not jobs:
        print("  No jobs to process.")
        return

    print(f"\nTotal API calls to make: {len(jobs)}")
    if dry_run:
        print("(DRY-RUN: payloads printed below, no HTTP requests)\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(send_request, job, headers, url, campaign_id, dry_run): job
            for job in jobs
        }
        for future in as_completed(futures):
            print(future.result())


# -- Entry point ---------------------------------------------------------------

def main() -> None:
    script_dir = Path(__file__).resolve().parent
    env = dotenv_values(script_dir / ".env")

    parser = argparse.ArgumentParser(
        description=(
            "Trigger CleverTap campaigns from priority exclusion outputs. "
            "Defaults to DRY-RUN mode -- no API calls are made without --live. "
            "Requires 03_prepare_campaign_content.py to have been run first."
        )
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Path to a slot output directory (e.g. outputs/19032026_morning).",
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

    # -- Resolve output directory ----------------------------------------------
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = (script_dir / output_dir).resolve()

    if not output_dir.exists():
        print(f"[ERROR] Output directory not found: {output_dir}")
        sys.exit(1)

    print(f"Output directory : {output_dir}")
    print(f"Campaign ID      : {campaign_id}")
    print(f"API endpoint     : {url}")
    print(f"Mode             : {'LIVE' if not dry_run else 'DRY-RUN'}")
    print(f"Max workers      : {args.max_workers}")
    print()

    # -- Build jobs ------------------------------------------------------------
    print("Building jobs from enriched CSVs...")
    try:
        jobs = build_jobs(output_dir)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    if not jobs:
        print("No jobs to dispatch. Exiting.")
        sys.exit(0)

    # -- Dispatch --------------------------------------------------------------
    run_parallel(jobs, headers, url, campaign_id, dry_run, args.max_workers)

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
