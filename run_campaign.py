"""
Orchestrate the full CleverTap automation pipeline.

⚠️ DISCLAIMER: This script defaults to DRY-RUN mode.
    No actual CleverTap API calls are made unless --live is explicitly passed.
    The --live flag must NEVER be used until the project is fully tested
    and approved by the team.

Stages:
    01 -- Fetch clinic_mastersheet from Google Sheets
    02 -- Generate priority exclusion CSVs
    03 -- Resolve title / body per user into enriched CSVs
    04 -- Trigger CleverTap campaigns (dry-run by default)

Usage:
python run_campaign.py --slot morning
python run_campaign.py --slot both --date 22032026
python run_campaign.py --slot morning --live   # AUTHORISED RUNS ONLY
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Optional
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import dotenv_values

LIVE_COUNTDOWN_SECONDS = 10

FULL_DISCLAIMER = """
================================================================================
  SUPERTAILS CLEVERTAP AUTOMATION PIPELINE
  [WARNING] Running in: {mode}

  Stages:
    1. Fetch clinic_mastersheet from Google Sheets (01_fetch_clinic_mastersheet.py)
    {fetch_cohorts_stage}
    2. Generate priority exclusion CSVs   (02_generate_priority_exclusions.py)
    3. Resolve title / body per user      (03_prepare_campaign_content.py)
    4. Trigger CleverTap campaigns        (04_trigger_campaign.py) [{mode}]

  The --live flag must NEVER be used until the project is fully tested
  and approved.  All dry-run output is safe to inspect freely.
================================================================================
"""
SLACK_DEFAULT_API_URL = "https://slack.com/api/chat.postMessage"


def print_header(live: bool) -> None:
    mode = "LIVE MODE " if live else "DRY-RUN MODE (default)"
    fetch_stage = "1. Fetch clinic_mastersheet from Google Sheets (01_fetch_clinic_mastersheet.py)"
    fetch_cohorts_stage = "1b. Fetch cohorts from Google Sheets      (00_fetch_cohorts.py) [LIVE ONLY]" if live else "1b. Fetch cohorts from Google Sheets      (00_fetch_cohorts.py) [SKIPPED]"
    print(FULL_DISCLAIMER.format(mode=mode, fetch_stage=fetch_stage, fetch_cohorts_stage=fetch_cohorts_stage))


def run_fetch(
    script_dir: Path,
    clinic_csv: str,
    cohort_map: str,
    exclusion_map: str,
    image_map: str,
) -> None:
    """Import and call script 01's main() to refresh clinic_mastersheet.csv."""
    sys.path.insert(0, str(script_dir))
    import importlib

    print("-" * 72)
    print("Stage 1 -- Fetching clinic mastersheet from Google Sheets...")
    print("-" * 72)
    try:
        mod = importlib.import_module("01_fetch_clinic_mastersheet".replace("-", "_"))
        # Script 01 uses sys.argv; temporarily patch it.
        import sys as _sys
        original_argv = _sys.argv[:]
        _sys.argv = [
            "01_fetch_clinic_mastersheet.py",
            "--output", clinic_csv,
            "--cohort-mapping-output", cohort_map,
            "--exclusion-mapping-output", exclusion_map,
            "--image-mapping-output", image_map,
        ]
        try:
            mod.main()
        finally:
            _sys.argv = original_argv
    except ImportError:
        # Fallback: run as subprocess if import fails (e.g. name starts with digit).
        import subprocess
        result = subprocess.run(
            [
                sys.executable,
                str(script_dir / "01_fetch_clinic_mastersheet.py"),
                "--output", clinic_csv,
                "--cohort-mapping-output", cohort_map,
                "--exclusion-mapping-output", exclusion_map,
                "--image-mapping-output", image_map,
            ],
            check=False,
        )
        if result.returncode != 0:
            print("[ERROR] Stage 1 failed. Aborting pipeline.")
            sys.exit(result.returncode)


def run_fetch_cohorts(script_dir: Path, live: bool) -> None:
    """Run script 00_fetch_cohorts.py in live mode only."""
    if not live:
        return

    print("-" * 72)
    print("Stage 1b -- Fetching cohorts (live mode only)...")
    print("-" * 72)
    import subprocess
    result = subprocess.run(
        [sys.executable, str(script_dir / "00_fetch_cohorts.py")],
        check=False,
    )
    if result.returncode != 0:
        print("[ERROR] Stage 1b failed. Aborting pipeline.")
        sys.exit(result.returncode)


def run_generate(
    script_dir: Path,
    clinic_csv: str,
    cohort_map: str,
    exclusion_map: str,
    output_dir: str,
    date_str: Optional[str] = None,
    slot: str = "both",
) -> None:
    """Import and call script 02's main() to generate priority exclusion CSVs."""
    print()
    print("-" * 72)
    print("Stage 2 -- Generating priority exclusion CSVs...")
    print("-" * 72)

    # Patch sys.argv so script 02's argparse picks up our arguments.
    import sys as _sys
    original_argv = _sys.argv[:]
    _sys.argv = [
        "02_generate_priority_exclusions.py",
        "--clinic-csv", clinic_csv,
        "--cohort-map", cohort_map,
        "--exclusion-map", exclusion_map,
        "--output-dir", output_dir,
        "--slot", slot,
    ]
    if date_str:
        _sys.argv.extend(["--date", date_str])
    try:
        sys.path.insert(0, str(script_dir))
        import importlib
        mod = importlib.import_module("02_generate_priority_exclusions")
        mod.main()
    finally:
        _sys.argv = original_argv


def run_prepare_content(
    script_dir: Path,
    slot_output_dir: Path,
    deeplink_map_path: Optional[Path] = None,
    image_map_path: Optional[Path] = None,
) -> None:
    """Load script 03 by file path and call prepare_content() for one slot directory.

    Uses importlib.util.spec_from_file_location because the filename starts
    with a digit, making it an invalid Python identifier for import_module().
    """
    import importlib.util

    print()
    print("-" * 72)
    print(f"Stage 3 -- Preparing campaign content: {slot_output_dir.name}")
    print("-" * 72)

    script_path = script_dir / "03_prepare_campaign_content.py"
    spec = importlib.util.spec_from_file_location("prepare_campaign_content", script_path)
    if spec is None or spec.loader is None:
        raise ImportError("Could not load module spec or loader")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.prepare_content(slot_output_dir, deeplink_map_path, image_map_path)


def run_trigger(
    script_dir: Path,
    slot_output_dir: str,
    live: bool,
    max_workers: int,
    cohorts: Optional[list] = None,
) -> None:
    """Import and call script 04's main() to trigger campaigns for one slot."""
    import sys as _sys
    original_argv = _sys.argv[:]
    live_flag = ["--live"] if live else []
    cohort_flags = (["--cohorts"] + cohorts) if cohorts else []
    _sys.argv = [
        "04_trigger_campaign.py",
        "--output-dir", slot_output_dir,
        "--max-workers", str(max_workers),
    ] + live_flag + cohort_flags
    try:
        sys.path.insert(0, str(script_dir))
        import importlib
        mod = importlib.import_module("04_trigger_campaign")
        mod.main()
    finally:
        _sys.argv = original_argv


def parse_date(date_str: str) -> datetime:
    """Parse DDMMYYYY date string."""
    try:
        return datetime.strptime(date_str, "%d%m%Y")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Date must be in DDMMYYYY format, got: {date_str!r}"
        )


def is_default_or_live_only_run(raw_argv: list[str]) -> bool:
    """Return True only for no flags or only '--live'."""
    return len(raw_argv) == 0 or raw_argv == ["--live"]


def resolve_ist_now() -> datetime:
    """Return current time in IST regardless of machine locale."""
    return datetime.now(ZoneInfo("Asia/Kolkata"))


def infer_slot_from_ist_now(now_ist: datetime) -> str:
    """Before 14:00 IST = morning, 14:00 IST and later = evening."""
    return "morning" if now_ist.hour < 14 else "evening"


def has_slot_data_in_mastersheet(
    clinic_csv_path: Path,
    run_date: datetime,
    slot: str,
) -> bool:
    """Check if mastersheet has at least one usable row for date + slot."""
    if not clinic_csv_path.exists():
        raise FileNotFoundError(f"clinic_mastersheet not found: {clinic_csv_path}")

    df = pd.read_csv(clinic_csv_path, dtype=str, keep_default_na=False)
    required = {"Date", "Slot", "Cohort Name", "Title", "Content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            "clinic_mastersheet is missing required columns for auto-slot check: "
            f"{sorted(missing)}"
        )

    df["_date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    df["_slot"] = df["Slot"].fillna("").str.strip().str.lower()
    target_date = pd.Timestamp(run_date.date())

    usable = (
        df["_date"].eq(target_date)
        & df["_slot"].eq(slot)
        & df["Cohort Name"].str.strip().ne("")
        & ~(df["Title"].str.strip().eq("") & df["Content"].str.strip().eq(""))
    )
    return bool(usable.any())


def validate_title_body_for_run_date(
    clinic_csv_path: Path,
    run_date: datetime,
    run_slots: list[str],
) -> None:
    """Abort if any row for run_date + selected slot(s) has missing Title or Content."""
    if not clinic_csv_path.exists():
        raise FileNotFoundError(f"clinic_mastersheet not found: {clinic_csv_path}")

    df = pd.read_csv(clinic_csv_path, dtype=str, keep_default_na=False)
    required = {"Date", "Title", "Content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            "clinic_mastersheet is missing required columns for title/body validation: "
            f"{sorted(missing)}"
        )

    df["_date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    target_date = pd.Timestamp(run_date.date())
    normalized_slots = {str(slot).strip().lower() for slot in run_slots}
    valid_slots = {"morning", "evening"}
    selected_slots = normalized_slots & valid_slots

    rows_for_date = df[df["_date"].eq(target_date)].copy()
    if selected_slots:
        if "Slot" not in rows_for_date.columns:
            rows_for_date["Slot"] = ""
        rows_for_date["_slot"] = rows_for_date["Slot"].fillna("").str.strip().str.lower()
        rows_for_date = rows_for_date[rows_for_date["_slot"].isin(selected_slots)].copy()

    if rows_for_date.empty:
        return

    rows_for_date["_title_missing"] = rows_for_date["Title"].fillna("").str.strip().eq("")
    rows_for_date["_content_missing"] = rows_for_date["Content"].fillna("").str.strip().eq("")
    invalid = rows_for_date[rows_for_date["_title_missing"] | rows_for_date["_content_missing"]]

    if invalid.empty:
        return

    print(
            f"[ERROR] Found {len(invalid)} row(s) for {run_date.strftime('%d/%m/%Y')} "
            f"for slot(s) {', '.join(sorted(selected_slots)) or 'all'} "
            "with missing title/body. Aborting pipeline."
    )
    for idx, row in invalid.head(10).iterrows():
        date_val = row.get("Date", "")
        slot_val = row.get("Slot", "")
        cohort_val = row.get("Cohort Name", "")
        title_missing = bool(row["_title_missing"])
        content_missing = bool(row["_content_missing"])
        if title_missing and content_missing:
            missing_desc = "Title and Content missing"
        elif title_missing:
            missing_desc = "Title missing"
        else:
            missing_desc = "Content missing"
        print(
            f"  - Row {idx + 2}: Date={date_val}, Slot={slot_val}, "
            f"Cohort={cohort_val}, Issue={missing_desc}"
        )
    if len(invalid) > 10:
        print(f"  ... and {len(invalid) - 10} more invalid row(s).")
    sys.exit(1)


def run_live_countdown(seconds: int) -> None:
    """Show a visible line-by-line countdown before live triggering."""
    print()
    print(f"[LIVE] Final safety countdown: starting stage 4 in {seconds} seconds")
    for remaining in range(seconds, 0, -1):
        print(f"  Starting in {remaining}...")
        time.sleep(1)
    print("  Proceeding now.")


def send_pipeline_slack_message(
    project_root: Path,
    status: str,
    date_text: str,
    slot: str,
    live: bool,
    error_message: Optional[str] = None,
) -> None:
    """Send one Slack notification for pipeline completion status."""
    env = dotenv_values(project_root / ".env")
    api_url = str(env.get("DEFAULT_SLACK_API_URL") or SLACK_DEFAULT_API_URL).strip()
    channel = str(env.get("DEFAULT_SLACK_CHANNEL") or "").strip()
    token = str(env.get("SLACK_API_TOKEN") or "").strip()

    if not channel or not token:
        print(
            "[WARNING] Slack notification skipped: "
            "missing DEFAULT_SLACK_CHANNEL or SLACK_API_TOKEN in .env."
        )
        return

    status_tag = "SUCCESS" if status.upper() == "SUCCESS" else "FAILED"
    slot_text = slot.capitalize()
    mode_text = "Live" if live else "Dry-Run"
    text = f"[{status_tag}] | Clinic PN Campaign | {date_text} | {slot_text} | {mode_text}"
    if error_message and status_tag == "FAILED":
        compact_error = " ".join(str(error_message).split())[:180]
        text = f"{text} | {compact_error}"

    try:
        response = requests.post(
            api_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={
                "channel": channel,
                "text": text,
            },
            timeout=20,
        )
    except requests.RequestException as exc:
        print(f"[WARNING] Slack notification failed to send: {exc}")
        return

    ok = False
    error = ""
    try:
        payload = response.json()
        ok = bool(payload.get("ok"))
        error = str(payload.get("error") or "")
    except ValueError:
        payload = {}

    if response.status_code >= 400 or not ok:
        detail = error or f"HTTP {response.status_code}"
        print(f"[WARNING] Slack notification failed: {detail}")


def main() -> None:
    project_root = Path(__file__).resolve().parent
    campaign_dir = project_root / "campaign_scripts"
    raw_argv = sys.argv[1:]

    if not campaign_dir.exists():
        raise FileNotFoundError(f"Campaign scripts directory not found: {campaign_dir}")

    parser = argparse.ArgumentParser(
        description=(
            "Orchestrate the full CleverTap automation pipeline. "
            "Defaults to DRY-RUN mode -- no API calls without --live."
        )
    )
    parser.add_argument(
        "--date",
        type=parse_date,
        default=None,
        help="Target date in DDMMYYYY format (default: today). Overrides auto-detect.",
    )
    parser.add_argument(
        "--slot",
        choices=["morning", "evening", "both"],
        default="both",
        help="Which slot(s) to process (default: both).",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help=(
            "Enable real API calls in stage 3. "
            "NEVER use this flag until the project is fully tested and approved."
        ),
    )
    parser.add_argument(
        "--clinic-csv",
        default="data/clinic_mastersheet.csv",
        help="Path to clinic_mastersheet.csv (default: data/clinic_mastersheet.csv).",
    )
    parser.add_argument(
        "--cohort-map",
        default="data/cohort_mapping.csv",
        help=(
            "Path to Cohort_Mapping export with cohort_code, campaign_id, "
            "cohort_dataset, default exclusions, and deeplink templates "
            "(default: data/cohort_mapping.csv)."
        ),
    )
    parser.add_argument(
        "--exclusion-map",
        default="data/exclusion_mapping.csv",
        help=(
            "Path to Exclusion_Mapping export with 'Exclusion Name' and 'Dataset' "
            "columns (default: data/exclusion_mapping.csv)."
        ),
    )
    parser.add_argument(
        "--image-map",
        default="data/image_mapping.csv",
        help=(
            "Path to Image_Mapping export with image_name and image_url "
            "columns (default: data/image_mapping.csv)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory (default: outputs).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Parallel API call threads for stage 3 (default: 10).",
    )
    parser.add_argument(
        "--deeplink-map",
        default=None,
        help=(
            "Optional override for the Cohort_Mapping export used by stage 3 "
            "(columns: cohort_code, cohort_name, campaign_id, android_base_url, ios_base_url). "
            "Stage 3 appends campaign_id, android_deeplink, and ios_deeplink columns "
            "when this file exists. "
            "Defaults to --cohort-map."
        ),
    )
    parser.add_argument(
        "--cohorts",
        nargs="+",
        default=None,
        metavar="COHORT",
        help=(
            "One or more cohort names to trigger in stage 4 (default: all). "
            "Example: --cohorts \"N2B_All_Bangalore\" \"Clinic_KN_Mar26\""
        ),
    )
    args = parser.parse_args()

    auto_slot_mode = is_default_or_live_only_run(raw_argv)
    inferred_slot: Optional[str] = None
    if auto_slot_mode:
        ist_now = resolve_ist_now()
        inferred_slot = infer_slot_from_ist_now(ist_now)
        print(
            "[INFO] Auto-slot mode active (no flags or only --live): "
            f"{ist_now.strftime('%d/%m/%Y %H:%M:%S')} IST -> {inferred_slot}."
        )
        # In auto-slot mode, today's date must be computed in IST.
        run_date = ist_now
        args.slot = inferred_slot
    else:
        run_date = args.date if args.date else datetime.now()
    date_str = run_date.strftime("%d%m%Y")
    display_date = run_date.strftime("%d/%m/%Y")
    using_default_today = args.date is None
    raw_output_base = Path(args.output_dir)
    output_base = (
        raw_output_base
        if raw_output_base.is_absolute()
        else (project_root / raw_output_base).resolve()
    )
    output_base.mkdir(parents=True, exist_ok=True)

    status = "SUCCESS"
    failure_message: Optional[str] = None
    pipeline_skipped = False
    live_trigger_stage_ran = False

    try:
        print_header(live=args.live)

        # -- Stage 1: Fetch mastersheet ----------------------------------------
        run_fetch(
            campaign_dir,
            clinic_csv=args.clinic_csv,
            cohort_map=args.cohort_map,
            exclusion_map=args.exclusion_map,
            image_map=args.image_map,
        )

        # Auto-slot guard: only proceed if today's inferred IST slot exists in mastersheet.
        raw_clinic_path = Path(args.clinic_csv)
        clinic_path = (
            raw_clinic_path
            if raw_clinic_path.is_absolute()
            else (project_root / raw_clinic_path).resolve()
        )

        # Global guard: for the running date, every row must have both title and body.
        selected_slots = ["morning", "evening"] if args.slot == "both" else [args.slot]
        validate_title_body_for_run_date(clinic_path, run_date, selected_slots)

        if auto_slot_mode and inferred_slot is not None:
            if not has_slot_data_in_mastersheet(clinic_path, run_date, inferred_slot):
                print(
                    f"[INFO] Auto-slot check: no mastersheet data for {display_date} "
                    f"({inferred_slot} slot). Exiting without running pipeline stages."
                )
                pipeline_skipped = True
                return
            print(
                f"[INFO] Auto-slot check: mastersheet has data for {display_date} "
                f"({inferred_slot} slot). Continuing."
            )

        # -- Stage 1b: Fetch cohorts (live mode only) -------------------------
        run_fetch_cohorts(campaign_dir, live=args.live)

        # -- Stage 2: Generate priority exclusion CSVs --------------------------
        run_generate(
            campaign_dir,
            clinic_csv=args.clinic_csv,
            cohort_map=args.cohort_map,
            exclusion_map=args.exclusion_map,
            output_dir=str(output_base),
            date_str=date_str,
            slot=args.slot,
        )

        # -- Stages 3 + 4: Prepare content then trigger campaigns per slot ---------
        slots = ["morning", "evening"] if args.slot == "both" else [args.slot]
        processed_slots = 0

        for slot in slots:
            slot_dir_full = output_base / f"{date_str}_{slot}"

            if not slot_dir_full.exists():
                if using_default_today:
                    print(
                        f"  [INFO] No data found in mastersheet for current date {display_date} "
                        f"({slot} slot) -- skipping."
                    )
                else:
                    print(
                        f"  [INFO] No data found in mastersheet for requested date {display_date} "
                        f"({slot} slot) -- skipping."
                    )
                continue

            # -- Stage 3: Resolve title / body per user + deeplinks ----------
            raw_dl = Path(args.deeplink_map or args.cohort_map)
            deeplink_map_path = raw_dl if raw_dl.is_absolute() else (project_root / raw_dl).resolve()
            if not deeplink_map_path.exists():
                print(f"  [WARNING] Cohort mapping not found: {deeplink_map_path} -- deeplink columns will be skipped.")
                deeplink_map_path = None

            raw_image = Path(args.image_map)
            image_map_path = raw_image if raw_image.is_absolute() else (project_root / raw_image).resolve()
            if not image_map_path.exists():
                image_map_path = None

            run_prepare_content(campaign_dir, slot_dir_full, deeplink_map_path, image_map_path)

            # -- Stage 4: Trigger campaigns ------------------------------------
            print()
            print("-" * 72)
            print(f"Stage 4 -- Triggering campaigns: {date_str} | {slot}")
            print("-" * 72)

            if args.live:
                live_trigger_stage_ran = True
                run_live_countdown(LIVE_COUNTDOWN_SECONDS)

            run_trigger(
                campaign_dir,
                slot_output_dir=str(slot_dir_full),
                live=args.live,
                max_workers=args.max_workers,
                cohorts=args.cohorts,
            )
            processed_slots += 1

        if processed_slots == 0:
            pipeline_skipped = True

        print()
        print("Pipeline complete.")
        if not args.live:
            print(
                "[INFO]  This was a dry-run. No campaigns were triggered. "
                "Pass --live when you are ready to send."
            )
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        if code != 0:
            status = "FAILED"
            failure_message = f"Exited with code {code}"
        raise
    except Exception as exc:
        status = "FAILED"
        failure_message = str(exc)
        raise
    finally:
        should_send_slack = not (pipeline_skipped and not live_trigger_stage_ran)
        if should_send_slack:
            send_pipeline_slack_message(
                project_root=project_root,
                status=status,
                date_text=display_date,
                slot=args.slot,
                live=args.live,
                error_message=failure_message,
            )
        else:
            print(
                "[INFO] Slack notification skipped: pipeline had no runnable slot "
                "and no live trigger API stage was executed."
            )


if __name__ == "__main__":
    main()
