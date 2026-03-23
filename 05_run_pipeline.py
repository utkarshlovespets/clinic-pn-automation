"""Orchestrate the full CleverTap automation pipeline.

⚠️  DISCLAIMER: This script defaults to DRY-RUN mode.
    No actual CleverTap API calls are made unless --live is explicitly passed.
    The --live flag must NEVER be used until the project is fully tested
    and approved by the team.

Stages:
    [Optional] 01 -- Fetch clinic_mastersheet from Google Sheets
              02 -- Generate priority exclusion CSVs
              03 -- Resolve title / body per user into enriched CSVs
              04 -- Trigger CleverTap campaigns (dry-run by default)

Usage:
    python 05_run_pipeline.py --slot morning
    python 05_run_pipeline.py --fetch-mastersheet --slot both --date 22032026
    python 05_run_pipeline.py --slot morning --live   # AUTHORISED RUNS ONLY
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

FULL_DISCLAIMER = """
================================================================================
  SUPERTAILS CLEVERTAP AUTOMATION PIPELINE
  [WARNING] Running in: {mode}

  Stages:
    {fetch_stage}
    2. Generate priority exclusion CSVs   (02_generate_priority_exclusions.py)
    3. Resolve title / body per user      (03_prepare_campaign_content.py)
    4. Trigger CleverTap campaigns        (04_trigger_campaign.py) [{mode}]

  The --live flag must NEVER be used until the project is fully tested
  and approved.  All dry-run output is safe to inspect freely.
================================================================================
"""


def print_header(live: bool, fetch: bool) -> None:
    mode = "LIVE MODE " if live else "DRY-RUN MODE (default)"
    fetch_stage = (
        "1. Fetch clinic_mastersheet from Google Sheets  (01_fetch_clinic_mastersheet.py)"
        if fetch
        else "1. [SKIPPED] Fetch mastersheet  (pass --fetch-mastersheet to enable)"
    )
    print(FULL_DISCLAIMER.format(mode=mode, fetch_stage=fetch_stage))


def run_fetch(script_dir: Path) -> None:
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
        _sys.argv = ["01_fetch_clinic_mastersheet.py"]
        try:
            mod.main()
        finally:
            _sys.argv = original_argv
    except ImportError:
        # Fallback: run as subprocess if import fails (e.g. name starts with digit).
        import subprocess
        result = subprocess.run(
            [sys.executable, str(script_dir / "01_fetch_clinic_mastersheet.py")],
            check=False,
        )
        if result.returncode != 0:
            print("[ERROR] Stage 1 failed. Aborting pipeline.")
            sys.exit(result.returncode)


def run_generate(
    script_dir: Path,
    clinic_csv: str,
    user_base_csv: str,
    output_dir: str,
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
        "--user-base-csv", user_base_csv,
        "--output-dir", output_dir,
    ]
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
    deeplink_map_path: Path = None,
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
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.prepare_content(slot_output_dir, deeplink_map_path)


def run_trigger(
    script_dir: Path,
    slot_output_dir: str,
    live: bool,
    max_workers: int,
) -> None:
    """Import and call script 03's main() to trigger campaigns for one slot."""
    import sys as _sys
    original_argv = _sys.argv[:]
    live_flag = ["--live"] if live else []
    _sys.argv = [
        "04_trigger_campaign.py",
        "--output-dir", slot_output_dir,
        "--max-workers", str(max_workers),
    ] + live_flag
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


def main() -> None:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description=(
            "Orchestrate the full CleverTap automation pipeline. "
            "Defaults to DRY-RUN mode -- no API calls without --live."
        )
    )
    parser.add_argument(
        "--fetch-mastersheet",
        action="store_true",
        default=False,
        help="Run stage 1: fetch clinic_mastersheet from Google Sheets before processing.",
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
        "--user-base-csv",
        default="data/clinic_user_base_mastersheet.csv",
        help=(
            "Path to clinic_user_base_mastersheet.csv "
            "(default: data/clinic_user_base_mastersheet.csv)."
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
        default="data/deeplink_map.csv",
        help=(
            "Path to deeplink_map.csv (columns: Cohort Name, android_base_url, ios_base_url). "
            "Stage 3 appends android_deeplink and ios_deeplink columns when this file exists. "
            "(default: data/deeplink_map.csv)"
        ),
    )
    args = parser.parse_args()

    print_header(live=args.live, fetch=args.fetch_mastersheet)

    # -- Stage 1: Fetch mastersheet (optional) -----------------------------
    if args.fetch_mastersheet:
        run_fetch(script_dir)

    # -- Stage 2: Generate priority exclusion CSVs --------------------------
    run_generate(
        script_dir,
        clinic_csv=args.clinic_csv,
        user_base_csv=args.user_base_csv,
        output_dir=args.output_dir,
    )

    # -- Stages 3 + 4: Prepare content then trigger campaigns per slot ---------
    run_date = args.date if args.date else datetime.now()
    date_str = run_date.strftime("%d%m%Y")

    slots = ["morning", "evening"] if args.slot == "both" else [args.slot]

    for slot in slots:
        slot_dir = str(Path(args.output_dir) / f"{date_str}_{slot}")
        slot_dir_full = (script_dir / slot_dir).resolve()

        if not slot_dir_full.exists():
            print(
                f"  [WARNING] Output directory not found: {slot_dir_full}\n"
                "  Stage 2 may not have found data for this date/slot -- skipping."
            )
            continue

        # -- Stage 3: Resolve title / body per user + deeplinks ----------
        raw_dl = Path(args.deeplink_map)
        deeplink_map_path = raw_dl if raw_dl.is_absolute() else (script_dir / raw_dl).resolve()
        if not deeplink_map_path.exists():
            print(f"  [WARNING] Deeplink map not found: {deeplink_map_path} -- deeplink columns will be skipped.")
            deeplink_map_path = None
        run_prepare_content(script_dir, slot_dir_full, deeplink_map_path)

        # -- Stage 4: Trigger campaigns ------------------------------------
        print()
        print("-" * 72)
        print(f"Stage 4 -- Triggering campaigns: {date_str} | {slot}")
        print("-" * 72)

        run_trigger(
            script_dir,
            slot_output_dir=str(slot_dir_full),
            live=args.live,
            max_workers=args.max_workers,
        )

    print()
    print("Pipeline complete.")
    if not args.live:
        print(
            "[INFO]  This was a dry-run. No campaigns were triggered. "
            "Pass --live when you are ready to send."
        )


if __name__ == "__main__":
    main()
