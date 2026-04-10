"""Generate per-priority exclusion CSVs from clinic_mastersheet and cohort CSV files.

⚠️  DISCLAIMER: This script only generates intermediate data files.
    No CleverTap API calls are made here. Campaign triggering requires
    04_trigger_campaign.py run explicitly with the --live flag.

Usage:
    python 02_generate_priority_exclusions.py
    python 02_generate_priority_exclusions.py --clinic-csv data/clinic_mastersheet.csv \\
        --cohort-map data/deeplink_map.csv --output-dir outputs
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import parse_qs, urlparse

import pandas as pd

# Allow importing shared modules from project root when running from campaign_scripts/.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils import normalize_cohort, sanitize_filename

# -- Feature flags ------------------------------------------------------------
# Exclusion-column-based filtering: exclude members of named cohorts from a
# campaign row, regardless of priority order.
ENABLE_EXCLUSION_COL: bool = True


# -- Exclusion column helper --------------------------------------------------

def parse_exclusion_col(exclusion_str: str) -> List[str]:
    """Parse the Exclusion column value into a list of normalized cohort keys.

    The Exclusion column contains comma-separated cohort names whose members
    should be excluded from the current cohort, regardless of priority order.

    Args:
        exclusion_str: Raw cell value from the Exclusion column.

    Returns:
        List of normalized cohort keys (empty list if blank / NaN).
    """
    if not exclusion_str or pd.isna(exclusion_str):
        return []
    parts = str(exclusion_str).split(",")
    return [normalize_cohort(p) for p in parts if p.strip()]


def build_deeplink(url_template: str, date_formatted: str, priority_token: str) -> str:
    """Replace {date} and {priority} placeholders in URL template."""
    if not url_template:
        return ""
    return url_template.replace("{date}", date_formatted).replace("{priority}", priority_token)


def get_deeplink_priority_token(run_slot: str, priority: int) -> str:
    """Return slot-tagged priority token for deeplink UTM tracking."""
    suffix = "E" if run_slot.strip().lower() == "evening" else "M"
    return f"{priority}{suffix}"


def extract_utm_campaign(url: str) -> str:
    """Extract utm_campaign query parameter from a URL."""
    if not url:
        return ""
    parsed = urlparse(url)
    utm = parse_qs(parsed.query).get("utm_campaign", [""])
    return utm[0]


# -- Data loaders -------------------------------------------------------------

def load_cohort_index_from_map(
    cohort_map_path: Path,
    cohorts_dir: Path,
) -> Tuple[Dict[str, List[Tuple[str, str, str]]], int, Dict[str, str]]:
    """Build cohort index by loading individual cohort CSV files.

    Reads cohort_map_path (deeplink_map.csv) to discover the mapping:
        Cohort Name -> cohort_dataset filename in cohorts_dir

    For each cohort with a non-blank cohort_dataset, loads the CSV from
    cohorts_dir and adds its users to the index.

    Expected cohort CSV columns (case-insensitive): email, first_name, pet_name

    Returns:
        {normalized_cohort_key: [(email, first_name, pet_name), ...]}
    """
    if not cohort_map_path.exists():
        raise FileNotFoundError(f"Cohort map not found: {cohort_map_path}")

    map_df = pd.read_csv(cohort_map_path, dtype=str, keep_default_na=False)

    required = {"Cohort Name", "cohort_dataset"}
    missing = required - set(map_df.columns)
    if missing:
        raise ValueError(
            f"Cohort map is missing columns: {sorted(missing)}. "
            "Add a 'cohort_dataset' column to deeplink_map.csv."
        )

    index: Dict[str, List[Tuple[str, str, str]]] = {}
    deeplink_url_map: Dict[str, str] = {}
    loaded = 0

    for _, row in map_df.iterrows():
        cohort_name = str(row["Cohort Name"]).strip()
        dataset_file = str(row["cohort_dataset"]).strip()

        if not cohort_name or not dataset_file:
            if cohort_name:
                print(
                    f"  [WARNING] '{cohort_name}' has no cohort_dataset entry "
                    "-- will be treated as empty cohort."
                )
            continue

        cohort_key = normalize_cohort(cohort_name)
        csv_path = cohorts_dir / dataset_file
        deeplink_url_map[cohort_key] = str(row.get("android_base_url", "")).strip()

        if not csv_path.exists():
            print(
                f"  [WARNING] Cohort file not found for '{cohort_name}': {csv_path} "
                "-- skipping."
            )
            continue

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

        # Normalize column names: lowercase and spaces → underscores.
        # Handles both "first_name" and "First Name" style headers.
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        if "email" not in df.columns:
            print(
                f"  [WARNING] '{dataset_file}' has no 'email' column -- skipping."
            )
            continue

        for col in ("first_name", "pet_name"):
            if col not in df.columns:
                df[col] = ""

        entries = []
        for _, user_row in df.iterrows():
            email = str(user_row["email"]).strip().lower()
            if not email or "@" not in email or email == "#error!":
                continue
            # Take only the first word of first_name so "Sneha Das" → "Sneha".
            raw_first = str(user_row.get("first_name", "")).strip()
            first_name = raw_first.split()[0] if raw_first else ""
            entries.append((
                email,
                first_name,
                str(user_row.get("pet_name", "")).strip(),
            ))

        index[cohort_key] = entries
        loaded += 1

    return index, loaded, deeplink_url_map


def load_clinic_mastersheet(path: Path) -> pd.DataFrame:
    """Load and validate clinic_mastersheet.csv.

    Expected columns: Date, Day, Slot, Cohort Name, Exclusion, Title, Content
    Adds '_date' (Timestamp) and '_slot' (lowercase string) columns.
    Rows with unparseable dates, blank cohort names, or both Title AND Content
    blank are filtered out.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    required = {"Date", "Cohort Name", "Title", "Content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"clinic_mastersheet is missing columns: {sorted(missing)}"
        )

    if "Slot" not in df.columns:
        df["Slot"] = ""
    if "Exclusion" not in df.columns:
        df["Exclusion"] = ""

    df["_date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    df["_slot"] = df["Slot"].fillna("").str.strip().str.lower()

    usable = (
        df["_date"].notna()
        & df["Cohort Name"].str.strip().ne("")
        & ~(df["Title"].str.strip().eq("") & df["Content"].str.strip().eq(""))
    )
    df = df[usable].copy().reset_index(drop=True)

    return df


# -- Core build logic ----------------------------------------------------------

def build_priority_files(
    clinic_df: pd.DataFrame,
    cohort_index: Dict[str, List[Tuple[str, str, str]]],
    deeplink_url_map: Dict[str, str],
    run_date: pd.Timestamp,
    run_slot: str,
    output_dir: Path,
) -> bool:
    """Generate exclusion CSVs and metadata for one date + slot combination.

    Priority ordering is determined by row order in clinic_df for the given
    date and slot.  Users targeted by a higher-priority cohort are excluded
    from all lower-priority cohorts.

    If ENABLE_EXCLUSION_COL is True, the Exclusion column is also applied:
    members of named cohorts are excluded from that campaign row regardless
    of priority.

    Returns True if at least one row was found and files were written.
    """
    run_df = clinic_df[
        clinic_df["_date"].eq(run_date) & clinic_df["_slot"].eq(run_slot)
    ].copy().reset_index(drop=True)

    if run_df.empty:
        return False

    # Deduplicate: if the same cohort appears multiple times for the same
    # date+slot, keep only the first occurrence (first row = highest priority).
    run_df = run_df.drop_duplicates(subset=["Cohort Name"], keep="first").reset_index(
        drop=True
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    targeted_emails: set = set()
    summary_rows = []
    campaign_meta_rows = []

    for priority, (_, row) in enumerate(run_df.iterrows(), start=1):
        cohort_name = str(row["Cohort Name"]).strip()
        cohort_key = normalize_cohort(cohort_name)

        candidate_tuples = cohort_index.get(cohort_key, [])
        input_candidates = len({t[0] for t in candidate_tuples})  # unique emails

        # -- Exclusion column -----------------------------------------------
        exclusion_cohort_names: List[str] = []
        if ENABLE_EXCLUSION_COL:
            raw_exclusion = str(row.get("Exclusion", "")).strip()
            exclusion_keys = parse_exclusion_col(raw_exclusion)
            exclusion_emails: set = set()
            for ex_key in exclusion_keys:
                for t in cohort_index.get(ex_key, []):
                    exclusion_emails.add(t[0])
            # Preserve the original names from the Exclusion cell for reporting.
            if raw_exclusion:
                exclusion_cohort_names = [
                    p.strip() for p in raw_exclusion.split(",") if p.strip()
                ]
        else:
            exclusion_emails = set()
        # -------------------------------------------------------------------

        candidate_emails = {t[0] for t in candidate_tuples}

        # Apply priority exclusion (and optional column exclusion).
        final_tuples = [
            t
            for t in candidate_tuples
            if t[0] not in targeted_emails and t[0] not in exclusion_emails
        ]
        # Count exclusions without double-counting users excluded by both filters.
        # Users in both sets are attributed to priority exclusion.
        col_only_excl = (candidate_emails & exclusion_emails) - targeted_emails
        priority_excl = candidate_emails & targeted_emails

        excluded_by_exclusion_col = len(col_only_excl)
        excluded_by_priority = len(priority_excl)

        # Update targeted set (by email, not by tuple -- one email = one slot).
        targeted_emails.update(t[0] for t in final_tuples)

        # Write per-priority CSV: Email, First Name, Pet Name
        # Always include the header even when final_tuples is empty.
        out_name = f"{priority:02d}_{sanitize_filename(cohort_name)}.csv"
        pd.DataFrame(
            [{"Email": t[0], "First Name": t[1], "Pet Name": t[2]} for t in final_tuples],
            columns=["Email", "First Name", "Pet Name"],
        ).to_csv(output_dir / out_name, index=False)

        summary_rows.append(
            {
                "priority": priority,
                "cohort_name": cohort_name,
                "title_template": str(row.get("Title", "")).strip(),
                "content_template": str(row.get("Content", "")).strip(),
                "input_candidates": input_candidates,
                "excluded_by_priority": excluded_by_priority,
                "excluded_by_exclusion_col": excluded_by_exclusion_col,
                "exclusion_cohorts": "; ".join(exclusion_cohort_names),
                "final_count": len(final_tuples),
                "output_file": out_name,
            }
        )
        campaign_meta_rows.append(
            {
                "priority": priority,
                "cohort_name": cohort_name,
                "title_template": str(row.get("Title", "")).strip(),
                "content_template": str(row.get("Content", "")).strip(),
                "cohort_size": input_candidates,
                "excluded_by_priority": excluded_by_priority,
                "excluded_by_exclusion_col": excluded_by_exclusion_col,
                "final_count": len(final_tuples),
            }
        )

    date_formatted = run_date.strftime("%d%B")
    date_value = run_date.strftime("%d/%m/%Y")
    slot_value = "Evening" if run_slot.strip().lower() == "evening" else "Morning"
    log_summary_rows = []
    for row in summary_rows:
        priority = int(row["priority"])
        cohort_key = normalize_cohort(str(row["cohort_name"]))
        deeplink_tpl = deeplink_url_map.get(cohort_key, "")
        deeplink_priority = get_deeplink_priority_token(run_slot, priority)
        resolved_url = build_deeplink(deeplink_tpl, date_formatted, deeplink_priority)
        log_summary_rows.append(
            {
                "date": date_value,
                "slot": slot_value,
                "priority": priority,
                "utm_campaign": extract_utm_campaign(resolved_url),
                "title_template": str(row.get("title_template", "")).strip(),
                "content_template": str(row.get("content_template", "")).strip(),
                "final_count": int(row["final_count"]),
            }
        )

    summary_log_dir = output_dir.parent / "log" / "summary"
    summary_log_dir.mkdir(parents=True, exist_ok=True)
    log_summary_path = summary_log_dir / f"{run_date.strftime('%d%m%Y')}_{run_slot}.csv"
    pd.DataFrame(
        log_summary_rows,
        columns=[
            "date",
            "slot",
            "priority",
            "utm_campaign",
            "title_template",
            "content_template",
            "final_count",
        ],
    ).to_csv(log_summary_path, index=False)

    pd.DataFrame(campaign_meta_rows).to_csv(
        output_dir / "campaign_meta.csv", index=False
    )

    date_str = run_date.strftime("%d/%m/%Y")
    total_final = sum(r["final_count"] for r in summary_rows)
    print(
        f"  [{date_str} | {run_slot}] "
        f"{len(summary_rows)} cohort(s) -> {total_final} user rows -> {output_dir}"
    )
    return True


# -- Entry point ---------------------------------------------------------------

def main() -> None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    parser = argparse.ArgumentParser(
        description=(
            "Generate per-priority exclusion CSVs from clinic_mastersheet "
            "and individual cohort CSV files for today's date, "
            "both morning and evening slots."
        )
    )
    parser.add_argument(
        "--clinic-csv",
        default="data/clinic_mastersheet.csv",
        help="Path to clinic_mastersheet.csv (default: data/clinic_mastersheet.csv)",
    )
    parser.add_argument(
        "--cohort-map",
        default="data/deeplink_map.csv",
        help=(
            "Path to deeplink_map.csv with a 'cohort_dataset' column mapping each "
            "cohort to its CSV file in data/cohorts/ "
            "(default: data/deeplink_map.csv)"
        ),
    )
    parser.add_argument(
        "--cohorts-dir",
        default="data/cohorts",
        help="Directory containing individual cohort CSV files (default: data/cohorts)",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory (default: outputs)",
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
    args = parser.parse_args()

    raw_clinic_path = Path(args.clinic_csv)
    clinic_path = raw_clinic_path if raw_clinic_path.is_absolute() else (project_root / raw_clinic_path).resolve()

    raw_cohort_map_path = Path(args.cohort_map)
    cohort_map_path = raw_cohort_map_path if raw_cohort_map_path.is_absolute() else (project_root / raw_cohort_map_path).resolve()

    raw_cohorts_dir = Path(args.cohorts_dir)
    cohorts_dir = raw_cohorts_dir if raw_cohorts_dir.is_absolute() else (project_root / raw_cohorts_dir).resolve()

    raw_output_dir = Path(args.output_dir)
    base_output = raw_output_dir if raw_output_dir.is_absolute() else (project_root / raw_output_dir).resolve()

    if not clinic_path.exists():
        raise FileNotFoundError(f"clinic_mastersheet not found: {clinic_path}")
    if not cohort_map_path.exists():
        raise FileNotFoundError(f"Cohort map not found: {cohort_map_path}")

    print("Loading data...")
    clinic_df = load_clinic_mastersheet(clinic_path)
    cohort_index, loaded_count, deeplink_url_map = load_cohort_index_from_map(cohort_map_path, cohorts_dir)

    total_users = sum(len(v) for v in cohort_index.values())
    print(f"  Clinic mastersheet : {len(clinic_df)} usable rows")
    print(f"  Cohort files loaded: {loaded_count} file(s), {total_users} users across {len(cohort_index)} cohort(s)")
    print()

    today = pd.Timestamp.now().normalize()

    if args.date:
        try:
            target = pd.Timestamp(datetime.strptime(args.date, "%d%m%Y"))
        except ValueError:
            print(f"[ERROR] --date must be in DDMMYYYY format, got: {args.date!r}")
            sys.exit(1)
        run_dates = [target]
    else:
        run_dates = [today]

    run_slots = ["morning", "evening"] if args.slot == "both" else [args.slot]

    print("Dates to process:")
    for d in run_dates:
        print(f"  {d.strftime('%d/%m/%Y')}")
    print(f"Slots    : {', '.join(run_slots)}")
    print()

    processed = 0
    total = len(run_dates) * len(run_slots)
    for run_date in run_dates:
        for run_slot in run_slots:
            out_dir = base_output / f"{run_date.strftime('%d%m%Y')}_{run_slot}"
            found = build_priority_files(
                clinic_df, cohort_index, deeplink_url_map, run_date, run_slot, out_dir
            )
            if not found:
                print(
                    f"  [{run_date.strftime('%d/%m/%Y')} | {run_slot}] "
                    "No matching rows -- skipped."
                )
            else:
                processed += 1

    print()
    print(f"Done. {processed}/{total} slot(s) had data and were processed.")


if __name__ == "__main__":
    main()
