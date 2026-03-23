"""Generate per-priority exclusion CSVs from clinic_mastersheet and clinic_user_base_mastersheet.

⚠️  DISCLAIMER: This script only generates intermediate data files.
    No CleverTap API calls are made here. Campaign triggering requires
    04_trigger_campaign.py run explicitly with the --live flag.

Usage:
    python 02_generate_priority_exclusions.py
    python 02_generate_priority_exclusions.py --clinic-csv data/clinic_mastersheet.csv \\
        --user-base-csv data/clinic_user_base_mastersheet.csv --output-dir outputs
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from utils import normalize_cohort, sanitize_filename

# -- Feature flags ------------------------------------------------------------
# Set to True to enable Exclusion-column-based filtering once the column
# contains real data and the feature is ready for production.
ENABLE_EXCLUSION_COL: bool = False


# -- Exclusion column helper (written but disabled) ---------------------------

def parse_exclusion_col(exclusion_str: str) -> List[str]:
    """Parse the Exclusion column value into a list of normalized cohort keys.

    The Exclusion column contains comma-separated cohort names whose members
    should be excluded from the current cohort, regardless of priority order.

    NOTE: This function is implemented but intentionally NOT called while
    ENABLE_EXCLUSION_COL is False.  Enable it by setting the flag above.

    Args:
        exclusion_str: Raw cell value from the Exclusion column.

    Returns:
        List of normalized cohort keys (empty list if blank / NaN).
    """
    if not exclusion_str or pd.isna(exclusion_str):
        return []
    parts = str(exclusion_str).split(",")
    return [normalize_cohort(p) for p in parts if p.strip()]


# -- Data loaders -------------------------------------------------------------

def load_user_base(path: Path) -> pd.DataFrame:
    """Load and validate clinic_user_base_mastersheet.csv.

    Expected columns: Email, Cohort Name, First Name, Pet Name
    'First Name' and 'Pet Name' are optional (blank cells are kept as-is).

    Returns a DataFrame with an additional '_cohort_key' column.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    required = {"Email", "Cohort Name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"clinic_user_base_mastersheet is missing columns: {sorted(missing)}"
        )

    # Ensure optional columns exist (may be absent in early data versions).
    for col in ("First Name", "Pet Name"):
        if col not in df.columns:
            df[col] = ""

    # Normalise emails; drop blanks and clearly invalid entries.
    df["_email"] = df["Email"].str.strip().str.lower()
    df = df[
        df["_email"].ne("")
        & df["_email"].str.contains("@", regex=False)
        & df["_email"].ne("#error!")
    ].copy()

    df["_cohort_key"] = df["Cohort Name"].map(normalize_cohort)

    return df.reset_index(drop=True)


def build_cohort_email_index(
    user_df: pd.DataFrame,
) -> Dict[str, List[Tuple[str, str, str]]]:
    """Build a lookup: normalized_cohort_key -> [(email, first_name, pet_name), ...].

    Multi-pet / multi-name users are preserved as separate tuples so that
    each pet receives its own personalized notification.
    """
    index: Dict[str, List[Tuple[str, str, str]]] = {}
    for _, row in user_df.iterrows():
        key = row["_cohort_key"]
        if not key:
            continue
        entry = (
            row["_email"],
            row.get("First Name", "").strip(),
            row.get("Pet Name", "").strip(),
        )
        index.setdefault(key, []).append(entry)
    return index


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
    run_date: pd.Timestamp,
    run_slot: str,
    output_dir: Path,
) -> bool:
    """Generate exclusion CSVs and metadata for one date + slot combination.

    Priority ordering is determined by row order in clinic_df for the given
    date and slot.  Users targeted by a higher-priority cohort are excluded
    from all lower-priority cohorts.

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

        # -- Exclusion column (disabled) -----------------------------------
        excluded_by_exclusion_col = 0
        if ENABLE_EXCLUSION_COL:
            exclusion_keys = parse_exclusion_col(row.get("Exclusion", ""))
            exclusion_emails: set = set()
            for ex_key in exclusion_keys:
                for t in cohort_index.get(ex_key, []):
                    exclusion_emails.add(t[0])
        else:
            exclusion_emails = set()
        # -----------------------------------------------------------------

        # Apply priority exclusion (and optional column exclusion).
        final_tuples = [
            t
            for t in candidate_tuples
            if t[0] not in targeted_emails and t[0] not in exclusion_emails
        ]

        excluded_by_priority = input_candidates - len(
            {t[0] for t in final_tuples}
        ) - excluded_by_exclusion_col

        # Update targeted set (by email, not by tuple -- one email = one slot).
        targeted_emails.update(t[0] for t in final_tuples)

        # Write per-priority CSV: Email, First Name, Pet Name
        out_name = f"{priority:02d}_{sanitize_filename(cohort_name)}.csv"
        pd.DataFrame(
            [{"Email": t[0], "First Name": t[1], "Pet Name": t[2]} for t in final_tuples]
        ).to_csv(output_dir / out_name, index=False)

        summary_rows.append(
            {
                "priority": priority,
                "cohort_name": cohort_name,
                "input_candidates": input_candidates,
                "excluded_by_priority": excluded_by_priority,
                "excluded_by_exclusion_col": excluded_by_exclusion_col,
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
            }
        )

    pd.DataFrame(summary_rows).to_csv(output_dir / "summary.csv", index=False)
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

    parser = argparse.ArgumentParser(
        description=(
            "Generate per-priority exclusion CSVs from clinic_mastersheet "
            "and clinic_user_base_mastersheet for today and tomorrow, "
            "both morning and evening slots."
        )
    )
    parser.add_argument(
        "--clinic-csv",
        default="data/clinic_mastersheet.csv",
        help="Path to clinic_mastersheet.csv (default: data/clinic_mastersheet.csv)",
    )
    parser.add_argument(
        "--user-base-csv",
        default="data/clinic_user_base_mastersheet.csv",
        help=(
            "Path to clinic_user_base_mastersheet.csv "
            "(default: data/clinic_user_base_mastersheet.csv)"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory (default: outputs)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date in DDMMYYYY format. Default: processes yesterday, today, and tomorrow.",
    )
    parser.add_argument(
        "--slot",
        choices=["morning", "evening", "both"],
        default="both",
        help="Slot(s) to process: morning, evening, or both (default: both).",
    )
    args = parser.parse_args()

    clinic_path = (script_dir / args.clinic_csv).resolve()
    user_base_path = (script_dir / args.user_base_csv).resolve()
    base_output = (script_dir / args.output_dir).resolve()

    if not clinic_path.exists():
        raise FileNotFoundError(f"clinic_mastersheet not found: {clinic_path}")
    if not user_base_path.exists():
        raise FileNotFoundError(
            f"clinic_user_base_mastersheet not found: {user_base_path}"
        )

    print("Loading data...")
    clinic_df = load_clinic_mastersheet(clinic_path)
    user_df = load_user_base(user_base_path)
    cohort_index = build_cohort_email_index(user_df)

    print(f"  Clinic mastersheet : {len(clinic_df)} usable rows")
    print(f"  User base          : {len(user_df)} users across {len(cohort_index)} cohorts")
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
        yesterday = today - pd.Timedelta(days=1)
        tomorrow = today + pd.Timedelta(days=1)
        run_dates = [yesterday, today, tomorrow]

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
                clinic_df, cohort_index, run_date, run_slot, out_dir
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
