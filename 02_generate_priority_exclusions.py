import argparse
import re
from pathlib import Path
from typing import Tuple

import pandas as pd


def normalize_key(value: str) -> str:
    """Lowercase and keep only alphanumeric chars for robust matching."""
    text = str(value).strip().lower()
    # Treat quantity variants as equivalent (e.g., multiple/double/two -> 2).
    text = re.sub(r"\b(multiple|double|two|2x)\b", "2", text)
    return re.sub(r"[^a-z0-9]", "", text)


def parse_tag_to_cohort_key(tag_value: str) -> str:
    """Convert tag strings like Healthcare:Clinic_VaccineDueN2B_Cx -> vaccineduen2b."""
    if pd.isna(tag_value):
        return ""

    raw = str(tag_value).strip()
    if ":" in raw:
        raw = raw.split(":", 1)[1]

    # Remove common wrappers in tag naming.
    raw = re.sub(r"^clinic_", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"_cx$", "", raw, flags=re.IGNORECASE)

    return normalize_key(raw)


def sanitize_filename(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name).strip())
    return safe.strip("_") or "unnamed"


def pick_slot_column(df: pd.DataFrame) -> str:
    """Prefer Campaign Name for morning/evening slot markers, then fallback to Slot."""
    for col in ["Campaign Name", "Campiagn Name", "Slot"]:
        if col in df.columns:
            values = df[col].fillna("").astype(str).str.strip().str.lower()
            if values.isin(["morning", "evening"]).any():
                return col
    raise ValueError("No slot column found with values 'morning'/'evening'.")


def build_priority_files(
    clinic_df: pd.DataFrame,
    cohort_df: pd.DataFrame,
    run_date: pd.Timestamp,
    run_slot: str,
    output_dir: Path,
) -> bool:
    """Generate exclusion CSVs for one date+slot combo.

    Returns True if rows were found and files were written, False if no data
    exists for this date/slot (caller should skip silently).
    """
    run_df = clinic_df.loc[
        clinic_df["_slot"].eq(run_slot)
        & clinic_df["_date"].eq(run_date)
        & clinic_df["Cohort Name"].str.strip().ne("")
        & clinic_df["Title"].str.strip().ne("")
        & clinic_df["Content"].str.strip().ne(""),
        ["Date", "Cohort Name", "Title", "Content"],
    ].copy().reset_index(drop=True)

    if run_df.empty:
        return False

    output_dir.mkdir(parents=True, exist_ok=True)

    targeted_emails: set = set()
    summary_rows = []

    for priority, (_, row) in enumerate(run_df.iterrows(), start=1):
        cohort_name = str(row["Cohort Name"]).strip()
        cohort_key = normalize_key(cohort_name)

        exact_match = cohort_df[cohort_df["_tag_key"].eq(cohort_key)]
        if exact_match.empty:
            matched_df = cohort_df[
                cohort_df["_tag_key"].apply(
                    lambda tag_key: bool(tag_key)
                    and (cohort_key in tag_key or tag_key in cohort_key)
                )
            ]
        else:
            matched_df = exact_match

        candidate_emails = set(matched_df["_email"].tolist())
        final_emails = sorted(candidate_emails - targeted_emails)
        targeted_emails.update(final_emails)

        out_name = f"{priority:02d}_{sanitize_filename(cohort_name)}.csv"
        pd.DataFrame({"Email": final_emails}).to_csv(output_dir / out_name, index=False)

        summary_rows.append(
            {
                "priority": priority,
                "cohort_name": cohort_name,
                "input_candidates": len(candidate_emails),
                "excluded_by_higher_priority": len(candidate_emails) - len(final_emails),
                "final_count": len(final_emails),
                "output_file": out_name,
            }
        )

    pd.DataFrame(summary_rows).to_csv(output_dir / "summary.csv", index=False)

    print(
        f"  [{run_date.strftime('%d/%m/%Y')} | {run_slot}] "
        f"{len(summary_rows)} priorities processed → {output_dir}"
    )
    return True


def load_and_prepare(
    clinic_csv: Path, cohort_csv: Path
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load, validate, and pre-process both source CSVs.

    Returns (clinic_df, cohort_df).
    """
    clinic_df = pd.read_csv(clinic_csv, dtype=str, keep_default_na=False)
    cohort_df = pd.read_csv(cohort_csv, dtype=str, keep_default_na=False)

    required_clinic_cols = {"Date", "Cohort Name", "Title", "Content"}
    missing_clinic = required_clinic_cols - set(clinic_df.columns)
    if missing_clinic:
        raise ValueError(f"clinic_mastersheet is missing columns: {sorted(missing_clinic)}")

    required_cohort_cols = {"Email", "Tags"}
    missing_cohort = required_cohort_cols - set(cohort_df.columns)
    if missing_cohort:
        raise ValueError(f"master_cohort is missing columns: {sorted(missing_cohort)}")

    # Pre-process clinic_df.
    slot_col = pick_slot_column(clinic_df)
    clinic_df["_slot"] = clinic_df[slot_col].fillna("").astype(str).str.strip().str.lower()
    clinic_df["_date"] = pd.to_datetime(clinic_df["Date"], format="%d/%m/%Y", errors="coerce")

    usable = (
        clinic_df["_slot"].isin(["morning", "evening"])
        & clinic_df["_date"].notna()
        & clinic_df["Cohort Name"].str.strip().ne("")
        & clinic_df["Title"].str.strip().ne("")
        & clinic_df["Content"].str.strip().ne("")
    )
    if not usable.any():
        raise ValueError("No usable rows found in clinic_mastersheet.")

    # Pre-process cohort_df.
    cohort_df["_email"] = cohort_df["Email"].fillna("").astype(str).str.strip().str.lower()
    cohort_df = cohort_df[
        cohort_df["_email"].ne("")
        & cohort_df["_email"].ne("#error!")
        & cohort_df["_email"].str.contains("@", regex=False)
    ].copy()
    cohort_df["_tag_key"] = cohort_df["Tags"].map(parse_tag_to_cohort_key)

    return clinic_df, cohort_df


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate per-priority exclusion CSVs for the latest date and the following day, "
            "for both morning and evening slots, based on available clinic master sheet data."
        )
    )
    parser.add_argument(
        "--clinic-csv",
        default="data/clinic_mastersheet.csv",
        help="Path to clinic master sheet CSV",
    )
    parser.add_argument(
        "--cohort-csv",
        default="data/master_cohort.csv",
        help="Path to master cohort CSV",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Base output directory. Each slot writes to <output-dir>/<DDMMYYYY>_<slot>/",
    )

    args = parser.parse_args()

    clinic_path = Path(args.clinic_csv)
    cohort_path = Path(args.cohort_csv)

    if not clinic_path.exists():
        raise FileNotFoundError(f"Clinic CSV not found: {clinic_path}")
    if not cohort_path.exists():
        raise FileNotFoundError(f"Cohort CSV not found: {cohort_path}")

    clinic_df, cohort_df = load_and_prepare(clinic_path, cohort_path)

    today = pd.Timestamp.now().normalize()
    tomorrow = today + pd.Timedelta(days=1)

    print(f"Today  : {today.strftime('%d/%m/%Y')}")
    print(f"Tomorrow: {tomorrow.strftime('%d/%m/%Y')}")
    print()

    base_dir = Path(args.output_dir)
    processed = 0

    for run_date in [today, tomorrow]:
        for run_slot in ["morning", "evening"]:
            out_dir = base_dir / f"{run_date.strftime('%d%m%Y')}_{run_slot}"
            found = build_priority_files(clinic_df, cohort_df, run_date, run_slot, out_dir)
            if not found:
                print(
                    f"  [{run_date.strftime('%d/%m/%Y')} | {run_slot}] No data — skipped."
                )
            else:
                processed += 1

    print(f"\nDone. {processed}/4 slot(s) had data and were processed.")


if __name__ == "__main__":
    main()
