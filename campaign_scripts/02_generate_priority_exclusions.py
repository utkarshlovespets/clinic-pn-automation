"""Generate per-priority exclusion CSVs from clinic_mastersheet and cohort CSV files.

⚠️  DISCLAIMER: This script only generates intermediate data files.
    No CleverTap API calls are made here. Campaign triggering requires
    04_trigger_campaign.py run explicitly with the --live flag.

Usage:
    python 02_generate_priority_exclusions.py
    python 02_generate_priority_exclusions.py --clinic-csv data/clinic_mastersheet.csv \\
        --cohort-map data/cohort_mapping.csv --exclusion-map data/exclusion_mapping.csv \\
        --output-dir outputs
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
COHORT_CODE_COL = "cohort_code"
COHORT_NAME_COL = "cohort_name"
COHORT_DEFAULT_EXCLUSION_COL = "exclusion"
EXCLUSION_NAME_COL = "exclusion_name"
EXCLUSION_DATASET_COL = "dataset"


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
    return [normalize_cohort(name) for name in parse_exclusion_names(exclusion_str)]


def parse_exclusion_names(exclusion_str: str) -> List[str]:
    """Parse a comma-separated exclusion cell into display names."""
    if not exclusion_str or pd.isna(exclusion_str):
        return []
    parts = str(exclusion_str).split(",")
    return [p.strip() for p in parts if p.strip()]


def merge_exclusion_names(*groups: List[str]) -> List[str]:
    """Merge exclusion name lists, preserving order and removing duplicates."""
    merged: List[str] = []
    seen = set()
    for group in groups:
        for name in group:
            key = normalize_cohort(name)
            if not key or key in seen:
                continue
            merged.append(name)
            seen.add(key)
    return merged


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
) -> Tuple[
    Dict[str, List[Tuple[str, str, str]]],
    Dict[str, List[Tuple[str, str, str]]],
    int,
    Dict[str, str],
    Dict[str, str],
]:
    """Build cohort index by loading individual cohort CSV files.

    Reads cohort_map_path to discover the mapping:
        campaign_id -> cohort_dataset filename in cohorts_dir

    For each cohort with a non-blank cohort_dataset, loads the CSV from
    cohorts_dir and adds its users to the index.

    Expected cohort CSV columns (case-insensitive): email, first_name, pet_name

    Returns:
        campaign_index:
            {campaign_id: [(email, first_name, pet_name), ...]}
        exclusion_index:
            {normalized_cohort_key: [(email, first_name, pet_name), ...]}
    """
    if not cohort_map_path.exists():
        raise FileNotFoundError(f"Cohort map not found: {cohort_map_path}")

    map_df = pd.read_csv(cohort_map_path, dtype=str, keep_default_na=False)

    required = {COHORT_CODE_COL, "campaign_id", "cohort_dataset"}
    missing = required - set(map_df.columns)
    if missing:
        raise ValueError(
            f"Cohort map is missing columns: {sorted(missing)}. "
            "Fetch or update the Cohort_Mapping sheet."
        )

    campaign_index: Dict[str, List[Tuple[str, str, str]]] = {}
    exclusion_index: Dict[str, List[Tuple[str, str, str]]] = {}
    deeplink_url_map: Dict[str, str] = {}
    campaign_cohort_name_map: Dict[str, str] = {}
    loaded = 0

    for _, row in map_df.iterrows():
        cohort_name = str(row[COHORT_CODE_COL]).strip()
        campaign_id = str(row["campaign_id"]).strip()
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
        if campaign_id:
            deeplink_url_map[campaign_id] = str(row.get("android_base_url", "")).strip()
            campaign_cohort_name_map[campaign_id] = cohort_name

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

        exclusion_index[cohort_key] = entries
        if campaign_id:
            campaign_index[campaign_id] = entries
        loaded += 1

    return campaign_index, exclusion_index, loaded, deeplink_url_map, campaign_cohort_name_map


def load_user_entries(csv_path: Path, dataset_label: str) -> List[Tuple[str, str, str]]:
    """Load one cohort/exclusion user CSV into normalized user tuples."""
    if not csv_path.exists():
        print(
            f"  [WARNING] Cohort file not found for '{dataset_label}': {csv_path} "
            "-- skipping."
        )
        return []

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "email" not in df.columns:
        print(f"  [WARNING] '{csv_path.name}' has no 'email' column -- skipping.")
        return []

    for col in ("first_name", "pet_name"):
        if col not in df.columns:
            df[col] = ""

    entries = []
    for _, user_row in df.iterrows():
        email = str(user_row["email"]).strip().lower()
        if not email or "@" not in email or email == "#error!":
            continue
        raw_first = str(user_row.get("first_name", "")).strip()
        first_name = raw_first.split()[0] if raw_first else ""
        entries.append((
            email,
            first_name,
            str(user_row.get("pet_name", "")).strip(),
        ))

    return entries


def load_campaign_cohort_index_from_map(
    cohort_map_path: Path,
    cohorts_dir: Path,
) -> Tuple[
    Dict[str, List[Tuple[str, str, str]]],
    Dict[str, List[Tuple[str, str, str]]],
    Dict[str, List[str]],
    int,
    Dict[str, str],
    Dict[str, str],
]:
    """Build campaign cohort index from Cohort_Mapping export."""
    if not cohort_map_path.exists():
        raise FileNotFoundError(f"Cohort map not found: {cohort_map_path}")

    map_df = pd.read_csv(cohort_map_path, dtype=str, keep_default_na=False)
    required = {COHORT_CODE_COL, "campaign_id", "cohort_dataset"}
    missing = required - set(map_df.columns)
    if missing:
        raise ValueError(
            f"Cohort map is missing columns: {sorted(missing)}. "
            "Fetch or update the Cohort_Mapping sheet."
        )

    campaign_index: Dict[str, List[Tuple[str, str, str]]] = {}
    cohort_exclusion_index: Dict[str, List[Tuple[str, str, str]]] = {}
    campaign_default_exclusion_map: Dict[str, List[str]] = {}
    deeplink_url_map: Dict[str, str] = {}
    campaign_cohort_name_map: Dict[str, str] = {}
    loaded = 0

    for _, row in map_df.iterrows():
        cohort_name = str(row.get(COHORT_NAME_COL, "")).strip()
        cohort_code = str(row[COHORT_CODE_COL]).strip()
        campaign_id = str(row["campaign_id"]).strip()
        dataset_file = str(row["cohort_dataset"]).strip()
        default_exclusion_names = parse_exclusion_names(
            str(row.get(COHORT_DEFAULT_EXCLUSION_COL, "")).strip()
        )
        if not cohort_code or not dataset_file:
            continue

        if campaign_id:
            deeplink_url_map[campaign_id] = str(row.get("android_base_url", "")).strip()
            campaign_cohort_name_map[campaign_id] = cohort_code
            campaign_default_exclusion_map[campaign_id] = default_exclusion_names

        entries = load_user_entries(cohorts_dir / dataset_file, cohort_code)
        if not entries:
            continue

        cohort_exclusion_index[normalize_cohort(cohort_code)] = entries
        if cohort_name:
            cohort_exclusion_index[normalize_cohort(cohort_name)] = entries
        if campaign_id:
            campaign_index[campaign_id] = entries
        loaded += 1

    return (
        campaign_index,
        cohort_exclusion_index,
        campaign_default_exclusion_map,
        loaded,
        deeplink_url_map,
        campaign_cohort_name_map,
    )


def load_exclusion_index_from_map(
    exclusion_map_path: Path,
    cohorts_dir: Path,
) -> Tuple[Dict[str, List[Tuple[str, str, str]]], int]:
    """Build exclusion index from Exclusion_Mapping export."""
    if not exclusion_map_path.exists():
        raise FileNotFoundError(f"Exclusion map not found: {exclusion_map_path}")

    map_df = pd.read_csv(exclusion_map_path, dtype=str, keep_default_na=False)
    required = {EXCLUSION_NAME_COL, EXCLUSION_DATASET_COL}
    missing = required - set(map_df.columns)
    if missing:
        raise ValueError(
            f"Exclusion map is missing columns: {sorted(missing)}. "
            "Fetch or update the Exclusion_Mapping sheet."
        )

    exclusion_index: Dict[str, List[Tuple[str, str, str]]] = {}
    loaded = 0

    for _, row in map_df.iterrows():
        exclusion_name = str(row[EXCLUSION_NAME_COL]).strip()
        dataset_file = str(row[EXCLUSION_DATASET_COL]).strip()
        if not exclusion_name or not dataset_file:
            continue

        entries = load_user_entries(cohorts_dir / dataset_file, exclusion_name)
        if not entries:
            continue

        exclusion_index[normalize_cohort(exclusion_name)] = entries
        loaded += 1

    return exclusion_index, loaded


def load_clinic_mastersheet(path: Path) -> pd.DataFrame:
    """Load and validate clinic_mastersheet.csv.

    Expected columns: Date, Day, Slot, Cohort Name, Campaign ID, Exclusion,
    Title, Content, Image
    Adds '_date' (Timestamp) and '_slot' (lowercase string) columns.
    Rows with unparseable dates or blank campaign IDs are filtered out.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    required = {"Date", "Cohort Name", "Campaign ID", "Title", "Content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"clinic_mastersheet is missing columns: {sorted(missing)}"
        )

    if "Slot" not in df.columns:
        df["Slot"] = ""
    if "Exclusion" not in df.columns:
        df["Exclusion"] = ""
    if "Image" not in df.columns:
        df["Image"] = ""

    df["_date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    df["_slot"] = df["Slot"].fillna("").str.strip().str.lower()

    usable = (
        df["_date"].notna()
        & df["Campaign ID"].str.strip().ne("")
    )
    df = df[usable].copy().reset_index(drop=True)

    return df


# -- Core build logic ----------------------------------------------------------

def build_priority_files(
    clinic_df: pd.DataFrame,
    cohort_index: Dict[str, List[Tuple[str, str, str]]],
    exclusion_index: Dict[str, List[Tuple[str, str, str]]],
    campaign_default_exclusion_map: Dict[str, List[str]],
    deeplink_url_map: Dict[str, str],
    campaign_cohort_name_map: Dict[str, str],
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
    of priority. Default exclusions from cohort_mapping.csv are applied before
    row-level mastersheet exclusions.

    Returns True if at least one row was found and files were written.
    """
    run_df = clinic_df[
        clinic_df["_date"].eq(run_date) & clinic_df["_slot"].eq(run_slot)
    ].copy().reset_index(drop=True)

    if run_df.empty:
        return False

    # Deduplicate: if the same campaign appears multiple times for the same
    # date+slot, keep only the first occurrence (first row = highest priority).
    run_df = run_df.drop_duplicates(subset=["Campaign ID"], keep="first").reset_index(
        drop=True
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    targeted_emails: set = set()
    summary_rows = []

    for priority, (_, row) in enumerate(run_df.iterrows(), start=1):
        mastersheet_cohort_name = str(row["Cohort Name"]).strip()
        campaign_id = str(row["Campaign ID"]).strip()
        image_name = str(row.get("Image", "")).strip()
        cohort_name = campaign_cohort_name_map.get(campaign_id, mastersheet_cohort_name)

        candidate_tuples = cohort_index.get(campaign_id, [])
        input_candidates = len({t[0] for t in candidate_tuples})  # unique emails

        # -- Exclusion column -----------------------------------------------
        default_exclusion_names: List[str] = []
        row_exclusion_names: List[str] = []
        exclusion_cohort_names: List[str] = []
        if ENABLE_EXCLUSION_COL:
            default_exclusion_names = merge_exclusion_names(
                campaign_default_exclusion_map.get(campaign_id, [])
            )
            row_exclusion_names = merge_exclusion_names(
                parse_exclusion_names(str(row.get("Exclusion", "")).strip())
            )
            default_exclusion_keys = {
                normalize_cohort(name) for name in default_exclusion_names
            }
            row_extra_exclusion_names = [
                name
                for name in row_exclusion_names
                if normalize_cohort(name) not in default_exclusion_keys
            ]
            exclusion_cohort_names = row_extra_exclusion_names
            default_exclusion_emails: set = set()
            for ex_key in [normalize_cohort(name) for name in default_exclusion_names]:
                for t in exclusion_index.get(ex_key, []):
                    default_exclusion_emails.add(t[0])

            row_exclusion_emails: set = set()
            for ex_key in [normalize_cohort(name) for name in row_extra_exclusion_names]:
                for t in exclusion_index.get(ex_key, []):
                    row_exclusion_emails.add(t[0])

            exclusion_emails = default_exclusion_emails | row_exclusion_emails
        else:
            default_exclusion_emails = set()
            row_exclusion_emails = set()
            exclusion_emails = set()
        # -------------------------------------------------------------------

        candidate_emails = {t[0] for t in candidate_tuples}

        # Apply priority exclusion and named exclusions.
        final_tuples = [
            t
            for t in candidate_tuples
            if t[0] not in targeted_emails and t[0] not in exclusion_emails
        ]
        # Count exclusions without double-counting users excluded by multiple
        # filters. Attribution order: priority, default exclusion, mastersheet
        # Exclusion column.
        priority_excl = candidate_emails & targeted_emails
        default_excl = (candidate_emails & default_exclusion_emails) - targeted_emails
        row_excl = (
            (candidate_emails & row_exclusion_emails)
            - targeted_emails
            - default_exclusion_emails
        )

        excluded_by_priority = len(priority_excl)
        excluded_by_default = len(default_excl)
        excluded_by_exclusion_col = len(row_excl)

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
                "mastersheet_cohort_name": mastersheet_cohort_name,
                "campaign_id": campaign_id,
                "image_name": image_name,
                "image_url": "",
                "base_campaign_id": campaign_id,
                "img_campaign_id": "",
                "title_template": str(row.get("Title", "")).strip(),
                "content_template": str(row.get("Content", "")).strip(),
                "input_candidates": input_candidates,
                "excluded_by_priority": excluded_by_priority,
                "excluded_by_default": excluded_by_default,
                "excluded_by_exclusion_col": excluded_by_exclusion_col,
                "default_exclusion_cohorts": "; ".join(default_exclusion_names),
                "exclusion_cohorts": "; ".join(exclusion_cohort_names),
                "final_count": len(final_tuples),
                "output_file": out_name,
            }
        )

    date_formatted = run_date.strftime("%d%B")
    date_value = run_date.strftime("%d/%m/%Y")
    slot_value = "Evening" if run_slot.strip().lower() == "evening" else "Morning"
    log_summary_rows = []
    for row in summary_rows:
        priority = int(row["priority"])
        campaign_id = str(row["campaign_id"]).strip()
        deeplink_tpl = deeplink_url_map.get(campaign_id, "")
        deeplink_priority = get_deeplink_priority_token(run_slot, priority)
        resolved_url = build_deeplink(deeplink_tpl, date_formatted, deeplink_priority)
        log_summary_rows.append(
            {
                "date": date_value,
                "slot": slot_value,
                "priority": priority,
                "campaign_id": campaign_id,
                "image_name": str(row.get("image_name", "")).strip(),
                "image_url": str(row.get("image_url", "")).strip(),
                "base_campaign_id": str(row.get("base_campaign_id", "")).strip(),
                "img_campaign_id": str(row.get("img_campaign_id", "")).strip(),
                "utm_campaign": extract_utm_campaign(resolved_url),
                "title_template": str(row.get("title_template", "")).strip(),
                "content_template": str(row.get("content_template", "")).strip(),
                "excluded_by_priority": int(row["excluded_by_priority"]),
                "excluded_by_default": int(row["excluded_by_default"]),
                "excluded_by_exclusion_col": int(row["excluded_by_exclusion_col"]),
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
            "campaign_id",
            "image_name",
            "image_url",
            "base_campaign_id",
            "img_campaign_id",
            "utm_campaign",
            "title_template",
            "content_template",
            "excluded_by_priority",
            "excluded_by_default",
            "excluded_by_exclusion_col",
            "final_count",
        ],
    ).to_csv(log_summary_path, index=False)

    legacy_meta_path = output_dir / "campaign_meta.csv"
    if legacy_meta_path.exists():
        try:
            legacy_meta_path.unlink()
        except PermissionError:
            print(
                f"  [WARNING] Could not remove legacy metadata file "
                f"{legacy_meta_path}. Close it and rerun Stage 2 if you want "
                "only summary.csv in the output folder."
            )
    pd.DataFrame(summary_rows).to_csv(output_dir / "summary.csv", index=False)

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
        default="data/cohort_mapping.csv",
        help=(
            "Path to Cohort_Mapping export with cohort_code, campaign_id, "
            "cohort_dataset, default exclusions, and deeplink templates "
            "(default: data/cohort_mapping.csv)"
        ),
    )
    parser.add_argument(
        "--exclusion-map",
        default="data/exclusion_mapping.csv",
        help=(
            "Path to Exclusion_Mapping export with 'Exclusion Name' and 'Dataset' "
            "columns (default: data/exclusion_mapping.csv)"
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

    raw_exclusion_map_path = Path(args.exclusion_map)
    exclusion_map_path = raw_exclusion_map_path if raw_exclusion_map_path.is_absolute() else (project_root / raw_exclusion_map_path).resolve()

    raw_cohorts_dir = Path(args.cohorts_dir)
    cohorts_dir = raw_cohorts_dir if raw_cohorts_dir.is_absolute() else (project_root / raw_cohorts_dir).resolve()

    raw_output_dir = Path(args.output_dir)
    base_output = raw_output_dir if raw_output_dir.is_absolute() else (project_root / raw_output_dir).resolve()

    if not clinic_path.exists():
        raise FileNotFoundError(f"clinic_mastersheet not found: {clinic_path}")
    if not cohort_map_path.exists():
        raise FileNotFoundError(f"Cohort map not found: {cohort_map_path}")
    if not exclusion_map_path.exists():
        raise FileNotFoundError(f"Exclusion map not found: {exclusion_map_path}")

    print("Loading data...")
    clinic_df = load_clinic_mastersheet(clinic_path)
    (
        cohort_index,
        cohort_exclusion_index,
        campaign_default_exclusion_map,
        loaded_count,
        deeplink_url_map,
        campaign_cohort_name_map,
    ) = load_campaign_cohort_index_from_map(cohort_map_path, cohorts_dir)
    exclusion_index, exclusion_loaded_count = load_exclusion_index_from_map(
        exclusion_map_path, cohorts_dir
    )
    combined_exclusion_index = dict(cohort_exclusion_index)
    combined_exclusion_index.update(exclusion_index)
    exclusion_index = combined_exclusion_index

    total_users = sum(len(v) for v in cohort_index.values())
    default_exclusion_count = sum(
        1 for names in campaign_default_exclusion_map.values() if names
    )
    print(f"  Clinic mastersheet : {len(clinic_df)} usable rows")
    print(f"  Cohort files loaded: {loaded_count} file(s), {total_users} users across {len(cohort_index)} campaign(s)")
    print(f"  Exclusion files loaded: {exclusion_loaded_count} file(s)")
    print(f"  Default exclusions : {default_exclusion_count} cohort mapping row(s)")
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
                clinic_df,
                cohort_index,
                exclusion_index,
                campaign_default_exclusion_map,
                deeplink_url_map,
                campaign_cohort_name_map,
                run_date,
                run_slot,
                out_dir,
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
