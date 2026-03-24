"""Append resolved title / body and deeplink columns to each priority exclusion CSV.

Reads the output directory produced by 02_generate_priority_exclusions.py,
resolves personalized (or generic) campaign content per user, builds deeplinks
with a dynamic date substitution, and writes the enriched CSVs back in place.

Added columns:
    title           -- resolved push notification title
    body           -- resolved push notification body
    android_deeplink -- Android deeplink URL with {date} placeholder filled in
    ios_deeplink     -- iOS universal link URL with {date} placeholder filled in

title / body rules:
    - User HAS Pet Name   -> {your pet} replaced with actual pet name
    - User HAS First Name -> {pet parent} replaced with actual first name
    - User has NEITHER    -> generic plain-text fallback: "your pet", "pet parent"
    (No CleverTap Liquid tags -- values are always fully resolved strings.)

Deeplink rules:
    - URL templates are read from a deeplink map CSV (--deeplink-map flag).
    - Each URL template may contain {date} and {priority} placeholders:
        {date}     -> DDMonth (e.g. "18March") from the output directory name
        {priority} -> cohort priority integer (1 = first/highest in the slot)
    - Cohort names in the deeplink map are matched via normalize_cohort() so
      apostrophes and casing differences are handled automatically.
    - If a cohort has no entry in the deeplink map, android_deeplink and
      ios_deeplink are written as empty strings (no crash).
    - If --deeplink-map is not provided, deeplink columns are omitted entirely.

deeplink_map.csv column format:
    Cohort Name      -- matches cohort names in clinic_mastersheet.csv
    android_base_url -- full Android URL template; use {date} where the date goes
    ios_base_url     -- full iOS URL template; use {date} where the date goes

    Example row:
        Rajaji_Nagar_n2b_15km,
        https://supertails.com/pages/supertails-clinic?utm_source=Clevertap&utm_medium=MobilePush&utm_campaign={date}_MP_{priority}_Clinic_xxRAJ,
        https://supertails.com/pages/supertails-clinic?utm_source=Clevertap&utm_medium=MobilePush&utm_campaign={date}_MP_1_Clinic_xxRAJ

After this script runs, each NN_<cohort>.csv will have columns:
    Email, First Name, Pet Name, title, body[, android_deeplink, ios_deeplink]

04_trigger_campaign.py then reads title/body directly without any
further template resolution.

Usage:
    python 03_prepare_campaign_content.py --output-dir outputs/19032026_morning
    python 03_prepare_campaign_content.py --output-dir outputs/19032026_morning \\
        --deeplink-map data/deeplink_map.csv
"""

import argparse
import sys
from datetime import datetime
from typing import Optional
from pathlib import Path

import pandas as pd

from utils import normalize_cohort, resolve_template


# -- Deeplink helpers ----------------------------------------------------------

def load_deeplink_map(path: Path) -> dict:
    """Load deeplink_map.csv and return a normalized-cohort-key lookup.

    Expected columns: Cohort Name, android_base_url, ios_base_url

    Returns:
        {normalized_cohort_key: (android_url_template, ios_url_template)}

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError:        If required columns are missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"Deeplink map not found: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"Cohort Name", "android_base_url", "ios_base_url"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"deeplink_map.csv is missing columns: {sorted(missing)}")

    result = {}
    for _, row in df.iterrows():
        key = normalize_cohort(row["Cohort Name"])
        if not key:
            continue
        result[key] = (
            str(row["android_base_url"]).strip(),
            str(row["ios_base_url"]).strip(),
        )
    return result


def build_deeplink(url_template: str, date_formatted: str, priority: int) -> str:
    """Replace {date} and {priority} placeholders in a URL template.

    Placeholders:
        {date}     -- replaced with DDMonth (e.g. "18March", "19March")
        {priority} -- replaced with the cohort's priority integer (e.g. 1, 2, 3)

    Args:
        url_template:   Full URL string containing placeholders.
                        Example: "...utm_campaign={date}_MP_{priority}_Clinic_xxRAJ"
        date_formatted: Date string in DDMonth format derived from the output dir name.
        priority:       Cohort priority (1 = highest, from the NN_ filename prefix).

    Returns:
        Resolved URL string, or "" if url_template is blank.
    """
    if not url_template:
        return ""
    return url_template.replace("{date}", date_formatted).replace("{priority}", str(priority))


def format_run_date(output_dir: Path) -> str:
    """Extract and format the run date from the output directory name.

    Directory names follow the pattern DDMMYYYY_slot (e.g. "19032026_morning").
    Returns the date as DDMonth with no leading zero on the day (e.g. "19March").
    """
    date_part = output_dir.name.split("_")[0]  # e.g. "19032026"
    run_date = datetime.strptime(date_part, "%d%m%Y")
    return f"{run_date.day}{run_date.strftime('%B')}"  # e.g. "19March"


# -- Core logic ----------------------------------------------------------------

def prepare_content(output_dir: Path, deeplink_map_path: Optional[Path] = None) -> None:
    """Enrich all priority CSVs in output_dir with title, body, and deeplink columns.

    Args:
        output_dir:        Path to a slot output directory (e.g. outputs/19032026_morning/).
        deeplink_map_path: Optional path to deeplink_map.csv. When provided, appends
                           android_deeplink and ios_deeplink columns. When None, those
                           columns are omitted.

    Raises:
        FileNotFoundError: If campaign_meta.csv or deeplink_map_path is missing.
        ValueError:        If required columns are absent from either CSV.
    """
    meta_path = output_dir / "campaign_meta.csv"
    if not meta_path.exists():
        raise FileNotFoundError(
            f"campaign_meta.csv not found in {output_dir}. "
            "Run 02_generate_priority_exclusions.py first."
        )

    meta_df = pd.read_csv(meta_path, dtype=str, keep_default_na=False)
    required = {"priority", "cohort_name", "title_template", "content_template"}
    missing = required - set(meta_df.columns)
    if missing:
        raise ValueError(f"campaign_meta.csv is missing columns: {sorted(missing)}")

    # Build priority -> (cohort_name, title_template, content_template) lookup.
    meta_lookup: dict = {}
    for _, row in meta_df.iterrows():
        try:
            p = int(row["priority"])
        except (ValueError, TypeError):
            continue
        meta_lookup[p] = (
            str(row["cohort_name"]).strip(),
            str(row["title_template"]).strip(),
            str(row["content_template"]).strip(),
        )

    # Load deeplink map if provided.
    deeplink_map: dict = {}
    use_deeplinks = deeplink_map_path is not None
    if use_deeplinks:
        deeplink_map = load_deeplink_map(deeplink_map_path)
        print(f"  Deeplink map loaded: {len(deeplink_map)} cohort(s) mapped.")

    # Date for {date} substitution in URL templates (e.g. "19March").
    date_formatted = format_run_date(output_dir)
    if use_deeplinks:
        print(f"  Run date            : {date_formatted}")

    csv_files = sorted(output_dir.glob("[0-9][0-9]_*.csv"))
    if not csv_files:
        print(f"  [WARNING] No priority CSV files found in {output_dir}")
        return

    total_enriched = 0
    total_rows = 0

    for csv_path in csv_files:
        try:
            priority = int(csv_path.stem.split("_")[0])
        except (ValueError, IndexError):
            print(f"  [WARNING] Skipping file with unexpected name: {csv_path.name}")
            continue

        if priority not in meta_lookup:
            print(
                f"  [WARNING] No campaign_meta entry for priority {priority} "
                f"({csv_path.name}) -- skipping."
            )
            continue

        cohort_name, title_tpl, content_tpl = meta_lookup[priority]

        # Resolve deeplinks for this cohort (same value for every user in the file).
        android_deeplink = ""
        ios_deeplink = ""
        if use_deeplinks:
            cohort_key = normalize_cohort(cohort_name)
            android_tpl, ios_tpl = deeplink_map.get(cohort_key, ("", ""))
            if not android_tpl and not ios_tpl:
                print(
                    f"  [WARNING] {csv_path.name}: '{cohort_name}' not in deeplink map "
                    "-- deeplink columns will be empty."
                )
            android_deeplink = build_deeplink(android_tpl, date_formatted, priority)
            ios_deeplink = build_deeplink(ios_tpl, date_formatted, priority)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

        if df.empty:
            df["title"] = ""
            df["body"] = ""
            if use_deeplinks:
                df["android_deeplink"] = ""
                df["ios_deeplink"] = ""
            df.to_csv(csv_path, index=False)
            cols = "title/body" + ("/deeplinks" if use_deeplinks else "")
            print(f"  {csv_path.name}: 0 rows -- written with empty {cols}.")
            continue

        for col in ("First Name", "Pet Name"):
            if col not in df.columns:
                df[col] = ""

        # Resolve title / body per user row.
        copy1_values = []
        copy2_values = []
        named_rows = 0

        for _, row in df.iterrows():
            first_name = str(row.get("First Name", "")).strip()
            pet_name = str(row.get("Pet Name", "")).strip()

            c1 = resolve_template(title_tpl, first_name, pet_name)
            c2 = resolve_template(content_tpl, first_name, pet_name)

            copy1_values.append(c1)
            copy2_values.append(c2)

            if first_name or pet_name:
                named_rows += 1

        df["title"] = copy1_values
        df["body"] = copy2_values

        if use_deeplinks:
            df["android_deeplink"] = android_deeplink
            df["ios_deeplink"] = ios_deeplink

        df.to_csv(csv_path, index=False)

        generic_rows = len(df) - named_rows
        print(
            f"  {csv_path.name}: {len(df)} rows enriched "
            f"({named_rows} personalised, {generic_rows} generic fallback)"
        )
        total_enriched += 1
        total_rows += len(df)

    print(
        f"\nDone. {total_enriched}/{len(csv_files)} CSV(s) enriched, "
        f"{total_rows} total rows."
    )


def main() -> None:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description=(
            "Append resolved title / body and optional deeplink columns to "
            "priority exclusion CSVs produced by 02_generate_priority_exclusions.py."
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
        "--deeplink-map",
        default="data/deeplink_map.csv",
        help=(
            "Path to deeplink_map.csv "
            "(columns: Cohort Name, android_base_url, ios_base_url). "
            "URL templates may contain {date} and {priority} placeholders. "
            "Appends android_deeplink and ios_deeplink columns when the file exists. "
            "(default: data/deeplink_map.csv)"
        ),
    )
    args = parser.parse_args()

    raw_dl = Path(args.deeplink_map)
    deeplink_map_path = raw_dl if raw_dl.is_absolute() else (script_dir / raw_dl).resolve()
    if not deeplink_map_path.exists():
        print(f"[WARNING] Deeplink map not found: {deeplink_map_path} -- deeplink columns will be skipped.")
        deeplink_map_path = None

    # Resolve output directories to process.
    if args.output_dir:
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = (script_dir / output_dir).resolve()
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
        base = (script_dir / args.output_base).resolve()
        output_dirs = [base / f"{d.strftime('%d%m%Y')}_{s}" for d in dates for s in slots]

    for output_dir in output_dirs:
        if not output_dir.exists():
            print(f"[INFO] Skipping {output_dir.name} -- directory not found.")
            continue
        print(f"Preparing campaign content for: {output_dir.name}")
        if deeplink_map_path:
            print(f"Deeplink map : {deeplink_map_path}")
        print()
        try:
            prepare_content(output_dir, deeplink_map_path)
        except (FileNotFoundError, ValueError) as exc:
            print(f"[ERROR] {output_dir.name}: {exc}")


if __name__ == "__main__":
    main()
