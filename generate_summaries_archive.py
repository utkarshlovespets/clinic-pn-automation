"""Generate summaries_archive.csv from clinic_mastersheet.csv.

Creates a flat archive of scheduled push notifications with:
date, day, utm name, campaign id, title, message

Usage:
    python generate_summaries_archive.py
    python generate_summaries_archive.py --clinic-csv data/clinic_mastersheet.csv \
        --deeplink-map data/deeplink_map.csv --output summaries_archive.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

from utils import normalize_cohort


def build_deeplink(template: str, date_token: str, priority_token: str) -> str:
    """Resolve known placeholders in deeplink URL templates."""
    if not template:
        return ""
    return template.replace("{date}", date_token).replace("{priority}", priority_token)


def extract_utm_name(primary_url: str, fallback_url: str) -> str:
    """Extract utm_campaign (or utm_name) from one of the deeplink URLs."""
    for candidate in (primary_url, fallback_url):
        if not candidate:
            continue
        parsed = urlparse(candidate if "://" in candidate else f"https://{candidate}")
        query = parse_qs(parsed.query)
        for key in ("utm_campaign", "utm_name"):
            values = query.get(key, [])
            if values and values[0]:
                return values[0]
    return ""


def load_deeplink_lookup(path: Path) -> dict[str, tuple[str, str, str]]:
    """Return normalized cohort -> (campaign_id, android_base_url, ios_base_url)."""
    if not path.exists():
        return {}

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"Cohort Name", "campaign_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"deeplink_map is missing columns: {sorted(missing)}")

    lookup: dict[str, tuple[str, str, str]] = {}
    for _, row in df.iterrows():
        key = normalize_cohort(str(row.get("Cohort Name", "")).strip())
        if not key:
            continue
        lookup[key] = (
            str(row.get("campaign_id", "")).strip(),
            str(row.get("android_base_url", "")).strip(),
            str(row.get("ios_base_url", "")).strip(),
        )
    return lookup


def generate_archive(clinic_csv: Path, deeplink_map: Path, output_csv: Path) -> int:
    """Build summaries archive and save it. Returns number of written rows."""
    if not clinic_csv.exists():
        raise FileNotFoundError(f"clinic_mastersheet not found: {clinic_csv}")

    df = pd.read_csv(clinic_csv, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    required = {"Date", "Cohort Name", "Title", "Content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"clinic_mastersheet is missing columns: {sorted(missing)}")

    if "Day" not in df.columns:
        df["Day"] = ""
    if "Slot" not in df.columns:
        df["Slot"] = ""

    df["_row_order"] = range(len(df))
    df["_date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    df["_slot"] = df["Slot"].fillna("").str.strip().str.lower()
    df["_cohort_name"] = df["Cohort Name"].fillna("").str.strip()
    df["_title"] = df["Title"].fillna("").str.strip()
    df["_message"] = df["Content"].fillna("").str.strip()

    scheduled = df[
        df["_date"].notna()
        & df["_cohort_name"].ne("")
        & df["_title"].ne("")
        & df["_message"].ne("")
    ].copy()

    if scheduled.empty:
        archive = pd.DataFrame(
            columns=["date", "day", "utm name", "campaign id", "title", "message"]
        )
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        archive.to_csv(output_csv, index=False, encoding="utf-8-sig")
        return 0

    scheduled = scheduled.sort_values(["_date", "_slot", "_row_order"])
    scheduled["_priority"] = (
        scheduled.groupby(["_date", "_slot"], sort=False).cumcount() + 1
    )
    scheduled["_priority_token"] = scheduled.apply(
        lambda row: f"{int(row['_priority'])}{'E' if row['_slot'] == 'evening' else 'M'}",
        axis=1,
    )
    scheduled["_date_token"] = scheduled["_date"].dt.strftime("%d%B")
    scheduled["_cohort_key"] = scheduled["_cohort_name"].apply(normalize_cohort)

    deeplink_lookup = load_deeplink_lookup(deeplink_map)

    def map_campaign_and_utm(row: pd.Series) -> tuple[str, str]:
        campaign_id, android_tpl, ios_tpl = deeplink_lookup.get(
            row["_cohort_key"], ("", "", "")
        )
        android_url = build_deeplink(
            android_tpl, row["_date_token"], row["_priority_token"]
        )
        ios_url = build_deeplink(ios_tpl, row["_date_token"], row["_priority_token"])
        utm_name = extract_utm_name(android_url, ios_url)
        return campaign_id, utm_name

    mapped = scheduled.apply(map_campaign_and_utm, axis=1, result_type="expand")
    scheduled["_campaign_id"] = mapped[0].fillna("")
    scheduled["_utm_name"] = mapped[1].fillna("")

    day_from_date = scheduled["_date"].dt.strftime("%a")
    scheduled["_day"] = scheduled["Day"].fillna("").str.strip()
    scheduled["_day"] = scheduled["_day"].where(scheduled["_day"].ne(""), day_from_date)

    archive = pd.DataFrame(
        {
            "date": scheduled["_date"].dt.strftime("%d/%m/%Y"),
            "day": scheduled["_day"],
            "utm name": scheduled["_utm_name"],
            "campaign id": scheduled["_campaign_id"],
            "title": scheduled["_title"],
            "message": scheduled["_message"],
        }
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    archive.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return len(archive)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate summaries_archive.csv from clinic_mastersheet.csv."
    )
    parser.add_argument(
        "--clinic-csv",
        default="data/clinic_mastersheet.csv",
        help="Path to clinic_mastersheet.csv (default: data/clinic_mastersheet.csv).",
    )
    parser.add_argument(
        "--deeplink-map",
        default="data/deeplink_map.csv",
        help="Path to deeplink_map.csv (default: data/deeplink_map.csv).",
    )
    parser.add_argument(
        "--output",
        default="summaries_archive.csv",
        help="Output CSV path (default: summaries_archive.csv).",
    )
    parser.add_argument(
        "--start-date",
        default="15/04/2026",
        help="Include rows on/after this date in DD/MM/YYYY format (default: 15/04/2026).",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    clinic_csv = Path(args.clinic_csv)
    deeplink_map = Path(args.deeplink_map)
    output_csv = Path(args.output)

    if not clinic_csv.is_absolute():
        clinic_csv = (script_dir / clinic_csv).resolve()
    if not deeplink_map.is_absolute():
        deeplink_map = (script_dir / deeplink_map).resolve()
    if not output_csv.is_absolute():
        output_csv = (script_dir / output_csv).resolve()

    start_date = pd.to_datetime(args.start_date, format="%d/%m/%Y", errors="coerce")
    if pd.isna(start_date):
        raise ValueError(
            f"Invalid --start-date: {args.start_date!r}. Use DD/MM/YYYY format."
        )

    row_count = generate_archive(clinic_csv, deeplink_map, output_csv)
    if row_count:
        archive_df = pd.read_csv(output_csv, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        archive_df["_date"] = pd.to_datetime(archive_df["date"], format="%d/%m/%Y", errors="coerce")
        archive_df = archive_df[archive_df["_date"].notna() & archive_df["_date"].ge(start_date)]
        archive_df = archive_df.drop(columns=["_date"])
        archive_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        row_count = len(archive_df)

    print(
        f"[OK] Wrote {row_count} row(s) to {output_csv} "
        f"(start date filter: {start_date.strftime('%d/%m/%Y')})"
    )


if __name__ == "__main__":
    main()
