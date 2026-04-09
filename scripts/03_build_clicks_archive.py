from __future__ import annotations

import csv
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path


MONTH_ORDER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

CLICK_FILE_PATTERN = re.compile(r"^(\d{8})_clicks\.csv$")
DATE_PREFIX_PATTERN = re.compile(r"^(\d{1,2})([A-Za-z]+)")


def parse_int(value: str | None) -> int:
    text = (value or "").strip()
    if not text:
        return 0

    try:
        return int(float(text))
    except ValueError:
        return 0


def load_start_date() -> datetime | None:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return None

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("#") or not line or "=" not in line:
            continue

        key, value = line.split("=", 1)
        if key.strip() == "START_DATE":
            date_str = value.strip().strip('"')
            try:
                return datetime.strptime(date_str, "%d/%m/%Y")
            except ValueError:
                return None

    return None


def load_campaign_id() -> str:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return ""

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("#") or not line or "=" not in line:
            continue

        key, value = line.split("=", 1)
        if key.strip() == "CAMPAIGN_ID":
            return value.strip().strip('"')

    return ""


def parse_date(date_str: str) -> datetime | None:
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y")
    except ValueError:
        return None


def calculate_ctr(utm_visited: int, push_impressions: int) -> str:
    if push_impressions == 0:
        return "0.00"
    ctr = (utm_visited / push_impressions) * 100
    return f"{ctr:.2f}"


def parse_source_date(file_name: str) -> datetime:
    match = CLICK_FILE_PATTERN.match(file_name)
    if not match:
        raise ValueError(f"Invalid clicks filename: {file_name}")

    return datetime.strptime(match.group(1), "%d%m%Y")


def load_summary_metadata(summary_dir: Path) -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}

    for input_file in sorted(summary_dir.glob("*.csv")):
        with input_file.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            required_columns = {
                "utm_campaign",
                "final_count",
                "date",
                "title_template",
                "content_template",
            }
            if not reader.fieldnames or not required_columns.issubset(set(reader.fieldnames)):
                raise ValueError(f"Missing required columns in {input_file}")

            for row in reader:
                campaign = (row.get("utm_campaign") or "").strip()
                if not campaign:
                    continue

                metadata[campaign] = {
                    "date": (row.get("date") or "").strip(),
                    "slot": (row.get("slot") or "").strip(),
                    "title_template": (row.get("title_template") or "").strip(),
                    "content_template": (row.get("content_template") or "").strip(),
                    "cohort_size": str(parse_int(row.get("final_count"))),
                }

    return metadata


def extract_campaign_sort_key(utm_campaign: str) -> tuple[int, int, str]:
    cleaned = utm_campaign.strip()
    match = DATE_PREFIX_PATTERN.match(cleaned)
    if not match:
        return (9999, 99, cleaned)

    day = int(match.group(1))
    month_name = match.group(2).lower()
    month = MONTH_ORDER.get(month_name, 99)
    return (month, day, cleaned)


def aggregate_clicks(clicks_dir: Path) -> dict[str, tuple[int, int, str]]:
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    latest_updates: dict[str, datetime] = {}

    for input_file in sorted(clicks_dir.glob("*_clicks.csv")):
        source_date = parse_source_date(input_file.name)

        with input_file.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            required_columns = {"utm_campaign", "push_impressions", "utm_visited"}
            if not reader.fieldnames or not required_columns.issubset(set(reader.fieldnames)):
                raise ValueError(f"Missing required columns in {input_file}")

            for row in reader:
                campaign = (row.get("utm_campaign") or "").strip()
                if not campaign:
                    continue

                totals[campaign][0] += parse_int(row.get("push_impressions"))
                totals[campaign][1] += parse_int(row.get("utm_visited"))
                current_update = latest_updates.get(campaign)
                if current_update is None or source_date > current_update:
                    latest_updates[campaign] = source_date

    archive_rows: dict[str, tuple[int, int, str]] = {}
    for campaign, values in totals.items():
        archive_rows[campaign] = (
            values[0],
            values[1],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

    return archive_rows


def write_archive(
    output_file: Path,
    totals: dict[str, tuple[int, int, str]],
    metadata: dict[str, dict[str, str]],
    start_date: datetime | None = None,
    campaign_id: str = "",
) -> None:
    sorted_campaigns = sorted(totals, key=extract_campaign_sort_key)

    # Use UTF-8 BOM so spreadsheet tools on Windows reliably preserve emoji text.
    with output_file.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "date",
                "slot",
                "utm_campaign",
                "campaign_id",
                "title_template",
                "content_template",
                "cohort_size",
                "push_impressions",
                "utm_visited",
                "ctr%",
                "last_updated",
            ],
        )
        writer.writeheader()
        for campaign in sorted_campaigns:
            push_impressions, utm_visited, last_updated = totals[campaign]
            meta = metadata.get(campaign, {})
            date_str = meta.get("date", "")
            if not date_str:
                continue

            campaign_date = parse_date(date_str)
            if start_date and campaign_date:
                if campaign_date < start_date:
                    continue

            writer.writerow(
                {
                    "date": date_str,
                    "slot": meta.get("slot", ""),
                    "utm_campaign": campaign,
                    "campaign_id": campaign_id,
                    "title_template": meta.get("title_template", ""),
                    "content_template": meta.get("content_template", ""),
                    "cohort_size": meta.get("cohort_size", ""),
                    "push_impressions": push_impressions,
                    "utm_visited": utm_visited,
                    "ctr%": calculate_ctr(utm_visited, push_impressions),
                    "last_updated": last_updated,
                }
            )


def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    clicks_dir = base_dir / "data" / "clicks"
    summary_dir = base_dir / "outputs" / "log" / "summary"
    output_file = base_dir / "data" / "clicks_archive.csv"

    if not clicks_dir.exists():
        raise FileNotFoundError(f"Clicks folder not found: {clicks_dir}")
    if not summary_dir.exists():
        raise FileNotFoundError(f"Summary folder not found: {summary_dir}")

    start_date = load_start_date()
    campaign_id = load_campaign_id()
    metadata = load_summary_metadata(summary_dir)
    totals = aggregate_clicks(clicks_dir)
    write_archive(output_file, totals, metadata, start_date, campaign_id)
    if start_date:
        print(f"Filtered rows by start date: {start_date.strftime('%d/%m/%Y')}")
    if campaign_id:
        print(f"Campaign ID: {campaign_id}")
    print(f"Saved {len(totals)} rows to {output_file}")


if __name__ == "__main__":
    main()