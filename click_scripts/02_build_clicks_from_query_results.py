from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path


QUERY_RESULT_PATTERN = re.compile(r"^(\d{8})_utm_campaign\.csv$")


def load_allowed_suffixes(campaign_file: Path) -> list[str]:
    with campaign_file.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames or "utm_campaign" not in reader.fieldnames:
            raise ValueError(f"Missing utm_campaign column in {campaign_file}")

        suffixes = {
            (row.get("utm_campaign") or "").strip()
            for row in reader
            if (row.get("utm_campaign") or "").strip()
        }

    # Prefer longer suffixes first in case one suffix is a tail of another.
    return sorted(suffixes, key=len, reverse=True)


def parse_int(value: str | None) -> int:
    text = (value or "").strip()
    if not text:
        return 0

    try:
        return int(float(text))
    except ValueError:
        return 0


def matched_suffix(campaign: str, allowed_suffixes: list[str]) -> str | None:
    cleaned = campaign.strip()
    if not cleaned:
        return None

    for suffix in allowed_suffixes:
        if cleaned.endswith(suffix):
            return suffix
    return None


def matches_allowed_suffix(campaign: str, allowed_suffixes: list[str]) -> bool:
    return matched_suffix(campaign, allowed_suffixes) is not None


def process_query_result_file(
    input_file: Path,
    output_file: Path,
    allowed_suffixes: list[str],
) -> int:
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    with input_file.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        required_columns = {"utm_campaign", "push_impressions", "utm_visited"}
        if not reader.fieldnames or not required_columns.issubset(set(reader.fieldnames)):
            raise ValueError(f"Missing required columns in {input_file}")

        for row in reader:
            full_campaign = (row.get("utm_campaign") or "").strip()
            if not matches_allowed_suffix(full_campaign, allowed_suffixes):
                continue

            totals[full_campaign][0] += parse_int(row.get("push_impressions"))
            totals[full_campaign][1] += parse_int(row.get("utm_visited"))

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["utm_campaign", "push_impressions", "utm_visited"],
        )
        writer.writeheader()
        for campaign in sorted(totals):
            push_impressions, utm_visited = totals[campaign]
            writer.writerow(
                {
                    "utm_campaign": campaign,
                    "push_impressions": push_impressions,
                    "utm_visited": utm_visited,
                }
            )

    return len(totals)


def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    campaign_file = base_dir / "data" / "utm_campaign.csv"
    query_results_dir = base_dir / "data" / "bigquery_results"
    clicks_dir = base_dir / "outputs" / "log" / "clicks"

    if not campaign_file.exists():
        raise FileNotFoundError(f"Campaign file not found: {campaign_file}")
    if not query_results_dir.exists():
        raise FileNotFoundError(f"Query results folder not found: {query_results_dir}")

    allowed_suffixes = load_allowed_suffixes(campaign_file)

    for input_file in sorted(query_results_dir.glob("*_utm_campaign.csv")):
        match = QUERY_RESULT_PATTERN.match(input_file.name)
        if not match:
            continue

        date_ddmmyyyy = match.group(1)
        output_file = clicks_dir / f"{date_ddmmyyyy}_clicks.csv"
        written_rows = process_query_result_file(input_file, output_file, allowed_suffixes)
        print(f"Saved {written_rows} rows to {output_file}")


if __name__ == "__main__":
    main()