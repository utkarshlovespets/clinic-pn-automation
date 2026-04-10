from __future__ import annotations

import csv
import re
from pathlib import Path


CAMPAIGN_SUFFIX_PATTERN = re.compile(r"_MP_[^_]+_(.+)$")


def extract_campaign_suffix(utm_campaign: str) -> str:
    match = CAMPAIGN_SUFFIX_PATTERN.search(utm_campaign)
    if match:
        return match.group(1).strip()
    return utm_campaign.strip()


def build_utm_campaign_csv(base_dir: Path) -> Path:
    summary_dir = base_dir / "outputs" / "log" / "summary"
    output_file = base_dir / "data" / "utm_campaign.csv"

    if not summary_dir.exists():
        raise FileNotFoundError(f"Summary folder not found: {summary_dir}")

    unique_campaigns: set[str] = set()

    for csv_file in sorted(summary_dir.glob("*.csv")):
        with csv_file.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames or "utm_campaign" not in reader.fieldnames:
                continue

            for row in reader:
                value = (row.get("utm_campaign") or "").strip()
                if value:
                    unique_campaigns.add(extract_campaign_suffix(value))

    with output_file.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["utm_campaign"])
        for campaign in sorted(unique_campaigns):
            writer.writerow([campaign])

    return output_file


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent.parent
    output_path = build_utm_campaign_csv(repo_root)
    print(f"Created: {output_path}")