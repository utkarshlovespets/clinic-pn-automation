"""Backfill campaign_meta.csv with cohort/exclusion/final metrics.

For each slot directory in outputs (DDMMYYYY_morning/evening), this script reads:
- campaign_meta.csv
- summary.csv

Then it writes/updates these columns in campaign_meta.csv:
- cohort_size
- excluded_by_priority
- excluded_by_exclusion_col
- final_count

Usage:
    python temp_backfill_campaign_meta_metrics.py
"""

import argparse
from pathlib import Path

import pandas as pd


def discover_slot_dirs(output_base: Path) -> list[Path]:
    slot_dirs: list[Path] = []
    for path in sorted(output_base.iterdir()):
        if not path.is_dir() or path.name == "log":
            continue
        parts = path.name.split("_", 1)
        if len(parts) != 2:
            continue
        date_part, slot = parts
        if len(date_part) != 8 or not date_part.isdigit():
            continue
        if slot not in {"morning", "evening"}:
            continue
        slot_dirs.append(path)
    return slot_dirs


def to_int_safe(value: object) -> int:
    try:
        text = str(value).strip()
        return int(text) if text else 0
    except (TypeError, ValueError):
        return 0


def backfill_one_slot(slot_dir: Path) -> tuple[bool, str]:
    meta_path = slot_dir / "campaign_meta.csv"
    summary_path = slot_dir / "summary.csv"
    metric_cols = [
        "cohort_size",
        "excluded_by_priority",
        "excluded_by_exclusion_col",
        "final_count",
    ]

    if not meta_path.exists():
        return False, f"campaign_meta.csv not found"
    if not summary_path.exists():
        meta_df = pd.read_csv(meta_path, dtype=str, keep_default_na=False)
        if all(col in meta_df.columns for col in metric_cols):
            return True, "already_has_metric_columns"
        return False, f"summary.csv not found"

    meta_df = pd.read_csv(meta_path, dtype=str, keep_default_na=False)
    summary_df = pd.read_csv(summary_path, dtype=str, keep_default_na=False)

    required_meta = {"priority", "cohort_name", "title_template", "content_template"}
    missing_meta = required_meta - set(meta_df.columns)
    if missing_meta:
        return False, f"campaign_meta missing columns: {sorted(missing_meta)}"

    required_summary = {
        "priority",
        "cohort_name",
        "input_candidates",
        "excluded_by_priority",
        "excluded_by_exclusion_col",
        "final_count",
    }
    missing_summary = required_summary - set(summary_df.columns)
    if missing_summary:
        return False, f"summary missing columns: {sorted(missing_summary)}"

    # Prefer matching by priority. If duplicates exist, first one wins.
    summary_by_priority: dict[int, dict[str, int]] = {}
    for _, row in summary_df.iterrows():
        p = to_int_safe(row.get("priority", ""))
        if p <= 0 or p in summary_by_priority:
            continue
        summary_by_priority[p] = {
            "cohort_size": to_int_safe(row.get("input_candidates", "0")),
            "excluded_by_priority": to_int_safe(row.get("excluded_by_priority", "0")),
            "excluded_by_exclusion_col": to_int_safe(row.get("excluded_by_exclusion_col", "0")),
            "final_count": to_int_safe(row.get("final_count", "0")),
        }

    for col in metric_cols:
        if col not in meta_df.columns:
            meta_df[col] = ""

    updated_rows = 0
    for idx, row in meta_df.iterrows():
        p = to_int_safe(row.get("priority", ""))
        metrics = summary_by_priority.get(p)
        if not metrics:
            continue
        for col in metric_cols:
            meta_df.at[idx, col] = str(metrics[col])
        updated_rows += 1

    meta_df.to_csv(meta_path, index=False)
    return True, f"updated_rows={updated_rows}"


def main() -> None:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description="Backfill campaign_meta.csv metrics from summary.csv for all slot folders."
    )
    parser.add_argument(
        "--output-base",
        default="outputs",
        help="Base output folder containing DDMMYYYY_slot directories (default: outputs)",
    )
    args = parser.parse_args()

    output_base = (script_dir / args.output_base).resolve()
    if not output_base.exists():
        raise FileNotFoundError(f"Output base not found: {output_base}")

    slot_dirs = discover_slot_dirs(output_base)
    if not slot_dirs:
        print("No slot directories found.")
        return

    success = 0
    failed = 0

    for slot_dir in slot_dirs:
        ok, msg = backfill_one_slot(slot_dir)
        if ok:
            success += 1
            print(f"[OK]   {slot_dir.name}: {msg}")
        else:
            failed += 1
            print(f"[SKIP] {slot_dir.name}: {msg}")

    print()
    print(f"Done. success={success}, skipped_or_failed={failed}")


if __name__ == "__main__":
    main()
