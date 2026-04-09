"""Backfill missing outputs/log/summary/DDMMYYYY_slot.csv files.

Builds log summaries with columns:
    date, priority, utm_campaign, title_template, content_template, final_count

Data sources per slot directory (outputs/DDMMYYYY_slot):
- campaign_meta.csv for title_template/content_template
- payload CSV files for utm_campaign and final_count

Usage:
    python temp_backfill_summary_logs.py
    python temp_backfill_summary_logs.py --overwrite
"""

import argparse
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd


def extract_utm_campaign(url: str) -> str:
    """Extract utm_campaign query parameter from a URL."""
    if not url:
        return ""
    utm_values = parse_qs(urlparse(url).query).get("utm_campaign", [""])
    return utm_values[0]


def load_campaign_meta(slot_dir: Path) -> pd.DataFrame:
    meta_path = slot_dir / "campaign_meta.csv"
    if not meta_path.exists():
        raise FileNotFoundError(f"campaign_meta.csv not found: {meta_path}")

    meta_df = pd.read_csv(meta_path, dtype=str, keep_default_na=False)
    required = {"priority", "title_template", "content_template"}
    missing = required - set(meta_df.columns)
    if missing:
        raise ValueError(f"campaign_meta.csv is missing columns: {sorted(missing)}")
    return meta_df


def load_final_counts(slot_dir: Path) -> dict[int, int]:
    summary_path = slot_dir / "summary.csv"
    if summary_path.exists():
        summary_df = pd.read_csv(summary_path, dtype=str, keep_default_na=False)
        if {"priority", "final_count"}.issubset(summary_df.columns):
            counts: dict[int, int] = {}
            for _, row in summary_df.iterrows():
                try:
                    priority = int(str(row.get("priority", "")).strip())
                    final_count = int(str(row.get("final_count", "0")).strip() or "0")
                except ValueError:
                    continue
                counts[priority] = final_count
            if counts:
                return counts

    counts: dict[int, int] = {}
    for csv_path in sorted(slot_dir.glob("[0-9][0-9]_*.csv")):
        try:
            priority = int(csv_path.stem.split("_", 1)[0])
        except ValueError:
            continue
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
            counts[priority] = len(df)
        except Exception:
            counts[priority] = 0
    return counts


def build_slot_summary(slot_dir: Path) -> pd.DataFrame:
    """Build summary from campaign_meta.csv plus enriched payload files."""
    meta_df = load_campaign_meta(slot_dir)
    final_counts = load_final_counts(slot_dir)

    date_part, slot = slot_dir.name.split("_", 1)
    run_date = datetime.strptime(date_part, "%d%m%Y")
    date_value = run_date.strftime("%d/%m/%Y")

    rows: list[dict[str, int | str]] = []

    for _, row in meta_df.iterrows():
        try:
            priority = int(str(row.get("priority", "")).strip())
        except ValueError:
            continue

        payload_path = None
        for csv_path in sorted(slot_dir.glob(f"{priority:02d}_*.csv")):
            payload_path = csv_path
            break

        utm_campaign = ""
        if payload_path is not None and payload_path.exists():
            try:
                payload_df = pd.read_csv(payload_path, dtype=str, keep_default_na=False)
            except Exception:
                payload_df = pd.DataFrame()
            for deeplink_col in ("android_deeplink", "ios_deeplink"):
                if deeplink_col in payload_df.columns and len(payload_df) > 0:
                    utm_campaign = extract_utm_campaign(str(payload_df[deeplink_col].iloc[0]).strip())
                    if utm_campaign:
                        break

        if not utm_campaign:
            # Fallback for payload files without deeplink columns.
            utm_campaign = ""

        rows.append(
            {
                "date": date_value,
                "priority": priority,
                "utm_campaign": utm_campaign,
                "title_template": str(row.get("title_template", "")).strip(),
                "content_template": str(row.get("content_template", "")).strip(),
                "final_count": int(final_counts.get(priority, 0)),
            }
        )

    result = pd.DataFrame(
        rows,
        columns=["date", "priority", "utm_campaign", "title_template", "content_template", "final_count"],
    )
    return result.sort_values(by="priority").reset_index(drop=True)


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


def main() -> None:
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(
        description="Backfill missing log summary CSV files using campaign_meta and payload files."
    )
    parser.add_argument("--output-base", default="outputs", help="Base output folder (default: outputs)")
    parser.add_argument(
        "--summary-dir",
        default="outputs/log/summary",
        help="Destination summary folder (default: outputs/log/summary)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing summary files")
    args = parser.parse_args()

    output_base = (script_dir / args.output_base).resolve()
    summary_dir = (script_dir / args.summary_dir).resolve()

    if not output_base.exists():
        raise FileNotFoundError(f"Output base not found: {output_base}")

    summary_dir.mkdir(parents=True, exist_ok=True)

    slot_dirs = discover_slot_dirs(output_base)
    if not slot_dirs:
        print("No slot directories found to process.")
        return

    created = 0
    skipped = 0
    failed = 0

    for slot_dir in slot_dirs:
        target_path = summary_dir / f"{slot_dir.name}.csv"
        if target_path.exists() and not args.overwrite:
            skipped += 1
            print(f"[SKIP] Exists: {target_path.name}")
            continue

        try:
            summary_df = build_slot_summary(slot_dir)
            summary_df.to_csv(target_path, index=False)
            created += 1
            print(f"[OK]   Wrote: {target_path.name} ({len(summary_df)} row(s))")
        except Exception as exc:
            failed += 1
            print(f"[FAIL] {slot_dir.name}: {exc}")

    print()
    print(f"Done. created={created}, skipped={skipped}, failed={failed}")


if __name__ == "__main__":
    main()
