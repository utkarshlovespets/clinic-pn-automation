"""
Stage 5 -- Append campaign summaries to database and log file.

This script runs automatically after each campaign run via run_campaign.py.
It:
1. Loads summary CSVs from the most recent date only
2. Deletes existing entries in DB for that date (overwrite)
3. Inserts new summary data to Neon database
4. Updates campaign_summary.log with the current run date
5. Runs during both dry-run and live mode

Usage:
    python 05_append_summaries.py --date DDMMYYYY
    python 05_append_summaries.py  # uses today's date
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from psycopg import connect
from psycopg.sql import Identifier

SUMMARY_DIR = "outputs/log/summary"
COLUMNS = ['date', 'slot', 'priority', 'utm_campaign', 'title_template', 'content_template', 'final_count']
SUMMARY_LOG = "campaign_summary.log"


def get_project_root() -> Path:
    """Get project root (parent of campaign_scripts)."""
    return Path(__file__).resolve().parent.parent


def parse_env_file(env_path: Path) -> dict[str, str]:
    """Parse .env file and return key-value pairs."""
    settings = {}
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    settings[key.strip()] = value.strip()
    return settings


def parse_automation_tables(raw: str) -> list[tuple[str, str]]:
    """Parse AUTOMATION_TABLES into list of (automation, table_name)."""
    mappings = []
    for chunk in re.split(r'[,;\n]+', raw):
        piece = chunk.strip()
        if not piece or '=' not in piece:
            continue
        automation, table_name = piece.split('=', 1)
        normalized = re.sub(r'[^a-zA-Z0-9]+', '_', automation).strip('_').lower()
        mappings.append((normalized, table_name.strip()))
    return mappings


def get_settings() -> dict[str, str]:
    """Load settings from .env file."""
    file_values = parse_env_file(get_project_root() / ".env")
    settings = file_values.copy()
    for key, value in os.environ.items():
        settings[key] = value
    return settings


def resolve_automations() -> list[tuple[str, str]]:
    """Resolve automation to table mappings from settings."""
    settings = get_settings()
    mapping_raw = settings.get("AUTOMATION_TABLES", "").strip()
    if mapping_raw:
        return parse_automation_tables(mapping_raw)
    return []


def parse_date(date_str: str) -> datetime:
    """Parse DDMMYYYY date string."""
    try:
        return datetime.strptime(date_str, "%d%m%Y")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Date must be in DDMMYYYY format, got: {date_str!r}"
        )


def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract DDMMYYYY date from filename like 14042026_morning.csv."""
    match = re.match(r'^(\d{8})', filename)
    if match:
        return match.group(1)
    return None


def get_all_summary_dates() -> dict[str, list[Path]]:
    """Get all unique dates from summary files, mapped to their CSV files."""
    summary_path = get_project_root() / SUMMARY_DIR
    if not summary_path.exists():
        return {}

    csv_files = list(summary_path.glob("*.csv"))
    if not csv_files:
        return {}

    date_files = {}
    for csv_file in csv_files:
        date_str = extract_date_from_filename(csv_file.name)
        if date_str is None:
            continue
        if date_str not in date_files:
            date_files[date_str] = []
        date_files[date_str].append(csv_file)

    return date_files


def get_db_dates() -> set[str]:
    """Get all unique dates currently in the database."""
    settings = get_settings()
    database_url = settings.get("NEON_DATABASE_URL", "").strip()
    if not database_url:
        return set()

    automations = resolve_automations()
    if not automations:
        return set()

    db_dates = set()
    try:
        with connect(database_url) as conn:
            with conn.cursor() as cursor:
                for _, table_name in automations:
                    cursor.execute(f"SELECT DISTINCT date FROM public.{Identifier(table_name).as_string()}")
                    for row in cursor.fetchall():
                        if row[0]:
                            db_dates.add(str(row[0]).strip())
    except Exception as e:
        print(f"[WARNING] Could not fetch DB dates: {e}")

    return db_dates


def sync_all_summaries_to_db() -> dict[str, int]:
    """Sync all summary files to database, returning {date: row_count} for processed dates."""
    summary_dates = get_all_summary_dates()
    db_dates = get_db_dates()

    summary_dates_formatted = {}
    for date_str, files in summary_dates.items():
        if files:
            try:
                df = pd.read_csv(files[0])
                if 'date' in df.columns and not df.empty:
                    db_date = str(df['date'].iloc[0]).strip()
                    summary_dates_formatted[db_date] = files
            except:
                continue

    missing_dates = set(summary_dates_formatted.keys()) - db_dates
    print(f"[INFO] Found {len(missing_dates)} missing date(s) in database")

    processed = {}
    for db_date in missing_dates:
        files = summary_dates_formatted.get(db_date, [])
        for csv_path in files:
            print(f"[INFO] Inserting missing date: {db_date} from {csv_path.name}")
            try:
                df = pd.read_csv(csv_path)
                row_count = insert_single_csv(csv_path, db_date)
                processed[db_date] = row_count
            except Exception as e:
                print(f"[ERROR] Failed to insert {csv_path.name}: {e}")

    return processed


def insert_single_csv(csv_path: Path, db_date: str) -> int:
    """Insert a single CSV file's data into database, overwriting existing entries for that date."""
    settings = get_settings()
    database_url = settings.get("NEON_DATABASE_URL", "").strip()
    if not database_url:
        return 0

    df = pd.read_csv(csv_path)
    if 'utm_campaign' not in df.columns:
        return 0

    automations = resolve_automations()
    total_inserted = 0

    for automation_name, table_name in automations:
        table_columns = []
        try:
            with connect(database_url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
                        (table_name,)
                    )
                    table_columns = {row[0] for row in cursor.fetchall()}
        except:
            continue

        insert_columns = ['utm_campaign', 'date', 'slot', 'title_template', 'content_template', 'final_count']
        insert_columns = [c for c in insert_columns if c in table_columns]

        if 'utm_campaign' not in insert_columns:
            continue

        try:
            with connect(database_url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM public.{Identifier(table_name).as_string()} WHERE date = %s",
                        (db_date,)
                    )

                    df_to_insert = pd.DataFrame()
                    for col in insert_columns:
                        if col in df.columns:
                            df_to_insert[col] = df[col].fillna('').astype(str)
                        else:
                            df_to_insert[col] = ''

                    for _, row in df_to_insert.iterrows():
                        values = [row.get(col, '') for col in insert_columns]
                        placeholders = ', '.join(['%s'] * len(insert_columns))
                        col_names = ', '.join(Identifier(col).as_string() for col in insert_columns)
                        sql = f"INSERT INTO public.{Identifier(table_name).as_string()} ({col_names}) VALUES ({placeholders})"
                        cursor.execute(sql, values)

                    total_inserted = len(df_to_insert)
                conn.commit()
        except Exception as e:
            print(f"[ERROR] Insert failed for {table_name}: {e}")

    return total_inserted


def insert_summaries_to_neon(csv_path: Path) -> None:
    """Insert summary data into Neon database tables, overwriting existing entries for the date."""
    settings = get_settings()
    database_url = settings.get("NEON_DATABASE_URL", "").strip()
    if not database_url:
        print("[WARNING] NEON_DATABASE_URL not found in .env, skipping database insert")
        return

    df = pd.read_csv(csv_path)
    if 'utm_campaign' not in df.columns:
        print(f"[ERROR] utm_campaign column not found in {csv_path}")
        return

    if 'date' not in df.columns or df.empty:
        print(f"[ERROR] No date column or empty data in {csv_path}")
        return

    db_date = str(df['date'].iloc[0]).strip()
    print(f"[INFO] Processing date: {db_date}")

    automations = resolve_automations()
    print(f"[INFO] Found automations: {[a[0] for a in automations]}")

    for automation_name, table_name in automations:
        print(f"[INFO] Processing {automation_name} -> {table_name}")

        table_columns = []
        try:
            with connect(database_url) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
                        (table_name,)
                    )
                    table_columns = {row[0] for row in cursor.fetchall()}
        except Exception as e:
            print(f"[ERROR] Could not fetch columns for {table_name}: {e}")
            continue

        insert_columns = ['utm_campaign', 'date', 'slot', 'title_template', 'content_template', 'final_count']
        insert_columns = [c for c in insert_columns if c in table_columns]

        if 'utm_campaign' not in insert_columns:
            print(f"  Skipping {table_name}: missing utm_campaign column")
            continue

        date_col = 'date' if 'date' in table_columns else None
        try:
            with connect(database_url) as conn:
                with conn.cursor() as cursor:
                    if date_col:
                        cursor.execute(
                            f"DELETE FROM public.{Identifier(table_name).as_string()} WHERE {Identifier(date_col).as_string()} = %s",
                            (db_date,)
                        )
                        print(f"  Deleted existing entries for date {db_date}")

                    df_to_insert = pd.DataFrame()
                    for col in insert_columns:
                        if col in df.columns:
                            df_to_insert[col] = df[col].fillna('').astype(str)
                        else:
                            df_to_insert[col] = ''

                    for _, row in df_to_insert.iterrows():
                        values = [row.get(col, '') for col in insert_columns]
                        placeholders = ', '.join(['%s'] * len(insert_columns))
                        col_names = ', '.join(Identifier(col).as_string() for col in insert_columns)
                        sql = f"INSERT INTO public.{Identifier(table_name).as_string()} ({col_names}) VALUES ({placeholders})"
                        try:
                            cursor.execute(sql, values)
                        except Exception as e:
                            print(f"  Error inserting row: {e}")

                conn.commit()
            print(f"  Inserted {len(df_to_insert)} rows into {table_name}")
        except Exception as e:
            print(f"[ERROR] Database insert failed for {table_name}: {e}")


def load_summary_log() -> dict[str, str]:
    """Load existing summary log entries as {date: entry}."""
    log_path = get_project_root() / SUMMARY_LOG
    entries = {}
    if log_path.exists():
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or ':' not in line:
                        continue
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        date_part = parts[0].strip()
                        try:
                            datetime.strptime(date_part, "%Y-%m-%d")
                            entries[date_part] = parts[1].strip()
                        except ValueError:
                            continue
        except Exception as e:
            print(f"[WARNING] Could not read summary log: {e}")
    return entries


def save_summary_log(entries: dict[str, str]) -> None:
    """Save summary log entries, sorted by date descending."""
    log_path = get_project_root() / SUMMARY_LOG
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, 'w', encoding='utf-8') as f:
            for date_str in sorted(entries.keys(), reverse=True):
                f.write(f"{date_str}: {entries[date_str]}\n")
        print(f"[INFO] Updated {SUMMARY_LOG}")
    except Exception as e:
        print(f"[ERROR] Could not write summary log: {e}")


def fill_missing_dates(entries: dict[str, str]) -> None:
    """Fill in missing dates between earliest entry and today with placeholder."""
    if not entries:
        return

    try:
        earliest = min(datetime.strptime(d, "%Y-%m-%d") for d in entries.keys())
    except ValueError:
        return

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    current = earliest

    while current <= today:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in entries:
            entries[date_str] = f"Missing campaign data for {date_str}"
        current += timedelta(days=1)


def append_summary_to_log(run_date: datetime, total_rows: int) -> None:
    """Append or update today's entry in the summary log."""
    entries = load_summary_log()

    date_str = run_date.strftime("%Y-%m-%d")

    if total_rows > 0:
        entries[date_str] = f"Completed campaign on {date_str}: {total_rows} entries processed"
    else:
        entries[date_str] = f"No campaign data for {date_str}"

    fill_missing_dates(entries)
    save_summary_log(entries)


def get_most_recent_summary_files() -> list[tuple[Path, str]]:
    """Get summary CSV files for the most recent date."""
    summary_dates = get_all_summary_dates()
    if not summary_dates:
        return []

    dates_with_files = []
    for date_str, files in summary_dates.items():
        try:
            dt = datetime.strptime(date_str, "%d%m%Y")
            dates_with_files.append((date_str, files, dt))
        except:
            continue

    if not dates_with_files:
        return []

    dates_with_files.sort(key=lambda x: x[2], reverse=True)
    most_recent_date = dates_with_files[0][0]
    most_recent_files = dates_with_files[0][1]

    return [(f, most_recent_date) for f in most_recent_files]


def main() -> None:
    parser = argparse.ArgumentParser(description="Append campaign summaries to database and log")
    parser.add_argument(
        "--date",
        type=parse_date,
        default=None,
        help="Target date in DDMMYYYY format (default: today).",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        default=False,
        help="Skip database insertion, only update log.",
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        default=False,
        help="Sync all missing dates from summary folder to database.",
    )
    args = parser.parse_args()

    run_date = args.date if args.date else datetime.now()
    date_str = run_date.strftime("%d%m%Y")

    if args.skip_db:
        print("[INFO] Skipping database operations (--skip-db)")
    else:
        print("[INFO] Checking for missing dates in database...")
        processed = sync_all_summaries_to_db()
        if processed:
            print(f"[INFO] Inserted {len(processed)} missing date(s) to database")

    recent_files = get_most_recent_summary_files()

    if not recent_files:
        print(f"[WARNING] No summary files found in {SUMMARY_DIR}")
        print("[INFO] Stage 5 complete.")
        return

    print(f"[INFO] Found {len(recent_files)} summary file(s) for most recent date")

    total_rows = 0
    most_recent_date = None

    for csv_path, file_date in recent_files:
        print(f"[INFO] Loading {csv_path.name}")
        try:
            df = pd.read_csv(csv_path)
            total_rows += len(df)
            most_recent_date = file_date

            if not args.skip_db:
                insert_summaries_to_neon(csv_path)
            else:
                print(f"[INFO] Skipping database insert (--skip-db)")

        except Exception as e:
            print(f"[ERROR] Failed to process {csv_path.name}: {e}")

    if total_rows == 0:
        print("[WARNING] No data loaded from summary files")

    print("[INFO] Stage 5 complete.")


if __name__ == "__main__":
    main()
