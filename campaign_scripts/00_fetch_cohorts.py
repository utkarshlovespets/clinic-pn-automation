"""Fetch cohort datasets from MySQL and save as CSV files in data/cohorts/.

Reads every .sql file from data/queries/, executes it against MySQL,
and writes the result to data/cohorts/<query_name>.csv.

MySQL credentials are read from .env:
    MYSQL_HOST      -- default: localhost
    MYSQL_PORT      -- default: 3306
    MYSQL_USER
    MYSQL_PASSWORD

Usage:
    python fetch_cohorts.py                        # run all queries
    python fetch_cohorts.py --query all_rajaji_nagar
    python fetch_cohorts.py --query all_rajaji_nagar n2b_bangalore
"""

import argparse
import sys
from pathlib import Path

import mysql.connector
import pandas as pd
from dotenv import dotenv_values


QUERIES_DIR = "data/sql_queries"
COHORTS_DIR = "data/cohorts"


def get_connection(env: dict):
    host = (env.get("MYSQL_HOST") or "localhost").strip()
    port = int((env.get("MYSQL_PORT") or "3306").strip())
    user = (env.get("MYSQL_USER") or "").strip()
    password = (env.get("MYSQL_PASSWORD") or "").strip()

    if not user or not password:
        print("[ERROR] MYSQL_USER and MYSQL_PASSWORD must be set in .env")
        sys.exit(1)

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )


def run_query(conn, sql: str) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(rows, columns=columns)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    env = dotenv_values(script_dir / ".env")

    queries_dir = (script_dir / QUERIES_DIR).resolve()
    cohorts_dir = (script_dir / COHORTS_DIR).resolve()

    if not queries_dir.exists():
        print(f"[ERROR] Queries directory not found: {queries_dir}")
        sys.exit(1)

    all_sql_files = sorted(queries_dir.glob("*.sql"))
    if not all_sql_files:
        print(f"[ERROR] No .sql files found in {queries_dir}")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Fetch cohort datasets from MySQL and save to data/cohorts/."
    )
    parser.add_argument(
        "--query",
        nargs="+",
        metavar="NAME",
        default=None,
        help=(
            "One or more query names to run (without .sql extension). "
            "Default: run all queries in data/queries/. "
            f"Available: {', '.join(f.stem for f in all_sql_files)}"
        ),
    )
    args = parser.parse_args()

    if args.query:
        available = {f.stem: f for f in all_sql_files}
        not_found = set(args.query) - available.keys()
        if not_found:
            print(f"[ERROR] Query file(s) not found: {', '.join(sorted(not_found))}")
            print(f"  Available: {', '.join(sorted(available.keys()))}")
            sys.exit(1)
        sql_files = [available[name] for name in args.query]
    else:
        sql_files = all_sql_files

    print(f"Queries to run : {len(sql_files)}")
    print(f"Output dir     : {cohorts_dir}")
    print()

    cohorts_dir.mkdir(parents=True, exist_ok=True)

    print("Connecting to MySQL...")
    try:
        conn = get_connection(env)
    except mysql.connector.Error as exc:
        print(f"[ERROR] Could not connect to MySQL: {exc}")
        sys.exit(1)
    print("Connected.\n")

    success = 0
    for sql_file in sql_files:
        output_path = cohorts_dir / f"{sql_file.stem}.csv"
        print(f"  Running : {sql_file.name}")
        try:
            sql = sql_file.read_text(encoding="utf-8")
            df = run_query(conn, sql)
            df.to_csv(output_path, index=False)
            print(f"  -> {len(df)} rows saved to {output_path.name}")
            success += 1
        except mysql.connector.Error as exc:
            print(f"  [ERROR] Query failed: {exc}")
        print()

    conn.close()
    print(f"Done. {success}/{len(sql_files)} query/queries completed.")


if __name__ == "__main__":
    main()
