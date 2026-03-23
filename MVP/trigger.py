"""MVP trigger — send CleverTap push notifications from payload.csv.

Reads payload.csv (Email, First Name, Pet Name, title, body,
android_deeplink, ios_deeplink) and fires CleverTap v1 External Trigger API.

DISCLAIMER: Defaults to DRY-RUN. Pass --live only when authorised.

Usage:
    python trigger.py           # dry-run (safe default)
    python trigger.py --live    # real send
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from dotenv import dotenv_values

# -------- Config from .env --------

SCRIPT_DIR  = Path(__file__).resolve().parent
_env        = dotenv_values(SCRIPT_DIR / ".env")

ACCOUNT_ID  = (_env.get("CLEVERTAP_ACCOUNT_ID") or "").strip()
PASSCODE    = (_env.get("CLEVERTAP_PASSCODE")   or "").strip()
REGION      = (_env.get("CLEVERTAP_REGION")     or "in1").strip()
CAMPAIGN_ID = int((_env.get("CLEVERTAP_CAMPAIGN_ID") or "0").strip())

URL = f"https://{REGION}.api.clevertap.com/1/send/externaltrigger.json"

HEADERS = {
    "X-CleverTap-Account-Id": ACCOUNT_ID,
    "X-CleverTap-Passcode":   PASSCODE,
    "Content-Type":           "application/json",
}

BATCH_SIZE   = 1000
MAX_WORKERS  = 10
LIVE_COUNTDOWN = 2

# -------- Load CSV --------

df = pd.read_csv(SCRIPT_DIR / "payload.csv", dtype=str, keep_default_na=False)

# Ensure optional deeplink columns exist so groupby doesn't fail
for _col in ("android_deeplink", "ios_deeplink"):
    if _col not in df.columns:
        df[_col] = ""

group_cols = ["title", "body", "android_deeplink", "ios_deeplink"]

# -------- Build all batches first --------

jobs = []

for (title, body, android_deeplink, ios_deeplink), group in df.groupby(group_cols, sort=False):

    emails = group["Email"].str.strip().str.lower().dropna().drop_duplicates().tolist()

    for i in range(0, len(emails), BATCH_SIZE):

        batch_emails = emails[i : i + BATCH_SIZE]

        ext_trigger = {
            "title":                        str(title),
            "body":                         str(body),
        }
        if android_deeplink:
            ext_trigger["android_deeplink"] = str(android_deeplink)
        if ios_deeplink:
            ext_trigger["ios_deeplink"] = str(ios_deeplink)

        payload = {
            "to":               {"email": batch_emails},
            "campaign_id": int(CAMPAIGN_ID),
            "ExternalTrigger":  ext_trigger,
        }

        jobs.append(payload)

# -------- Function to send request --------

def send_request(payload, dry_run=False):
    emails = payload["to"]["email"]
    label  = f"Campaign {CAMPAIGN_ID} | {len(emails)} email(s)"

    if dry_run:
        preview = dict(payload)
        preview["to"] = {"email": emails[:3] + ([f"... +{len(emails)-3} more"] if len(emails) > 3 else [])}
        print(f"\n  [{label}] DRY-RUN payload:")
        print("  " + json.dumps(preview, indent=4).replace("\n", "\n  "))
        return f"{label} → [DRY-RUN] skipped"

    try:
        r = requests.post(URL, headers=HEADERS, json=payload, timeout=30)

        if r.status_code == 200:
            return f"{label} → ✓ OK"
        else:
            return f"{label} → ✗ FAILED | {r.text[:200]}"

    except Exception as e:
        return f"{label} → ✗ ERROR | {str(e)}"


# -------- Entry point --------

def main():
    if not ACCOUNT_ID or not PASSCODE or not CAMPAIGN_ID:
        print("[ERROR] Missing credentials in .env — check CLEVERTAP_ACCOUNT_ID, CLEVERTAP_PASSCODE, CLEVERTAP_CAMPAIGN_ID")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="MVP CleverTap trigger from payload.csv")
    parser.add_argument("--live", action="store_true", default=False,
                        help="Send real API calls (omit for dry-run)")
    args    = parser.parse_args()
    dry_run = not args.live

    if dry_run:
        print("\n[DRY-RUN] No API calls will be made. Pass --live to send.\n")
    else:
        print("\n!! LIVE MODE — REAL API CALLS WILL BE MADE !!")
        for n in range(LIVE_COUNTDOWN, 0, -1):
            print(f"  Starting in {n}...", end="\r", flush=True)
            time.sleep(1)
        print()

    print(f"Total API calls to make: {len(jobs)}\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [executor.submit(send_request, job, dry_run) for job in jobs]

        for f in as_completed(futures):
            print(f.result())

    print()
    if dry_run:
        print("[OK] Dry-run complete. Run with --live when authorised.")
    else:
        print("[OK] Live dispatch complete.")


if __name__ == "__main__":
    main()
