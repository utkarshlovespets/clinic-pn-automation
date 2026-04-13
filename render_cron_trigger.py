from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def main() -> int:
    parser = argparse.ArgumentParser(description="Trigger a campaign job through the web API.")
    parser.add_argument("--slot", choices=["morning", "evening", "both"], default="morning")
    parser.add_argument("--endpoint", default="/api/campaign/run")
    parser.add_argument("--date", default=None)
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("CRON_MAX_WORKERS", "10")))
    parser.add_argument(
        "--live",
        action="store_true",
        default=(os.getenv("CRON_LIVE", "false").strip().lower() == "true"),
    )
    args = parser.parse_args()

    base_url = (os.getenv("APP_BASE_URL") or "").strip().rstrip("/")
    admin_key = (os.getenv("ADMIN_API_KEY") or "").strip()
    if not base_url:
        print("APP_BASE_URL is not set", file=sys.stderr)
        return 1
    if not admin_key:
        print("ADMIN_API_KEY is not set", file=sys.stderr)
        return 1

    payload = {
        "date": args.date or datetime.now(tz=IST).strftime("%d%m%Y"),
        "slot": args.slot,
        "live": args.live,
        "max_workers": args.max_workers,
    }

    request = Request(
        f"{base_url}{args.endpoint}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Admin-Key": admin_key,
        },
        method="POST",
    )
    with urlopen(request, timeout=60) as response:
        body = response.read().decode("utf-8")
        print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
