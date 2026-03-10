import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import requests
from dotenv import dotenv_values


DEFAULT_CLINIC_CSV = "data/clinic_mastersheet.csv"
DEFAULT_SEGMENT_MAP_CSV = "data/segment_map.csv"
SLOT_TIMES = {
    "morning": "11:30",
    "evening": "17:30",
}
SLOT_MP_OFFSET = {
    "morning": 0,
    "evening": 6,
}

# Campaign naming suffixes requested by business.
COHORT_CAMPAIGN_SUFFIX = {
    "N2B": "xxN2B",
    "Dental health": "Dental_xxN2B",
    "Multiple Pet Parents": "DoublePet_xxN2B",
    "Gut health": "Gut_xxN2B",
    "Vaccine Due N2B": "Vaccine_xxN2B",
    "Kalyan Nagar": "xxKAL",
    "Rajaji Nagar": "xxRAJ",
    "Skin Health": "Skin_xxN2B",
}


def normalize_slot(value: str) -> str:
    return str(value).strip().lower()


def pick_slot_column(df: pd.DataFrame) -> str:
    for col in ["Campaign Name", "Campiagn Name", "Slot"]:
        if col in df.columns:
            values = df[col].fillna("").astype(str).str.strip().str.lower()
            if values.isin(["morning", "evening"]).any():
                return col
    raise ValueError("No slot column found with values 'morning'/'evening'.")


def parse_exclusion_cell(exclusion_text: str) -> Tuple[List[str], List[dict], List[dict], List[str]]:
    segment_excludes: List[str] = []
    profile_filters: List[dict] = []
    event_did_not: List[dict] = []
    warnings: List[str] = []

    text = str(exclusion_text).strip()
    if not text:
        return segment_excludes, profile_filters, event_did_not, warnings

    tokens = [t.strip() for t in text.replace("|", ";").split(";") if t.strip()]
    for token in tokens:
        lower = token.lower()

        if lower == "30_min_delivery":
            profile_filters.append(
                {"name": "30min_delivery", "operator": "not_equals", "value": True}
            )
            continue

        if lower.startswith("segment:"):
            name = token.split(":", 1)[1].strip()
            if name:
                segment_excludes.append(name)
            else:
                warnings.append(f"Invalid segment token: '{token}'")
            continue

        if lower.startswith("profile:"):
            parts = token.split(":", 3)
            if len(parts) == 4:
                _, prop, operator, value = parts
                prop = prop.strip()
                operator = operator.strip().lower()
                value = value.strip()
                if prop and operator and value:
                    profile_filters.append({"name": prop, "operator": operator, "value": value})
                else:
                    warnings.append(f"Incomplete profile token: '{token}'")
            else:
                warnings.append(f"Invalid profile token: '{token}'")
            continue

        if lower.startswith("event_not_done:"):
            parts = token.split(":", 2)
            if len(parts) == 3:
                _, event_name, days_text = parts
                event_name = event_name.strip()
                try:
                    days = int(days_text.strip())
                except ValueError:
                    warnings.append(f"Invalid event_not_done days: '{token}'")
                    continue
                if event_name:
                    event_did_not.append({"event_name": event_name, "in_the_last_days": days})
                else:
                    warnings.append(f"Invalid event_not_done token: '{token}'")
            else:
                warnings.append(f"Invalid event_not_done token: '{token}'")
            continue

        # Allow plain event name tokens in Exclusion cell, e.g. "Appointment Completed".
        event_did_not.append({"event_name": token, "in_the_last_days": 365})
        continue

        warnings.append(f"Unknown exclusion token ignored: '{token}'")

    return segment_excludes, profile_filters, event_did_not, warnings


def build_segment_lookup(segment_map: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """Build a normalized lookup for segment IDs from cohort names, segment names, or IDs."""
    lookup: Dict[str, str] = {}
    for cohort_name, segment_info in segment_map.items():
        seg_name = str(segment_info.get("segment_name", "")).strip()
        seg_id = str(segment_info.get("segment_id", "")).strip()
        if not seg_id:
            continue

        lookup[seg_id] = seg_id
        if cohort_name:
            lookup[cohort_name.strip().lower()] = seg_id
        if seg_name:
            lookup[seg_name.lower()] = seg_id
    return lookup


def resolve_segment_exclusions(raw_segments: List[str], segment_lookup: Dict[str, str]) -> Tuple[List[str], List[str]]:
    """Resolve segment exclusion tokens to IDs; return (resolved_ids, unresolved_tokens)."""
    resolved: List[str] = []
    unresolved: List[str] = []
    for token in raw_segments:
        value = str(token).strip()
        if not value:
            continue
        resolved_id = segment_lookup.get(value) or segment_lookup.get(value.lower())
        if resolved_id:
            resolved.append(resolved_id)
        else:
            unresolved.append(value)
    return resolved, unresolved


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    output: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def validate_payload_exclusions(campaign_name: str, payload: dict) -> None:
    """Fail fast if exclude_segments contains non-numeric values."""
    checks: List[Tuple[str, List[str]]] = []

    top_level = payload.get("exclude_segments")
    if isinstance(top_level, list):
        checks.append(("exclude_segments", top_level))

    where_obj = payload.get("where")
    if isinstance(where_obj, dict):
        where_excludes = where_obj.get("exclude_segments")
        if isinstance(where_excludes, list):
            checks.append(("where.exclude_segments", where_excludes))

    for field_name, values in checks:
        invalid = [str(v) for v in values if not str(v).isdigit()]
        if invalid:
            raise ValueError(
                f"Invalid non-numeric segment exclusions in {field_name} for {campaign_name}: {invalid}"
            )


def export_payload(output_dir: Path, run_date: pd.Timestamp, slot: str, campaign_name: str, payload: dict) -> Path:
    date_prefix = run_date.strftime("%d%m%Y")
    slot_dir = output_dir / f"{date_prefix}_{slot}"
    slot_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"payload_{campaign_name}.json"
    output_path = slot_dir / file_name
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return output_path


def build_campaign_name(run_date: pd.Timestamp, mp_index: int, cohort_name: str) -> str:
    suffix = COHORT_CAMPAIGN_SUFFIX.get(cohort_name)
    if not suffix:
        fallback = "".join(ch for ch in cohort_name if ch.isalnum())
        suffix = fallback or "Unknown"
    return f"{run_date.day:02d}{run_date.strftime('%B')}_MP_{mp_index}_Clinic_{suffix}"


def build_schedule_string(run_date: pd.Timestamp, slot: str) -> str:
    return f"{run_date.strftime('%Y%m%d')} {SLOT_TIMES[slot]}"


def build_deep_links(campaign_name: str) -> Tuple[str, str]:
    android = (
        "https://supertails.com/pages/supertails-clinic"
        f"?utm_source=Clevertap&utm_medium=MobilePush&utm_campaign={campaign_name}"
    )
    ios = android
    return android, ios


def normalize_dynamic_variables(text: str) -> str:
    """Replace known placeholders with fallback text since the creation API rejects Liquid tags."""
    value = str(text)
    value = value.replace("{your pet}", "your pet")
    value = value.replace("{pet parent}", "pet parent")
    return value


def load_segment_map(path: Path) -> Dict[str, Dict[str, str]]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"cohort_name", "segment_name", "segment_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"segment_map is missing columns: {sorted(missing)}")

    result: Dict[str, Dict[str, str]] = {}
    for _, row in df.iterrows():
        cohort = str(row["cohort_name"]).strip()
        if not cohort:
            continue
        result[cohort] = {
            "segment_name": str(row["segment_name"]).strip(),
            "segment_id": str(row["segment_id"]).strip(),
        }
    return result


def load_clinic_sheet(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    required = {"Date", "Cohort Name", "Title", "Content"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"clinic_mastersheet is missing columns: {sorted(missing)}")

    slot_col = pick_slot_column(df)
    df["_slot"] = df[slot_col].map(normalize_slot)
    df["_date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
    df["_cohort"] = df["Cohort Name"].fillna("").astype(str).str.strip()
    df["_exclusion"] = df["Exclusion"].fillna("").astype(str).str.strip() if "Exclusion" in df.columns else ""
    return df


def build_where(
    cohort_name: str,
    waterfall_excluded_segments: List[str],
    extra_segment_excludes: List[str],
    extra_profile_filters: List[dict],
    extra_event_did_not: List[dict],
    run_date: pd.Timestamp,
) -> dict:
    profile_fields: List[dict] = []

    if cohort_name == "N2B":
        profile_fields.append({"name": "City", "operator": "equals", "value": "Bangalore"})

    profile_fields.extend(extra_profile_filters)

    where: dict = {}
    if profile_fields:
        where["common_profile_properties"] = {"profile_fields": profile_fields}

    excluded_segments = [x for x in waterfall_excluded_segments + extra_segment_excludes if x]
    if excluded_segments:
        where["exclude_segments"] = excluded_segments

    if extra_event_did_not:
        did_not_rules: List[dict] = []
        to_date = int(run_date.strftime("%Y%m%d"))
        for rule in extra_event_did_not:
            event_name = str(rule.get("event_name", "")).strip()
            if not event_name:
                continue

            if "from" in rule and "to" in rule:
                did_not_rules.append(
                    {
                        "event_name": event_name,
                        "from": int(rule["from"]),
                        "to": int(rule["to"]),
                    }
                )
                continue

            days = int(rule.get("in_the_last_days", 365))
            from_date = int((run_date - pd.Timedelta(days=days)).strftime("%Y%m%d"))
            did_not_rules.append(
                {
                    "event_name": event_name,
                    "from": from_date,
                    "to": to_date,
                }
            )

        if did_not_rules:
            where["did_not"] = did_not_rules[0] if len(did_not_rules) == 1 else did_not_rules

    return where


def create_payload(
    campaign_name: str,
    title: str,
    body: str,
    schedule_at: str,
    android_link: str,
    ios_link: str,
    android_wzrk_cid: str,
    segment_id: int | None,
    where_obj: dict | None,
    exclude_segments: List[str] | None,
) -> dict:
    payload = {
        "name": campaign_name,
        "target_mode": "push",
        "content": {
            "title": normalize_dynamic_variables(title),
            "body": normalize_dynamic_variables(body),
            "platform_specific": {
                "android": {
                    "deep_link": android_link,
                    # CleverTap sample payload uses wzrk_cid for Android channel routing.
                    "wzrk_cid": android_wzrk_cid,
                    "enable_rendermax": True,
                    "notification_delivery_priority": "high",
                    "notification_tray_priority": "max",
                },
                "ios": {
                    "deep_link": ios_link,
                    "mutable-content": "true",
                },
            },
        },
        "devices": ["android", "ios"],
        "when": schedule_at,
    }

    # API-supported targeting: segment and where cannot be used together.
    if segment_id is not None:
        payload["segment"] = str(segment_id)
        if exclude_segments:
            payload["exclude_segments"] = exclude_segments
    elif where_obj:
        payload["where"] = where_obj

    return payload


def post_campaign(api_url: str, account_id: str, passcode: str, payload: dict) -> dict:
    headers = {
        "X-CleverTap-Account-Id": account_id,
        "X-CleverTap-Passcode": passcode,
        "Content-Type": "application/json",
    }
    response = requests.post(api_url, json=payload, headers=headers, timeout=60)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body_text = response.text.strip()
        try:
            parsed = response.json()
            body_text = json.dumps(parsed, ensure_ascii=True)
        except ValueError:
            pass
        raise RuntimeError(
            f"CleverTap API error {response.status_code}: {body_text} | payload={json.dumps(payload, ensure_ascii=True)}"
        ) from exc

    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create scheduled CleverTap campaigns.")
    parser.add_argument("--clinic-csv", default=DEFAULT_CLINIC_CSV)
    parser.add_argument("--segment-map", default=DEFAULT_SEGMENT_MAP_CSV)
    parser.add_argument("--payload-output-dir", default="outputs")
    parser.add_argument("--date", default=None, help="DD/MM/YYYY (default tomorrow)")
    parser.add_argument("--live", action="store_true", help="Call API, default dry-run")
    parser.add_argument(
        "--live-limit",
        type=int,
        default=1,
        help="Maximum campaigns to create in live mode (default 1 for development safety).",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    env = dotenv_values(script_dir / ".env")
    account_id = (env.get("CLEVERTAP_ACCOUNT_ID") or "").strip()
    passcode = (env.get("CLEVERTAP_PASSCODE") or "").strip()
    region = (env.get("CLEVERTAP_REGION") or "").strip()
    android_wzrk_cid = (
        env.get("CLEVERTAP_ANDROID_WZRK_CID")
        or env.get("CLEVERTAP_ANDROID_NOTIFICATION_CHANNEL")
        or ""
    ).strip()

    if not account_id or not passcode or not region:
        raise ValueError("Missing CLEVERTAP_ACCOUNT_ID / CLEVERTAP_PASSCODE / CLEVERTAP_REGION in .env")

    run_date = (
        pd.to_datetime(args.date, format="%d/%m/%Y", errors="raise").normalize()
        if args.date
        else (pd.Timestamp.now().normalize() + pd.Timedelta(days=1))
    )

    clinic_df = load_clinic_sheet(Path(args.clinic_csv))
    segment_map = load_segment_map(Path(args.segment_map))
    segment_lookup = build_segment_lookup(segment_map)
    api_url = f"https://{region}.api.clevertap.com/1/targets/create.json"

    day_rows = clinic_df.loc[
        clinic_df["_date"].eq(run_date)
        & clinic_df["_slot"].isin(["morning", "evening"])
        & clinic_df["_cohort"].ne("")
        & clinic_df["Title"].astype(str).str.strip().ne("")
        & clinic_df["Content"].astype(str).str.strip().ne("")
    ].copy()

    mode = "LIVE" if args.live else "DRY-RUN"
    print(f"Mode: {mode}")
    print(f"Target date: {run_date.strftime('%d/%m/%Y')}")
    print("Delivery mode: scheduled only")
    if args.live:
        print(f"Live mode cap: max {args.live_limit} campaign(s)")

    if args.live and not android_wzrk_cid:
        raise ValueError(
            "Missing Android channel config. Set CLEVERTAP_ANDROID_WZRK_CID in .env "
            "(example: Marketing, or your exact CleverTap Android notification channel)."
        )

    if day_rows.empty:
        print("No usable rows found for target date.")
        return

    created = 0
    skipped = 0
    live_attempts = 0
    stop_live = False

    for slot in ["morning", "evening"]:
        if stop_live:
            break

        slot_rows = day_rows.loc[day_rows["_slot"].eq(slot)].copy()
        if slot_rows.empty:
            print(f"[{slot}] no rows, skipped")
            continue

        slot_rows = slot_rows.reset_index(drop=True)
        prior_cohorts: List[str] = []

        for idx, (_, row) in enumerate(slot_rows.iterrows()):
            if args.live and live_attempts >= max(args.live_limit, 0):
                print("Live mode campaign cap reached. Stopping further scheduling.")
                stop_live = True
                break

            cohort_name = str(row["_cohort"]).strip()
            mp_index = SLOT_MP_OFFSET[slot] + idx + 1

            campaign_name = build_campaign_name(run_date, mp_index, cohort_name)
            schedule_at = build_schedule_string(run_date, slot)
            android_link, ios_link = build_deep_links(campaign_name)

            waterfall_excluded_segments: List[str] = []
            for prior in prior_cohorts:
                seg = segment_map.get(prior, {}).get("segment_id", "").strip()
                if seg:
                    waterfall_excluded_segments.append(seg)

            extra_seg_raw, extra_profile, extra_event_did_not, warnings = parse_exclusion_cell(
                str(row.get("_exclusion", ""))
            )
            extra_seg, unresolved_segments = resolve_segment_exclusions(extra_seg_raw, segment_lookup)
            for unknown_segment in unresolved_segments:
                warnings.append(
                    f"Unknown segment exclusion '{unknown_segment}'. Add it to segment_map.csv as cohort_name or segment_name."
                )
            for warning in warnings:
                print(f"[{slot}] {cohort_name}: WARN {warning}")

            where_obj = build_where(
                cohort_name=cohort_name,
                waterfall_excluded_segments=waterfall_excluded_segments,
                extra_segment_excludes=extra_seg,
                extra_profile_filters=extra_profile,
                extra_event_did_not=extra_event_did_not,
                run_date=run_date,
            )
            segment_excludes = dedupe_preserve_order(
                [x for x in waterfall_excluded_segments + extra_seg if x]
            )

            seg_id_text = segment_map.get(cohort_name, {}).get("segment_id", "").strip()
            segment_id: int | None = None
            # N2B can run on City=Bangalore where filter when segment id is not available.
            if cohort_name != "N2B":
                if not seg_id_text.isdigit():
                    print(f"[{slot}] {cohort_name}: missing/invalid segment_id '{seg_id_text}', skipped")
                    skipped += 1
                    prior_cohorts.append(cohort_name)
                    continue
                segment_id = int(seg_id_text)

            payload = create_payload(
                campaign_name=campaign_name,
                title=str(row["Title"]),
                body=str(row["Content"]),
                schedule_at=schedule_at,
                android_link=android_link,
                ios_link=ios_link,
                android_wzrk_cid=android_wzrk_cid or "Marketing",
                segment_id=segment_id,
                where_obj=where_obj if where_obj else None,
                exclude_segments=segment_excludes,
            )

            validate_payload_exclusions(campaign_name, payload)
            payload_path = export_payload(
                output_dir=Path(args.payload_output_dir),
                run_date=run_date,
                slot=slot,
                campaign_name=campaign_name,
                payload=payload,
            )

            if args.live:
                live_attempts += 1
                try:
                    result = post_campaign(api_url, account_id, passcode, payload)
                    print(f"[{slot}] created {campaign_name} -> {result} | payload_file={payload_path}")
                except Exception as exc:
                    print(f"[{slot}] FAILED {campaign_name}: {exc}")
                    skipped += 1
                    prior_cohorts.append(cohort_name)
                    continue
            else:
                target_desc = "where(City=Bangalore)" if cohort_name == "N2B" else f"segment({segment_id})"
                print(f"[{slot}] draft {campaign_name} | {target_desc} | {schedule_at} | payload_file={payload_path}")

            created += 1
            prior_cohorts.append(cohort_name)

    print(f"\nDone. created={created}, skipped={skipped}")
    if not args.live:
        print("Dry-run only. Use --live to create scheduled campaigns in CleverTap.")


if __name__ == "__main__":
    main()
