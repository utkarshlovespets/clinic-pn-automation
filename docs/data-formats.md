# Data Formats

Schema reference for all CSV files used and produced by the pipeline.

---

## Input Data

### `data/clinic_mastersheet.csv`

Fetched from Google Sheets by Stage 1. Defines the campaign schedule.

| Column | Type | Description |
|---|---|---|
| `Date` | string | `DD/MM/YYYY` |
| `Day` | string | Day abbreviation (Mon, Tue, ...) |
| `Slot` | string | `morning` or `evening` |
| `Cohort Name` | string | Friendly label for your reference |
| `Campaign ID` | string | Must match `campaign_id` in `cohort_mapping.csv` |
| `Exclusion` | string | (Optional) Comma-separated cohort names to exclude |
| `Title` | string | Push notification title template |
| `Content` | string | Push notification body template |

---

### `data/cohort_mapping.csv`

Maps cohort codes to their data files, CleverTap campaign IDs, and URL templates.

| Column | Type | Description |
|---|---|---|
| `cohort_code` | string | Canonical cohort code used by the automation |
| `cohort_name` | string | Optional friendly name for personal reference |
| `campaign_id` | string | CleverTap External Trigger campaign ID for this cohort |
| `cohort_dataset` | string | Cohort CSV filename in `data/cohorts/` |
| `android_base_url` | string | Android deeplink URL (may contain `{date}`, `{priority}` tokens) |
| `ios_base_url` | string | iOS deeplink URL (may contain `{date}`, `{priority}` tokens) |

### `data/exclusion_mapping.csv`

Fetched from the `Exclusion_Mapping` Google Sheet tab.

| Column | Type | Description |
|---|---|---|
| `Exclusion Name` | string | Name used in the mastersheet `Exclusion` column |
| `Dataset` | string | Exclusion CSV filename in `data/cohorts/` |

**Priority Token Format:**
- Morning campaigns: `{priority}` replaced with `1M`, `2M`, `3M`, ...
- Evening campaigns: `{priority}` replaced with `1E`, `2E`, `3E`, ...

Example: `https://supertails.com/clinic?utm_campaign={date}_MP_{priority}_Clinic_xxRAJ`

Result (morning, priority 1): `https://supertails.com/clinic?utm_campaign=25March_MP_1M_Clinic_xxRAJ`

Result (evening, priority 3): `https://supertails.com/clinic?utm_campaign=25March_MP_3E_Clinic_xxRAJ`

---

### `data/cohorts/*.csv`

User lists per cohort segment. One file per cohort, named to match `cohort_dataset` in the deeplink map.

| Column | Type | Description |
|---|---|---|
| `email` | string | User's email address (used as CleverTap identity) |
| `first_name` | string | Customer's first name (may be blank) |
| `pet_name` | string | Pet's name (may be blank) |

---

## Intermediate Data (Stage 2 Outputs)

Located in `outputs/{DDMMYYYY}_{slot}/`

### `NN_CohortName.csv` (after Stage 2)

One file per cohort. `NN` is the zero-padded priority number (01, 02, ...).

| Column | Type | Description |
|---|---|---|
| `Email` | string | User email |
| `First Name` | string | Customer's first name |
| `Pet Name` | string | Pet's name |

---

### `campaign_meta.csv`

Metadata linking each priority slot to its campaign configuration.

| Column | Type | Description |
|---|---|---|
| `priority` | integer | Priority rank (1 = highest) |
| `cohort_name` | string | Raw cohort name from mastersheet |
| `title` | string | Title template |
| `body` | string | Body template |
| `android_base_url` | string | Android URL template |
| `ios_base_url` | string | iOS URL template |

---

### `summary.csv`

Exclusion statistics for the campaign run.

| Column | Type | Description |
|---|---|---|
| `priority` | integer | Priority number |
| `cohort_name` | string | Cohort name |
| `input_candidates` | integer | Users in raw cohort CSV |
| `excluded_by_priority` | integer | Removed by priority deduplication |
| `excluded_by_exclusion_col` | integer | Removed by explicit exclusion column |
| `final_count` | integer | Users targeted after all exclusions |

**Example:**

```
priority,cohort_name,input_candidates,excluded_by_priority,excluded_by_exclusion_col,final_count
1,Rajaji_Nagar_n2b_15km,4688,0,0,4688
2,Clinic_KN_Mar26,8464,0,0,8464
3,Clinic_Birthday,3761,471,0,3290
```

---

## Final Data (Stage 3 Outputs)

### `NN_CohortName.csv` (after Stage 3 enrichment)

Same files as Stage 2 output, with additional columns added in-place.

| Column | Type | Description |
|---|---|---|
| `Email` | string | User email |
| `First Name` | string | Customer's first name |
| `Pet Name` | string | Pet's name |
| `title` | string | Resolved notification title (personalized) |
| `body` | string | Resolved notification body (personalized) |
| `campaign_id` | string | Campaign ID copied from `cohort_mapping.csv` for this cohort |
| `android_deeplink` | string | Final Android URL (date and slot-tagged priority substituted) |
| `ios_deeplink` | string | Final iOS URL (date and slot-tagged priority substituted) |

**Example (morning, priority 1):**
- `android_deeplink`: `https://supertails.com/clinic?utm_campaign=25March_MP_1M_Clinic_xxRAJ`
- `ios_deeplink`: `https://supertails.com/clinic?utm_campaign=25March_MP_1M_Clinic_xxRAJ`

**Example (evening, priority 2):**
- `android_deeplink`: `https://supertails.com/clinic?utm_campaign=25March_MP_2E_Clinic_xxRAJ`
- `ios_deeplink`: `https://supertails.com/clinic?utm_campaign=25March_MP_2E_Clinic_xxRAJ`

---

## Log Data

### `outputs/log/{DDMMYYYY}_{slot}_dispatch_log.csv`

One row per individual send attempt. Appended to on each run (not overwritten).

| Column | Type | Description |
|---|---|---|
| `email` | string | Recipient email |
| `cohort_name` | string | Cohort the user belongs to |
| `priority` | integer | Cohort priority |
| `title` | string | Notification title sent |
| `body` | string | Notification body sent |
| `dry_run` | boolean | `True` if no actual API call was made |
| `timestamp` | string | ISO 8601 datetime of the attempt |
| `status` | string | HTTP status code (e.g., `200`) or error message |

---

## Cohort Segments Reference

| Filename | Segment Description |
|---|---|
| `all_kalyan_nagar.csv` | All customers in Kalyan Nagar pincodes |
| `all_kr_puram.csv` | All customers in KR Puram pincodes |
| `all_rajaji_nagar.csv` | All customers in Rajaji Nagar pincodes |
| `appointment_completed.csv` | Customers who completed a clinic appointment |
| `lapser_remove.csv` | Lapsed customers (used for exclusion) |
| `multiple_pet_parents.csv` | Customers with multiple pets |
| `n2b_bangalore.csv` | New-to-business customers in Bangalore |
| `n2b_birthday.csv` | New-to-business customers with upcoming pet birthdays |
| `n2b_dental.csv` | New-to-business customers for dental services |
| `n2b_gut.csv` | New-to-business customers for gut health services |
| `n2b_skin.csv` | New-to-business customers for skin health services |
| `repeat_ahs.csv` | Repeat Animal Health Services customers |
| `repeat_clinic.csv` | Repeat clinic visitors |
| `vaccination_due.csv` | Customers with upcoming vaccination appointments |
