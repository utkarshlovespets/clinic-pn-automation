# Data Formats

CSV schema reference for current inputs, intermediate files, enriched outputs, and logs.

## Inputs Fetched From Google Sheets

### `data/clinic_mastersheet.csv`

| Column | Type | Notes |
|---|---|---|
| `Date` | string | `DD/MM/YYYY` |
| `Day` | string | Human reference |
| `Slot` | string | `Morning` or `Evening`; matching is case-insensitive |
| `Cohort Name` | string | Friendly mastersheet label |
| `Campaign ID` | string | Matches `campaign_id` in `data/cohort_mapping.csv` |
| `Exclusion` | string | Optional comma-separated exclusion names |
| `Image` | string | Optional image name. When present, Stage 3 resolves it through `data/image_mapping.csv` and uses `img_campaign_id` |
| `Title` | string | Push title template |
| `Content` | string | Push body template |

### `data/cohort_mapping.csv`

Fetched from `Cohort_Mapping`.

| Column | Type | Notes |
|---|---|---|
| `cohort_name` | string | Personal/reference name only |
| `cohort_code` | string | Required automation key |
| `campaign_id` | string | CleverTap External Trigger campaign ID |
| `img_campaign_id` | string | CleverTap External Trigger campaign ID used when the mastersheet row has an image |
| `cohort_dataset` | string | Cohort CSV filename under `data/cohorts/` |
| `android_base_url` | string | Android URL template |
| `ios_base_url` | string | iOS URL template |
| `exclusion` | string | Optional default comma-separated exclusions for this cohort. Values can match `exclusion_mapping.csv.exclusion_name`, `cohort_code`, or `cohort_name` |

### `data/exclusion_mapping.csv`

Fetched from `Exclusion_Mapping`.

| Column | Type | Notes |
|---|---|---|
| `Exclusion Name` | string | Name used in mastersheet `Exclusion` cells |
| `Dataset` | string | Exclusion CSV filename under `data/cohorts/` |

### `data/image_mapping.csv`

Fetched from `Image_Mapping`.

| Column | Type | Notes |
|---|---|---|
| `image_name` | string | Name used in mastersheet `Image` cells |
| `image_url` | string | URL passed as `ExternalTrigger.image_url` |

### `data/cohorts/*.csv`

Audience files used by both campaign cohorts and exclusions.

| Column | Type | Notes |
|---|---|---|
| `email` | string | User identity for CleverTap |
| `first_name` | string | Optional; first word is used |
| `pet_name` | string | Optional |

## Stage 2 Outputs

Located in `outputs/{DDMMYYYY}_{slot}/`.

### `NN_{cohort_code}.csv`

One file per campaign row after priority, default, and explicit exclusions.

| Column | Type | Notes |
|---|---|---|
| `Email` | string | Lowercased email |
| `First Name` | string | First-name value used for personalization |
| `Pet Name` | string | Pet-name value used for personalization |

### `summary.csv`

One metadata row per generated priority CSV. This is the canonical Stage 2 metadata file.

| Column | Type | Notes |
|---|---|---|
| `priority` | integer | Row order priority, starting at 1 |
| `cohort_name` | string | `cohort_code` resolved from `Cohort_Mapping` |
| `mastersheet_cohort_name` | string | Friendly label from mastersheet |
| `campaign_id` | string | Campaign ID from mastersheet |
| `base_campaign_id` | string | Generic campaign ID from the mastersheet/cohort map |
| `img_campaign_id` | string | Image campaign ID from `cohort_mapping.csv` |
| `image_name` | string | Image name from the mastersheet `Image` column |
| `image_url` | string | Resolved URL from `image_mapping.csv`; populated by Stage 3 |
| `title_template` | string | Raw title template |
| `content_template` | string | Raw body template |
| `input_candidates` | integer | Unique candidate emails before filtering |
| `excluded_by_priority` | integer | Removed because already targeted by a higher-priority row |
| `excluded_by_default` | integer | Removed because of `cohort_mapping.csv.exclusion` |
| `excluded_by_exclusion_col` | integer | Removed because of the mastersheet `Exclusion` column |
| `default_exclusion_cohorts` | string | Default exclusions from `cohort_mapping.csv.exclusion` |
| `exclusion_cohorts` | string | Row-level exclusions from the mastersheet `Exclusion` column |
| `final_count` | integer | Users written to the priority CSV |
| `output_file` | string | Generated cohort CSV filename |

### `outputs/log/summary/{DDMMYYYY}_{slot}.csv`

Stage 2 writes a compact run summary here.

| Column | Type |
|---|---|
| `date` | string |
| `slot` | string |
| `priority` | integer |
| `campaign_id` | string |
| `base_campaign_id` | string |
| `img_campaign_id` | string |
| `image_name` | string |
| `image_url` | string |
| `utm_campaign` | string |
| `title_template` | string |
| `content_template` | string |
| `excluded_by_priority` | integer |
| `excluded_by_default` | integer |
| `excluded_by_exclusion_col` | integer |
| `final_count` | integer |

## Stage 3 Outputs

Stage 3 enriches the same `NN_{cohort_code}.csv` files in place.

| Added Column | Type | Notes |
|---|---|---|
| `title` | string | Personalized title |
| `body` | string | Personalized body |
| `campaign_id` | string | Effective CleverTap campaign ID. Uses `img_campaign_id` when `image_name` is present |
| `base_campaign_id` | string | Generic CleverTap campaign ID |
| `img_campaign_id` | string | Image CleverTap campaign ID |
| `image_name` | string | Image name from mastersheet metadata |
| `image_url` | string | URL passed as `ExternalTrigger.image_url` |
| `android_deeplink` | string | URL with `{date}` and `{priority}` resolved |
| `ios_deeplink` | string | URL with `{date}` and `{priority}` resolved |

## Stage 4 Logs

Campaign logs are written under:

```text
outputs/log/dry_run/{DDMMYYYY}_{slot}_campaign_log.csv
outputs/log/live/{DDMMYYYY}_{slot}_campaign_log.csv
```

| Column | Type | Notes |
|---|---|---|
| `image_url` | string | Image URL included in the External Trigger payload, when present |

| Column | Type | Notes |
|---|---|---|
| `timestamp` | string | Send/log timestamp |
| `email` | string | Recipient email |
| `utm_name` | string | Extracted from deeplink URL |
| `clicked` | string | Blank placeholder |
| `title` | string | Truncated title preview |
| `body` | string | Truncated body preview |

## Template Placeholders

`Title` and `Content` support these placeholders in single or double braces:

| Placeholder | Uses | Fallback |
|---|---|---|
| `{your pet}` | Pet name | `your pet` |
| `{your pet's}` | Pet possessive | `your pet's` |
| `{pet parent}` | First name | `pet parent` |

## Deeplink Tokens

| Token | Replacement |
|---|---|
| `{date}` | `DDMonth`, for example `04May` |
| `{priority}` | Slot-tagged priority, for example `1M`, `2M`, `1E`, `2E` |
