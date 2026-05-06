import argparse
import csv
import os
import re
from pathlib import Path
from typing import Any, List

from dotenv import dotenv_values
from google.auth.credentials import Credentials as GoogleAuthCredentials
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
WORKSHEET_NAME = "Clinic_PN_Automation"
COHORT_MAPPING_WORKSHEET_NAME = "Cohort_Mapping"
EXCLUSION_MAPPING_WORKSHEET_NAME = "Exclusion_Mapping"
IMAGE_MAPPING_WORKSHEET_NAME = "Image_Mapping"
DEFAULT_OUTPUT = "data/clinic_mastersheet.csv"
DEFAULT_COHORT_MAPPING_OUTPUT = "data/cohort_mapping.csv"
DEFAULT_EXCLUSION_MAPPING_OUTPUT = "data/exclusion_mapping.csv"
DEFAULT_IMAGE_MAPPING_OUTPUT = "data/image_mapping.csv"
DEFAULT_COLUMNS_RANGE = "A:Z"
DISCOUNT_PLACEHOLDER_RE = re.compile(r"x\s*x\s*%", re.IGNORECASE)


def resolve_path(path_value: str, fallback: str, base_dir: Path) -> Path:
	"""Resolve paths from CLI/.env with sensible project-relative fallbacks."""
	raw = (path_value or "").strip() or fallback
	raw_path = Path(raw)

	if raw_path.is_absolute():
		return raw_path

	candidates = [base_dir / raw_path]

	# If only filename is provided, try secrets/<filename> as well.
	if raw_path.parent == Path("."):
		candidates.append(base_dir / "secrets" / raw_path.name)

	for candidate in candidates:
		if candidate.exists():
			return candidate

	return candidates[0]


def quote_sheet_title(sheet_title: str) -> str:
	"""Safely quote worksheet title for A1 notation, including apostrophes."""
	escaped = sheet_title.replace("'", "''")
	return f"'{escaped}'"


def build_range(worksheet_name: str, columns_range: str = DEFAULT_COLUMNS_RANGE) -> str:
	return f"{quote_sheet_title(worksheet_name)}!{columns_range}"


def get_credentials(credentials_path: Path, token_path: Path) -> GoogleAuthCredentials:
	creds: GoogleAuthCredentials | None = None

	if token_path.exists():
		creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

	if not creds or not creds.valid:
		if creds and creds.expired and creds.refresh_token:
			creds.refresh(Request())
		else:
			flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
			creds = flow.run_local_server(port=0)

		token_path.parent.mkdir(parents=True, exist_ok=True)
		token_path.write_text(creds.to_json(), encoding="utf-8")

	return creds


def fetch_sheet_values(service: Any, spreadsheet_id: str, data_range: str) -> List[List[str]]:
	result = (
		service.spreadsheets()
		.values()
		.get(spreadsheetId=spreadsheet_id, range=data_range)
		.execute()
	)
	return result.get("values", [])


def get_first_sheet_title(service: Any, spreadsheet_id: str) -> str:
	metadata: dict[str, Any] = (
		service.spreadsheets()
		.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))")
		.execute()
	)
	sheets = metadata.get("sheets", [])
	if not sheets:
		raise ValueError("Spreadsheet has no worksheets.")
	return sheets[0]["properties"]["title"]


def values_to_csv(
	values: List[List[str]],
	output_path: Path,
	apply_discount_cleanup: bool = True,
) -> None:
	if not values:
		raise ValueError("No data returned from Google Sheets. Check spreadsheet ID and range.")

	max_cols = max(len(row) for row in values)
	normalized_rows = [row + [""] * (max_cols - len(row)) for row in values]

	if apply_discount_cleanup:
		header = normalized_rows[0] if normalized_rows else []
		header_map = {str(col).strip().lower(): idx for idx, col in enumerate(header)}
		title_idx = header_map.get("title")
		content_idx = header_map.get("content")

		# Some sheets may be exported without a header row.
		# In that case, clinic mastersheet columns are expected as:
		# Date, Day, Slot, Cohort Name, Campaign ID, Exclusion, Title, Content
		if title_idx is None and content_idx is None:
			max_cols_count = len(header)
			if max_cols_count >= 8:
				title_idx = 6
				content_idx = 7

		if title_idx is not None or content_idx is not None:
			for row in normalized_rows[1:]:
				if title_idx is not None:
					row[title_idx] = DISCOUNT_PLACEHOLDER_RE.sub("10%", str(row[title_idx]))
				if content_idx is not None:
					row[content_idx] = DISCOUNT_PLACEHOLDER_RE.sub("10%", str(row[content_idx]))

	output_path.parent.mkdir(parents=True, exist_ok=True)
	with output_path.open("w", newline="", encoding="utf-8") as csv_file:
		writer = csv.writer(csv_file)
		writer.writerows(normalized_rows)


def fetch_values_to_csv(
	service: Any,
	spreadsheet_id: str,
	worksheet_name: str,
	output_path: Path,
	columns_range: str = DEFAULT_COLUMNS_RANGE,
	apply_discount_cleanup: bool = False,
) -> int:
	"""Fetch one worksheet by name and save it as CSV. Returns data row count."""
	data_range = build_range(worksheet_name, columns_range)
	values = fetch_sheet_values(service, spreadsheet_id, data_range)
	values_to_csv(values, output_path, apply_discount_cleanup=apply_discount_cleanup)
	return len(values) - 1 if values else 0


def main() -> None:
	script_dir = Path(__file__).resolve().parent
	project_root = script_dir.parent

	# Read .env directly into a dict — avoids os.environ interference entirely.
	env = dotenv_values(project_root / ".env")

	env_spreadsheet_id = (env.get("SPREADSHEET_ID") or "").strip()
	env_credentials = (env.get("GOOGLE_CREDENTIALS_FILE") or "secrets/credentials.json").strip()
	env_token = (env.get("GOOGLE_TOKEN_FILE") or "secrets/token.json").strip()

	parser = argparse.ArgumentParser(
		description="Fetch clinic master sheet data from Google Sheets and save as CSV."
	)
	parser.add_argument(
		"--spreadsheet-id",
		default=env_spreadsheet_id or None,
		help="Google Spreadsheet ID (the long ID in the sheet URL).",
	)
	parser.add_argument(
		"--range",
		default=None,
		help="Explicit Google Sheets A1 range (overrides --worksheet-name).",
	)
	parser.add_argument(
		"--credentials",
		default=env_credentials or "secrets/credentials.json",
		help="Path to OAuth client credentials JSON.",
	)
	parser.add_argument(
		"--token",
		default=env_token or "secrets/token.json",
		help="Path to OAuth token JSON.",
	)
	parser.add_argument(
		"--output",
		default=DEFAULT_OUTPUT,
		help="Path to save output CSV.",
	)
	parser.add_argument(
		"--cohort-mapping-output",
		default=DEFAULT_COHORT_MAPPING_OUTPUT,
		help="Path to save Cohort_Mapping CSV.",
	)
	parser.add_argument(
		"--exclusion-mapping-output",
		default=DEFAULT_EXCLUSION_MAPPING_OUTPUT,
		help="Path to save Exclusion_Mapping CSV.",
	)
	parser.add_argument(
		"--image-mapping-output",
		default=DEFAULT_IMAGE_MAPPING_OUTPUT,
		help="Path to save Image_Mapping CSV.",
	)
	parser.add_argument(
		"--skip-mapping-fetch",
		action="store_true",
		help="Only fetch the clinic mastersheet; skip Cohort_Mapping and Exclusion_Mapping.",
	)

	args = parser.parse_args()

	if not args.spreadsheet_id:
		parser.error("Missing spreadsheet ID. Set SPREADSHEET_ID in .env or pass --spreadsheet-id.")

	credentials_path = resolve_path(args.credentials, "secrets/credentials.json", project_root)
	token_path = resolve_path(args.token, "secrets/token.json", project_root)
	output_path = resolve_path(args.output, DEFAULT_OUTPUT, project_root)
	cohort_mapping_output_path = resolve_path(
		args.cohort_mapping_output, DEFAULT_COHORT_MAPPING_OUTPUT, project_root
	)
	exclusion_mapping_output_path = resolve_path(
		args.exclusion_mapping_output, DEFAULT_EXCLUSION_MAPPING_OUTPUT, project_root
	)
	image_mapping_output_path = resolve_path(
		args.image_mapping_output, DEFAULT_IMAGE_MAPPING_OUTPUT, project_root
	)

	if not credentials_path.exists():
		raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

	creds = get_credentials(credentials_path, token_path)
	service = build("sheets", "v4", credentials=creds)

	user_provided_range = bool(args.range and args.range.strip())
	final_range = args.range.strip() if user_provided_range else build_range(WORKSHEET_NAME)

	try:
		values = fetch_sheet_values(service, args.spreadsheet_id, final_range)
	except HttpError as exc:
		if "Unable to parse range" not in str(exc):
			raise

		fallback_title = get_first_sheet_title(service, args.spreadsheet_id)
		fallback_range = build_range(fallback_title)

		print(f"Warning: range '{final_range}' not found. Falling back to first worksheet: {fallback_range}")

		final_range = fallback_range
		values = fetch_sheet_values(service, args.spreadsheet_id, final_range)

	values_to_csv(values, output_path)

	data_rows = len(values) - 1 if values else 0
	print(f"Fetched {data_rows} data rows from {final_range}.")
	print(f"Saved CSV to: {output_path}")

	if not args.skip_mapping_fetch:
		cohort_rows = fetch_values_to_csv(
			service,
			args.spreadsheet_id,
			COHORT_MAPPING_WORKSHEET_NAME,
			cohort_mapping_output_path,
		)
		print(
			f"Fetched {cohort_rows} data rows from "
			f"{build_range(COHORT_MAPPING_WORKSHEET_NAME)}."
		)
		print(f"Saved CSV to: {cohort_mapping_output_path}")

		exclusion_rows = fetch_values_to_csv(
			service,
			args.spreadsheet_id,
			EXCLUSION_MAPPING_WORKSHEET_NAME,
			exclusion_mapping_output_path,
		)
		print(
			f"Fetched {exclusion_rows} data rows from "
			f"{build_range(EXCLUSION_MAPPING_WORKSHEET_NAME)}."
		)
		print(f"Saved CSV to: {exclusion_mapping_output_path}")

		image_rows = fetch_values_to_csv(
			service,
			args.spreadsheet_id,
			IMAGE_MAPPING_WORKSHEET_NAME,
			image_mapping_output_path,
		)
		print(
			f"Fetched {image_rows} data rows from "
			f"{build_range(IMAGE_MAPPING_WORKSHEET_NAME)}."
		)
		print(f"Saved CSV to: {image_mapping_output_path}")


if __name__ == "__main__":
	main()
