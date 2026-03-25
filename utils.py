"""Shared utilities for the CleverTap automation pipeline."""

import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Template placeholder regex
# Matches {your pet}, {{your pet}}, {pet parent}, {{pet parent}} and all
# case/possessive variants in both single- and double-brace forms.
# ---------------------------------------------------------------------------
_PLACEHOLDER_RE = re.compile(
    r"\{{1,2}"
    r"(your\s+pet(?:'s|s)?|pet(?:\s+parent)?)"
    r"\}{1,2}",
    re.IGNORECASE,
)
_PET_WORDS = re.compile(r"^(your\s+pet(?:'s|s)?|pet)$", re.IGNORECASE)
_PARENT_WORDS = re.compile(r"^pet\s+parent$", re.IGNORECASE)


def normalize_cohort(name: str) -> str:
    """Normalize a cohort name for robust matching.

    Strips apostrophes (straight and curly), lowercases, then removes all
    non-alphanumeric characters so that names like "Clinic_Gut_N2B_Mar'26"
    and "clinic_gut_n2b_mar26" resolve to the same key.

    Examples:
        "Clinic_Gut_N2B_Mar'26"  -> "clinicgutn2bmar26"
        "N2B_All_Bangalore"      -> "n2ballbangalore"
        "Clinic_Birthday"  -> "clinicbirthday"
    """
    text = str(name).strip()
    # Remove straight and curly apostrophes before lowercasing.
    text = text.replace("'", "").replace("\u2019", "")
    text = text.lower()
    return re.sub(r"[^a-z0-9]", "", text)


def sanitize_filename(name: str) -> str:
    """Convert a cohort name to a safe filesystem filename component.

    Replaces runs of characters that are not alphanumeric, underscore, or dot
    with a single underscore.

    Examples:
        "Clinic_Birthday"   -> "Clinic_Birthday"
        "N2B- SUPERCASH500"       -> "N2B-_SUPERCASH500"
        "Clinic_Gut_N2B_Mar'26"   -> "Clinic_Gut_N2B_Mar_26"
    """
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name).strip())
    return safe.strip("_") or "unnamed"


def resolve_template(template: str, first_name: str, pet_name: str) -> str:
    """Replace placeholder tokens in a template string with actual values.

    Handles both {single} and {{double}} brace forms, all case variants,
    and possessive forms like {your pet's}.

    Generic fallbacks (when name is blank) are plain text -- no Liquid tags:
        pet_name  blank -> "your pet"
        first_name blank -> "pet parent"

    Args:
        template:   Raw title or content string (from campaign_meta.csv).
        first_name: User's first name (blank -> "pet parent").
        pet_name:   User's pet name   (blank -> "your pet").

    Returns:
        Resolved string with all placeholders substituted.
    """
    if not template:
        return template

    def _cap(value: str, at_start: bool) -> str:
        """Capitalize the first letter if the value opens the sentence."""
        if not value or not at_start:
            return value
        return value[0].upper() + value[1:]

    def _replace(match: re.Match) -> str:
        at_start = match.start() == 0
        token = match.group(1)
        if _PET_WORDS.match(token):
            if token.lower().endswith("'s"):
                return (_cap(pet_name, at_start) + "'s") if pet_name else _cap("your pet's", at_start)
            return _cap(pet_name, at_start) if pet_name else _cap("your pet", at_start)
        if _PARENT_WORDS.match(token):
            return _cap(first_name, at_start) if first_name else _cap("pet parent", at_start)
        return match.group(0)  # unknown token -- leave as-is

    return _PLACEHOLDER_RE.sub(_replace, template)


def resolve_path(path_value: str, fallback: str, base_dir: Path) -> Path:
    """Resolve a path from CLI / .env with sensible project-relative fallbacks.

    If only a bare filename is given (no directory component), also tries
    base_dir/secret/<filename> as a secondary candidate.  Returns the first
    existing candidate, or the primary candidate if none exist.
    """
    raw = (path_value or "").strip() or fallback
    raw_path = Path(raw)

    if raw_path.is_absolute():
        return raw_path

    candidates = [base_dir / raw_path]
    if raw_path.parent == Path("."):
        candidates.append(base_dir / "secret" / raw_path.name)

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]
