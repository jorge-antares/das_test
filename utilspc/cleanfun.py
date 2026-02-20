"""
Cleaning Functions for Plane Crashes Dataset
"""

import re
from datetime import datetime

def clean_text(value: str | None) -> str | None:
    """Strip whitespace; map '?' to NULL."""
    if value is None:
        return None
    v = value.strip()
    return None if v in ("?", "") else v


MAX_YEAR = 2018   # last year present in the dataset

def parse_date(value: str | None) -> str | None:
    """Parse 'DD-Mon-YY' → 'YYYY-MM-DD', or None on failure."""
    if not value or (value.strip() in ("?", "")):
        return None
    try:
        dt = datetime.strptime(value.strip(), "%d-%b-%y")
        year = dt.year
        # strptime maps 00-68 → 2000-2068, 69-99 → 1969-1999.
        # Shift any future year (> MAX_YEAR) back one century.
        if year > MAX_YEAR:
            year -= 100
        return f"{year:04d}-{dt.month:02d}-{dt.day:02d}"
    except ValueError:
        return None


def parse_time(value: str | None) -> str | None:
    """
    Normalise to 'HH:MM' (24-hour).
    Strips approximate prefix 'c' with optional space.
    Handles bare 4-digit integers (e.g. '1730' → '17:30').
    Returns None for unknown / unparseable values.
    """
    if not value or value.strip() in ("?", ""):
        return None
    v = value.strip()

    # Remove approximate prefix 'c' or 'c '
    v = re.sub(r"^c\s*", "", v, flags=re.IGNORECASE)

    # If already HH:MM or H:MM
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", v)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
        return None  # out-of-range

    # Bare 3 or 4-digit integer (HHMM or HMM)
    m = re.fullmatch(r"(\d{3,4})", v)
    if m:
        digits = m.group(1).zfill(4)
        hh, mm = int(digits[:2]), int(digits[2:])
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
        return None

    return None  # unparseable


# Regex to extract N and the breakdown tuple from 'N ▸ (passengers:X▸ crew:Y)'
# The '▸' character can be various Unicode bullets; use a broad match.
_ABOARD_RE = re.compile(
    r"(\d+)\s*.+passengers:(\?|\d+).+crew:(\?|\d+)",
    re.IGNORECASE,
)

def parse_count_field(value: str | None) -> tuple[int | None, int | None, int | None]:
    """
    Parse 'aboard' / 'fatalities' fields.
    Returns (total, passengers, crew) as int or None.
    """
    if not value or value.strip() in ("?", ""):
        return None, None, None
    m = _ABOARD_RE.search(value)
    if not m:
        # Try a plain integer fallback
        try:
            return int(value.strip()), None, None
        except ValueError:
            return None, None, None
    total = int(m.group(1))
    pax   = None if m.group(2) == "?" else int(m.group(2))
    crew  = None if m.group(3) == "?" else int(m.group(3))
    return total, pax, crew


def parse_ground(value: str | None) -> int | None:
    """Convert ground kills to int; '?' → None."""
    if not value or value.strip() in ("?", ""):
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


def safe_sum(*args: int | None) -> int:
    """Sum integers treating None as 0."""
    return sum((x or 0) for x in args)