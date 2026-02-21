"""
Cleaning Functions for Plane Crashes Dataset
"""

import re
import sqlite3
from datetime import datetime




def clean_text(value: str | None) -> str | None: 
    """Strip whitespace; map '?' to NULL."""
    if value is None:
        return None
    v = value.strip()
    return None if v in ("?", "") else v




def parse_date(value: str | None) -> str | None: 
    """Parse 'DD-Mon-YY' → 'YYYY-MM-DD', or None on failure."""
    MAX_YEAR = 2025   # last year present in the dataset
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


# ---------------------------------------------------------------------------
# Location parsing helpers
# ---------------------------------------------------------------------------

_US_STATE_NAMES: frozenset[str] = frozenset({
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming", "District Of Columbia",
    # Territories commonly appearing in the dataset
    "Puerto Rico", "Guam", "American Samoa",
})

_US_STATE_ABBREVS: frozenset[str] = frozenset({
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
})

_UK_COMPONENTS: frozenset[str] = frozenset({
    "england", "scotland", "wales", "northern ireland",
    "uk", "u.k.", "united kingdom", "great britain", "britain",
})

_GEO_WORDS = re.compile(
    r"\b(ocean|sea|channel|gulf|strait|atlantic|pacific|mediterranean|"
    r"arctic|antarctic|bay|lake|river|north sea|indian ocean)\b",
    re.IGNORECASE,
)


def parse_location(value: str | None) -> tuple[str | None, str | None]:
    """
    Extract the location and country from a location string.

    Returns a tuple ``(first, last)`` where ``first`` is the location
    excluding the country (or None when not available) and ``last`` is the
    country name (or None when not parseable).

    Rules
    -----
    - Unknown / empty ('?') → (None, None).
    - Last comma-separated token is treated as the country candidate;
      everything before it is joined back as the location (``first``).
    - If that token is a US state name or 2-letter state code → last = "United States".
    - If that token is a UK constituent (England, Scotland, Wales, etc.)
      → last = "United Kingdom".
    - If that token (or the whole string when there is no comma) matches a
      geographic feature (ocean, sea, gulf, …) → (None, None).
    - Single-token entries with no geographic word: first = None,
      last = the bare country name (e.g. 'Russia').
    - Otherwise first = everything before the last comma, last = the last token.
    """
    if not value or value.strip() in ("?", ""):
        return None, None

    parts = [p.strip() for p in value.strip().split(",")]
    last = parts[-1]
    first = ", ".join(parts[:-1]) if len(parts) > 1 else None

    # --- US state check ---
    if last.upper() in _US_STATE_ABBREVS:
        return first, "United States"
    if last.title() in _US_STATE_NAMES:
        return first, "United States"

    # --- UK constituent check ---
    if last.lower() in _UK_COMPONENTS:
        return first, "United Kingdom"

    # --- Geographic feature as last token → no country parseable ---
    if _GEO_WORDS.search(last):
        return None, None

    # --- Single token (no comma) ---
    if len(parts) == 1:
        if _GEO_WORDS.search(value):
            return None, None
        # Check for embedded UK component (e.g. 'Glasgow Scotland')
        for component in _UK_COMPONENTS:
            if re.search(rf"\b{re.escape(component)}\b", value, re.IGNORECASE):
                return None, "United Kingdom"
        return None, last  # bare country name (e.g. 'Russia', 'Brazil')

    # --- Multi-part: last token is the country name ---
    return first, last


# ---------------------------------------------------------------------------
# Operator parsing helpers
# ---------------------------------------------------------------------------

# Entries that are purely numeric / serial codes (e.g. '46826/109')
_OPERATOR_PURE_CODE: re.Pattern = re.compile(r"^\d+[/\-]\d+$")

# Aircraft manufacturer prefixes that occasionally appear in the operator field
# because of data-entry errors (the aircraft type was placed in the operator
# column).  A negative lookahead filters out *legitimate* company names that
# start with the same manufacturer word, e.g.:
#   Boeing Air Transport       → kept  (Air)
#   Boeing Aircraft Company    → kept  (Aircraft)
#   Bristol Aeroplane Co.      → kept  (Aeroplane)
#   Boeing KC-135E             → NULL  (model designator, not a company name)
#   Lockheed AC-130H Hercules  → NULL
#   Bristol 170 Freighter 31M  → NULL  (starts with a digit after Bristol)
#   IAI 1124 Westwind          → NULL
#   North American Sabreliner 40 → NULL
#   Lockheed Loadstar          → NULL  (aircraft name, no company indicator)
_OPERATOR_AC_TYPE: re.Pattern = re.compile(
    r"^(?:Boeing|Lockheed|Bristol|Carvair|IAI|North\s+American)\s+"
    r"(?!Air\b|Aircraft\b|Aeroplane\b|Airways\b|Transport\b|Company\b|Co\.)",
    re.IGNORECASE,
)

# Trailing aircraft-type designator run-on (e.g. '…Air ForceC-47').
# Matches an uppercase letter (immediately preceded by a lowercase letter)
# followed by an optional hyphen, one or more digits, and an optional
# trailing uppercase variant letter — all with no separating space.
_OPERATOR_TRAILING_AC: re.Pattern = re.compile(
    r"(?<=[a-z])[A-Z]-?\d+[A-Z]?\s*$"
)


def parse_operator(value: str | None) -> str | None: 
    """
    Clean the 'operator' field.

    In addition to the basic clean_text rules (strip whitespace, map '?' →
    NULL), this function nullifies values that are clearly *not* airline
    operators:

    - Pure serial / registration codes, e.g. ``'46826/109'``.
    - Aircraft manufacturer + model designator entries that were incorrectly
      placed in the operator column, e.g. ``'Boeing KC-135E'``,
      ``'Lockheed AC-130H Hercules'``, ``'Bristol 170 Freighter 31M'``,
      ``'Carvair ATL-98'``, ``'IAI 1124 Westwind'``,
      ``'North American Sabreliner 40'``, ``'Lockheed Loadstar'``.

    A trailing ICAO-style aircraft designator accidentally concatenated onto a
    legitimate operator name is stripped before returning, e.g.
    ``'…Turkish Air ForceC-47'`` → ``'…Turkish Air Force'``.
    """
    v = clean_text(value)
    if v is None:
        return None

    # Pure numeric / registration code → not an operator
    if _OPERATOR_PURE_CODE.match(v):
        return None

    # Aircraft manufacturer + model designation in operator field → not an operator
    if _OPERATOR_AC_TYPE.match(v):
        return None

    # Strip trailing run-on aircraft designator (e.g. '…Air ForceC-47')
    v = _OPERATOR_TRAILING_AC.sub("", v).strip()
    return v or None


# ---------------------------------------------------------------------------
# Flight-number parsing helpers
# ---------------------------------------------------------------------------

# Misplaced date entries in the flight_no column (e.g. '10-Jan', '2-Apr').
# strptime '%d-%b-%y' ambiguity causes dates to land here occasionally.
_FLIGHT_NO_DATE: re.Pattern = re.compile(
    r"^\d{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.IGNORECASE,
)


def parse_flight_no(value: str | None) -> str | None:
    """
    Clean the ``flight_no`` field and return a dash-free string, or None.

    Steps
    -----
    1. Apply basic ``clean_text`` rules (strip whitespace, ``'?'`` → None).
    2. Bare ``'-'`` (unknown marker) → None.
    3. Values starting with ``'?'`` (e.g. ``'?/8301'``) → None.
    4. Misplaced date strings (e.g. ``'10-Jan'``, ``'2-Apr'``) → None.
    5. The word ``'Charter'`` (operation type, not a flight number) → None.
    6. Remove all ``'-'`` characters from the remaining value.
    7. Strip leading/trailing ``'/ '`` or ``' /'`` artefacts left after
       removing an unknown sub-part (e.g. ``'686 / -'`` → ``'686'``).
    8. Collapse multiple spaces; return None if the result is empty.
    """
    v = clean_text(value)
    if v is None:
        return None

    # Bare unknown marker
    if v == "-":
        return None

    # Starts with '?' but not a bare '?' (e.g. '?/8301')
    if v.startswith("?"):
        return None

    # Misplaced date entry (e.g. '10-Jan', '8-Nov')
    if _FLIGHT_NO_DATE.match(v):
        return None

    # Operation type, not a flight number
    if v.lower() == "charter":
        return None

    # Remove all dashes
    v = v.replace("-", "")

    # Clean up artefacts from removed unknown sub-parts (e.g. '686 / -' → '686 / ' → '686')
    v = re.sub(r"\s*/\s*$", "", v)  # trailing ' /'
    v = re.sub(r"^\s*/\s*", "", v)  # leading '/ '
    # Collapse multiple spaces
    v = re.sub(r" {2,}", " ", v).strip()

    return v or None


# ---------------------------------------------------------------------------
# Route parsing helpers
# ---------------------------------------------------------------------------

# Operation-type labels that sometimes appear as the *entire* value or as
# a leading prefix before an actual origin-destination route.  The function
# strips the prefix when a real route follows, and returns None when nothing
# useful remains.
#
# Examples nullified (prefix only, no route):
#   'Positioning', 'Reconnaissance', 'Training', 'Training exercise',
#   'Training flight', 'Training/Aranchi', 'Test', 'Test Flight', 'Testing',
#   'Sightseeing', 'Sight seeing', 'Sighteeing', 'Sightseeing Fentress Airpark',
#   'Sightseeing over Rotterdam', 'Sky Diving', 'Skydiving', 'Survey',
#   'Shuttle', 'Search mission', 'Service mission', 'Traffic reporting',
#   'Practice maneuvers', 'Radio surveillance mission', 'Transporting troops'
#
# Examples where the prefix is stripped and the route is returned:
#   'Positiioning - San Jose - Honolulu'   → 'San Jose - Honolulu'
#   'Training -Montreal - Ottawa'          → 'Montreal - Ottawa'
#   'Sightseeing / Teteboro - Ocean City, NJ' → 'Teteboro - Ocean City, NJ'
_ROUTE_OP_PREFIX: re.Pattern = re.compile(
    r"^(?:"
    r"positi+oning"
    r"|practice\s+maneuvers?"
    r"|radio\s+surveillance\s+mission"
    r"|reconn?aiss?ance"
    r"|search\s+mission"
    r"|service\s+mission"
    r"|shuttle"
    r"|sight[\s]*see(?:ing|eing)?|sighteeing"  # sightseeing / sight seeing / sighteeing
    r"|sky\s*diving"
    r"|survey"
    r"|test(?:ing)?(?:\s+flight)?"
    r"|traffic\s+reporting"
    r"|training(?:\s+exercise|\s+flight|/\w+)?"
    r"|transporting\s+troops"
    r")"
    r"\s*(?:[/\-]\s*)?",                    # optional trailing separator
    re.IGNORECASE,
)

# Pure aircraft-registration code in the route field (e.g. 'VP-BPS').
_ROUTE_REGISTRATION: re.Pattern = re.compile(
    r"^[A-Z]{1,2}-[A-Z0-9]+$", re.IGNORECASE
)


def parse_route(value: str | None) -> str | None:
    """
    Clean the ``route`` field and return a normalised string, or None.

    Steps
    -----
    1. Apply basic ``clean_text`` rules (strip whitespace, ``'?'`` → None).
    2. Normalise whitespace around ``' - '`` separators (collapse runs of
       spaces so ``'A -  B'`` becomes ``'A - B'``).
    3. Remove a trailing period (typo artefact, e.g. ``'Tauranga.'``).
    4. Collapse doubled commas (``',,'`` → ``','``).
    5. Strip a leading operation-type prefix (``'Positioning'``,
       ``'Training'``, ``'Sightseeing'``, ``'Test'``, etc.)
       — if the remainder after stripping is non-empty, return it;
       if nothing remains, return None.
    6. Nullify pure aircraft-registration codes (e.g. ``'VP-BPS'``).
    """
    v = clean_text(value)
    if v is None:
        return None

    # Pure registration code — check before dash normalisation (e.g. 'VP-BPS')
    if _ROUTE_REGISTRATION.match(v.strip()):
        return None

    # Normalise whitespace around ' - ' separators
    v = re.sub(r"\s{2,}", " ", v)           # collapse interior runs of spaces
    v = re.sub(r"\s*-\s*", " - ", v)        # exactly one space either side of '-'
    v = v.strip(" -")                        # remove stray leading/trailing dashes

    # Trailing period
    v = v.rstrip(".")

    # Doubled commas
    v = re.sub(r",\s*,", ",", v)

    # Strip leading operation-type prefix
    stripped = _ROUTE_OP_PREFIX.sub("", v).strip(" -")
    if stripped != v:
        # Keep the remainder only when it looks like an O-D route (contains ' - ').
        # Bare location fragments (e.g. 'Fentress Airpark', 'over Rotterdam')
        # left after stripping the prefix are not routes → return None.
        return stripped if " - " in stripped else None

    return v.strip() or None


def parse_ac_type(value: str | None) -> str | None:
    """
    Clean the ``ac_type`` (aircraft type) field and return a normalised string,
    or None.

    Steps
    -----
    1. Apply basic ``clean_text`` rules (strip whitespace, ``'?'`` → None).
    2. Remove all parenthetical annotations – these appear as vehicle-category
       labels (``(flying boat)``, ``(airship)``, ``(amphibian)``,
       ``(freighter)``), helicopter noise (``(helicopter)``,
       ``(helilcopter)``, ``(helicopters)``), fleet-count suffixes
       (``(3 aircraft)``), and alternate ICAO designators (``(DC-3)``,
       ``(CV-340-79)``, ``(WL)``).
    3. Collapse runs of whitespace (e.g. ``'Aero Commander  520'``
       → ``'Aero Commander 520'``).
    4. Strip any residual leading / trailing whitespace.
    5. Return None if the result is empty.

    Multi-aircraft mid-air collision entries such as
    ``'Bell UH-1H / Bell UH-1H'`` are left intact (the slash separator
    is meaningful).
    """
    v = clean_text(value)
    if v is None:
        return None

    # Remove all parenthetical content (including nested cases)
    v = re.sub(r"\(.*?\)", "", v)

    # Collapse multiple whitespace characters into a single space
    v = re.sub(r"\s{2,}", " ", v).strip()

    return v or None


# ---------------------------------------------------------------------------
# Registration parsing helpers
# ---------------------------------------------------------------------------

# Misplaced date strings that appear in the registration field.
# 'DD-Mon' (e.g. '12-May') and 'D/M/YYYY' (e.g. '1/2/2003').
_REG_DATE: re.Pattern = re.compile(
    r"^\d{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$"
    r"|^\d{1,2}/\d{1,2}/\d{4}$",
    re.IGNORECASE,
)


def _normalise_single_reg(raw: str) -> str | None:
    """
    Normalise one registration token.

    - Strip whitespace and map ``'?'`` / empty → None.
    - Remove spaces around hyphens (e.g. ``'B- 305'`` → ``'B-305'``,
      ``'PT - CIH'`` → ``'PT-CIH'``).
    - US N / NC / NR prefixes with an errant space: drop the space
      (``'N 79069'`` → ``'N79069'``, ``'NC 21714'`` → ``'NC21714'``).
    - Multi-letter prefix (``CCCP``, ``FAC``, …) followed by a space:
      replace space with hyphen
      (``'CCCP 11000'`` → ``'CCCP-11000'``, ``'FAC 4020'`` → ``'FAC-4020'``).
    """
    v = raw.strip()
    if not v or v == "?":
        return None
    # Remove spaces adjacent to hyphens
    v = re.sub(r"\s*-\s*", "-", v)
    # US N/NC/NR prefix: just remove the errant space (no hyphen in standard)
    v = re.sub(r"^(N[CR]?)\s+", r"\1", v, flags=re.IGNORECASE)
    # Multi-letter prefix (CCCP, FAC, etc.) + space → insert hyphen
    v = re.sub(r"^([A-Z]{2,})\s+(?=[A-Z0-9])", r"\1-", v, flags=re.IGNORECASE)
    return v.strip() or None


def parse_registration(value: str | None) -> str | None:
    """
    Clean the ``registration`` field and return a normalised string, or None.

    Steps
    -----
    1. Apply basic ``clean_text`` rules (strip whitespace, ``'?'`` → None).
    2. Misplaced date strings (``'12-May'``, ``'1/2/2003'``) → None.
    3. Split on ``'/'`` to detect two-aircraft mid-air collision pairs
       (e.g. ``'N11360/N4862F'``, ``'C-GYYB/ C-GYPZ'``).
       Each side is normalised individually; ``'?'`` sides are dropped.
       If both sides are unknown the result is None.
       Valid pairs are rejoined as ``'A / B'``.
    4. Within each token, normalise spacing artefacts:
       - spaces around hyphens (``'B- 305'`` → ``'B-305'``),
       - US N / NC / NR prefix with space (``'N 79069'`` → ``'N79069'``),
       - multi-letter prefix with space (``'CCCP 11000'`` → ``'CCCP-11000'``).
    """
    v = clean_text(value)
    if v is None:
        return None

    # Misplaced date entry
    if _REG_DATE.match(v):
        return None

    # Split on '/' to handle two-aircraft collision pairs
    parts = [p.strip() for p in v.split("/")]

    if len(parts) == 1:
        return _normalise_single_reg(v)

    # Two (or more) aircraft — normalise each side, drop unknown halves
    cleaned = [_normalise_single_reg(p) for p in parts]
    cleaned = [c for c in cleaned if c is not None]
    if not cleaned:
        return None
    return " / ".join(cleaned)


# ---------------------------------------------------------------------------
# Construction / line number parsing helpers
# ---------------------------------------------------------------------------

# Misplaced date strings in the cn_ln column (e.g. '2-Jan', '7-May').
_CN_LN_DATE: re.Pattern = re.compile(
    r"^\d{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$",
    re.IGNORECASE,
)


def parse_cn_ln(value: str | None) -> str | None:
    """
    Clean the ``cn_ln`` (construction / line number) field and return a
    normalised, hyphen-free string, or None.

    Steps
    -----
    1. Apply basic ``clean_text`` rules (strip whitespace, ``'?'`` → None).
    2. Misplaced date strings (``'2-Jan'``, ``'7-May'``) → None.
    3. Strip parenthetical annotations (e.g. ``'1533 (KLM-1)'`` → ``'1533'``).
    4. Remove trailing ``'/?'`` or ``'/ '`` artefacts left by an unknown
       line-number suffix (e.g. ``'2528 /?'`` → ``'2528'``,
       ``'18365 /'`` → ``'18365'``).
    5. Split on ``' / '`` to detect two-aircraft mid-air collision pairs
       (e.g. ``'19643/11 / 20400/157'``).  Each side is processed
       individually; ``'?'`` sides are dropped.  If both sides are unknown
       the result is None.  Valid pairs are rejoined as ``'A / B'``.
    6. Remove all ``'-'`` characters from every segment.
    7. Collapse runs of whitespace; return None if the result is empty.
    """
    v = clean_text(value)
    if v is None:
        return None

    # Misplaced date entry
    if _CN_LN_DATE.match(v):
        return None

    # Strip parenthetical annotations (e.g. ' (KLM-1)')
    v = re.sub(r"\s*\(.*?\)", "", v).strip()

    # Strip trailing '/ ?' or '/' artefacts (unknown ln suffix)
    v = re.sub(r"\s*/\s*\??$", "", v).strip()

    # Split on ' / ' for multi-aircraft collision pairs
    parts = [p.strip() for p in re.split(r"\s+/\s+", v)]

    result_parts: list[str] = []
    for part in parts:
        if not part or part == "?":
            continue
        # Remove all hyphens
        part = part.replace("-", "")
        # Collapse multiple whitespace characters
        part = re.sub(r"\s{2,}", " ", part).strip()
        if part:
            result_parts.append(part)

    if not result_parts:
        return None
    return " / ".join(result_parts)

def parse_count_field(value: str | None) -> tuple[int | None, int | None, int | None]:
    """
    Parse 'aboard' / 'fatalities' fields.
    Returns (total, passengers, crew) as int or None.
    """
    # Regex to extract N and the breakdown tuple from 'N ▸ (passengers:X▸ crew:Y)'
    # The '▸' character can be various Unicode bullets; use a broad match.
    abord_regex = re.compile(
        r"(\d+)\s*.+passengers:(\?|\d+).+crew:(\?|\d+)",
        re.IGNORECASE,
    )
    if not value or value.strip() in ("?", ""):
        return None, None, None
    m = abord_regex.search(value)
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


def parse_summary(value: str | None) -> str | None:
    """
    Clean the ``summary`` field and return a normalised string, or None.

    Steps
    -----
    1. Apply basic ``clean_text`` rules (strip whitespace, ``'?'`` → None).
    2. Replace newline and tab characters with a single space.
    3. Collapse runs of multiple consecutive spaces into one space.
    4. Collapse repeated periods (``'..'``) into a single period.
    5. Strip any residual leading / trailing whitespace.
    6. Return None if the result is empty.
    """
    v = clean_text(value)
    if v is None:
        return None

    # Replace newlines and tabs with a space
    v = v.replace("\n", " ").replace("\t", " ")

    # Collapse multiple consecutive spaces
    v = re.sub(r" {2,}", " ", v)

    # Collapse repeated periods (e.g. '..') into a single period
    v = re.sub(r"\.{2,}", ".", v)

    return v.strip() or None


def get_unique_values(
    cursor: "sqlite3.Cursor", table: str, field: str
) -> list:
    """
    Return the unique values found in *field* of *table*.

    Parameters
    ----------
    cursor : open sqlite3.Cursor
    table  : table name to query
    field  : column name to inspect

    Returns
    -------
    Sorted list of unique values (NULLs excluded).
    """
    cursor.execute(
        f'SELECT DISTINCT "{field}" FROM "{table}" WHERE "{field}" IS NOT NULL ORDER BY "{field}"'
    )
    return [row[0] for row in cursor.fetchall()]


def safe_sum(*args: int | None) -> int:
    """Sum integers treating None as 0."""
    return sum((x or 0) for x in args)


def check_no_duplicates(conn: "sqlite3.Connection", table: str) -> bool:
    """
    Check that *table* contains no fully-duplicate rows.

    Parameters
    ----------
    conn  : open sqlite3.Connection
    table : table name to inspect

    Returns
    -------
    True  – every row is unique.
    False – duplicates exist (the repeated rows are printed before returning).
    """
    import sqlite3

    cur = conn.cursor()

    # Retrieve column names via PRAGMA
    cur.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cur.fetchall()]
    if not columns:
        raise ValueError(f"Table '{table}' not found or has no columns.")

    cols_csv = ", ".join(f'"{c}"' for c in columns)

    query = f"""
        SELECT {cols_csv}, COUNT(*) AS _dup_count
        FROM "{table}"
        GROUP BY {cols_csv}
        HAVING COUNT(*) > 1
    """
    cur.execute(query)
    duplicates = cur.fetchall()

    if not duplicates:
        return True

    # Print duplicate rows with their repetition count
    header = columns + ["_dup_count"]
    col_widths = [max(len(str(h)), max((len(str(row[i])) for row in duplicates), default=0))
                  for i, h in enumerate(header)]
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(f"\nDuplicate rows found in '{table}':")
    print(fmt.format(*header))
    print("  ".join("-" * w for w in col_widths))
    for row in duplicates:
        print(fmt.format(*row))
    print(f"\nTotal duplicate groups: {len(duplicates)}")
    return False