"""
validation.py
-------------
Validates the cleaned plane crashes SQLite database.

Checks performed
────────────────
1.  Schema  expected columns and declared types are present.
2.  Type    INTEGER columns contain only integers or NULLs;
              TEXT columns contain only strings or NULLs.
3.  Date    non-NULL dates match YYYY-MM-DD and fall in 1908-2018.
4.  Time    non-NULL times match HH:MM (24-hour).
5.  Ranges  numeric columns are non-negative.
6.  Totals  aboard_passengers + aboard_crew = aboard_total
              fatalities_passengers + fatalities_crew = fatalities_aboard
              fatalities_aboard <= aboard_total
              fatalities_total = fatalities_aboard + ground (where all non-NULL)
7.  Duplicates rows sharing (date, operator, route) flagged as potential duplicates.
"""

import re
import sqlite3
from pathlib import Path
from dataclasses import dataclass, field


EXPECTED_SCHEMA: dict[str, str] = {
    "date":                   "TEXT",
    "time":                   "TEXT",
    "location":               "TEXT",
    "operator":               "TEXT",
    "flight_no":              "TEXT",
    "route":                  "TEXT",
    "ac_type":                "TEXT",
    "registration":           "TEXT",
    "cn_ln":                  "TEXT",
    "aboard_total":           "INTEGER",
    "aboard_passengers":      "INTEGER",
    "aboard_crew":            "INTEGER",
    "fatalities_aboard":      "INTEGER",
    "fatalities_passengers":  "INTEGER",
    "fatalities_crew":        "INTEGER",
    "ground":                 "INTEGER",
    "fatalities_total":       "INTEGER",
    "summary":                "TEXT",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}$")


# ── Result container ──────────────────────────────────────────────────────────

@dataclass
class ValidationReport:
    passed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def ok(self, msg: str) -> None:
        self.passed.append(msg)

    def fail(self, msg: str) -> None:
        self.failed.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def print_summary(self) -> None:
        width = 70
        print("=" * width)
        print("VALIDATION REPORT")
        print("=" * width)

        print(f"\n  PASSED  ({len(self.passed)})")
        for m in self.passed:
            print(f"    [OK]   {m}")

        if self.warnings:
            print(f"\n  WARNINGS  ({len(self.warnings)})")
            for m in self.warnings:
                print(f"    [WARN] {m}")

        if self.failed:
            print(f"\n  FAILED  ({len(self.failed)})")
            for m in self.failed:
                print(f"    [FAIL] {m}")

        print("\n" + "-" * width)
        status = "PASS" if not self.failed else "FAIL"
        print(f"  Result: {status}  |  "
              f"Passed: {len(self.passed)}  |  "
              f"Warnings: {len(self.warnings)}  |  "
              f"Failed: {len(self.failed)}")
        print("=" * width)


# ── Individual checks ─────────────────────────────────────────────────────────

def check_schema(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """Check that all expected columns exist with the correct declared type."""
    cur.execute(f"PRAGMA table_info({table})")
    actual = {row[1]: row[2].upper() for row in cur.fetchall()}

    for col, expected_type in EXPECTED_SCHEMA.items():
        if col not in actual:
            report.fail(f"Schema: column '{col}' is missing")
        elif actual[col] != expected_type.upper():
            report.fail(
                f"Schema: '{col}' declared as {actual[col]}, expected {expected_type}"
            )
        else:
            report.ok(f"Schema: '{col}' → {expected_type}")

    extra = set(actual) - set(EXPECTED_SCHEMA)
    for col in extra:
        report.warn(f"Schema: unexpected column '{col}' ({actual[col]})")


def check_python_types(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """Fetch all rows and verify Python-level types match declared SQLite types."""
    integer_cols = [c for c, t in EXPECTED_SCHEMA.items() if t == "INTEGER"]
    text_cols    = [c for c, t in EXPECTED_SCHEMA.items() if t == "TEXT"]

    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    int_errors: dict[str, int] = {}
    txt_errors: dict[str, int] = {}

    for row in rows:
        for col in integer_cols:
            val = row[col]
            if val is not None and not isinstance(val, int):
                int_errors[col] = int_errors.get(col, 0) + 1
        for col in text_cols:
            val = row[col]
            if val is not None and not isinstance(val, str):
                txt_errors[col] = txt_errors.get(col, 0) + 1

    for col in integer_cols:
        if col in int_errors:
            report.fail(f"Types: '{col}' has {int_errors[col]} non-integer value(s)")
        else:
            report.ok(f"Types: '{col}' all values are INTEGER or NULL")

    for col in text_cols:
        if col in txt_errors:
            report.fail(f"Types: '{col}' has {txt_errors[col]} non-text value(s)")
        else:
            report.ok(f"Types: '{col}' all values are TEXT or NULL")


def check_date_format(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """Validate date format (YYYY-MM-DD) and range (1908–2018)."""
    cur.execute(f"SELECT date FROM {table} WHERE date IS NOT NULL")
    dates = [row[0] for row in cur.fetchall()]

    bad_format = [d for d in dates if not DATE_RE.match(d)]
    if bad_format:
        report.fail(
            f"Date format: {len(bad_format)} value(s) do not match YYYY-MM-DD "
            f"(e.g. {bad_format[:3]})"
        )
    else:
        report.ok(f"Date format: all {len(dates)} non-NULL dates match YYYY-MM-DD")

    years = []
    for d in dates:
        try:
            years.append(int(d[:4]))
        except ValueError:
            pass

    out_of_range = [y for y in years if not (1908 <= y <= 2018)]
    if out_of_range:
        report.warn(
            f"Date range: {len(out_of_range)} date(s) outside 1908-2018 "
            f"(e.g. {sorted(set(out_of_range))[:5]})"
        )
    else:
        report.ok(f"Date range: all years are within 1908–2018")


def check_time_format(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """Validate time format (HH:MM)."""
    cur.execute(f"SELECT time FROM {table} WHERE time IS NOT NULL")
    times = [row[0] for row in cur.fetchall()]

    bad = [t for t in times if not TIME_RE.match(t)]
    if bad:
        report.fail(
            f"Time format: {len(bad)} value(s) do not match HH:MM "
            f"(e.g. {bad[:3]})"
        )
    else:
        report.ok(f"Time format: all {len(times)} non-NULL times match HH:MM")


def check_non_negative(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """All numeric columns must be >= 0."""
    int_cols = [c for c, t in EXPECTED_SCHEMA.items() if t == "INTEGER"]
    for col in int_cols:
        cur.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL AND {col} < 0"
        )
        n = cur.fetchone()[0]
        if n:
            report.fail(f"Non-negative: '{col}' has {n} negative value(s)")
        else:
            report.ok(f"Non-negative: '{col}' ≥ 0")


def check_totals_consistency(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """Cross-check computed totals against component parts."""

    checks = [
        (
            "aboard_total = aboard_passengers + aboard_crew",
            f"""SELECT COUNT(*) FROM {table}
                WHERE aboard_total IS NOT NULL
                  AND aboard_passengers IS NOT NULL
                  AND aboard_crew IS NOT NULL
                  AND aboard_total != aboard_passengers + aboard_crew""",
        ),
        (
            "fatalities_aboard = fatalities_passengers + fatalities_crew",
            f"""SELECT COUNT(*) FROM {table}
                WHERE fatalities_aboard IS NOT NULL
                  AND fatalities_passengers IS NOT NULL
                  AND fatalities_crew IS NOT NULL
                  AND fatalities_aboard != fatalities_passengers + fatalities_crew""",
        ),
        (
            "fatalities_aboard <= aboard_total",
            f"""SELECT COUNT(*) FROM {table}
                WHERE fatalities_aboard IS NOT NULL
                  AND aboard_total IS NOT NULL
                  AND fatalities_aboard > aboard_total""",
        ),
        (
            "fatalities_total = fatalities_aboard + ground",
            f"""SELECT COUNT(*) FROM {table}
                WHERE fatalities_total IS NOT NULL
                  AND fatalities_aboard IS NOT NULL
                  AND ground IS NOT NULL
                  AND fatalities_total != fatalities_aboard + ground""",
        ),
    ]

    for label, query in checks:
        cur.execute(query)
        n = cur.fetchone()[0]
        if n:
            report.warn(f"Totals: '{label}' violated by {n} row(s)")
        else:
            report.ok(f"Totals: '{label}'")


def check_duplicates(cur: sqlite3.Cursor, report: ValidationReport, table: str) -> None:
    """Flag rows sharing (date, operator, route) as potential duplicates."""
    cur.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT date, operator, route
            FROM {table}
            WHERE date IS NOT NULL AND operator IS NOT NULL AND route IS NOT NULL
            GROUP BY date, operator, route
            HAVING COUNT(*) > 1
        )
    """)
    n = cur.fetchone()[0]
    if n:
        report.warn(
            f"Duplicates: {n} (date, operator, route) group(s) appear more than once"
        )
    else:
        report.ok("Duplicates: no duplicate (date, operator, route) combinations found")


# ── Runner ────────────────────────────────────────────────────────────────────

def run_validation(db_path: str | Path, table: str) -> ValidationReport:
    if isinstance(db_path, str):
        db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    report = ValidationReport()

    print(f"Validating: {db_path.resolve()}\n")

    check_schema(cur, report, table)
    check_python_types(cur, report, table)
    check_date_format(cur, report, table)
    check_time_format(cur, report, table)
    check_non_negative(cur, report, table)
    check_totals_consistency(cur, report, table)
    check_duplicates(cur, report, table)

    conn.close()
    return report


if __name__ == "__main__":
    
    report = run_validation(Path("output/cleaned_plane_crashes.db"), "data")
    report.print_summary()
