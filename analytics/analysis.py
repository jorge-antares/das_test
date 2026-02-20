"""
analysis.py
-----------
Analyses and profiles the cleaned plane crashes SQLite database.

Sections
────────
1.  Data Profiling          NULL distribution per column
2.  Descriptive Stats       fatality rate, survival rate, ground casualties,
                            crew vs passenger fatality ratio
3.  Trend Analysis          crashes & fatalities per year, deadliest decades,
                            top operators, most dangerous aircraft types
4.  Geographic Analysis     top crash locations (country-level estimate)
5.  Data Quality            mismatched totals, potential duplicate rows,
                            registration reuse across aircraft types
"""

import sys
import sqlite3
import contextlib
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "utilspc"))
from utilspc.cleanerclass import BaseSQLite



TABLE   = "data"
SEP   = "─" * 70
THICK = "═" * 70


def section(title: str) -> None:
    print(f"\n{THICK}")
    print(f"  {title}")
    print(THICK)


def sub(title: str) -> None:
    print(f"\n  {title}")
    print(f"  {SEP[:len(title) + 2]}")


def fmt_pct(num: float | None, den: float | None, decimals: int = 1) -> str:
    if not num or not den or den == 0:
        return "  N/A"
    return f"{num / den * 100:.{decimals}f}%"


def print_table(headers: list[str], rows: list[tuple], col_widths: list[int]) -> None:
    fmt     = "  " + "  ".join(f"{{:<{w}}}" for w in col_widths)
    divider = "  " + "  ".join("-" * w for w in col_widths)
    if headers:
        print(fmt.format(*[str(h) for h in headers]))
        print(divider)
    for row in rows:
        print(fmt.format(*[str(v) if v is not None else "NULL" for v in row]))


# ── 1. Data Profiling ─────────────────────────────────────────────────────────

def profile_nulls(conn: sqlite3.Connection) -> None:
    section("1. DATA PROFILING — NULL Distribution")
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    total = cur.fetchone()[0]
    print(f"\n  Total rows: {total:,}\n")

    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [(r[1], r[2]) for r in cur.fetchall()]

    headers    = ["Column", "Type", "Non-NULL", "NULL", "NULL %", "Unique"]
    col_widths = [26, 8, 10, 8, 8, 8]
    print_table(headers, [], col_widths)

    for col_name, col_type in columns:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE {col_name} IS NOT NULL")
        non_null = cur.fetchone()[0]
        n_null   = total - non_null
        cur.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {TABLE}")
        n_unique = cur.fetchone()[0]
        pct      = f"{n_null / total * 100:.1f}%" if total else "N/A"
        print_table([], [(col_name, col_type, f"{non_null:,}", f"{n_null:,}", pct, f"{n_unique:,}")], col_widths)


# ── 2. Descriptive Statistics ─────────────────────────────────────────────────

def descriptive_stats(conn: sqlite3.Connection) -> None:
    section("2. DESCRIPTIVE STATISTICS")
    cur = conn.cursor()

    # Overall fatality rate
    sub("Fatality Rate (fatalities_aboard / aboard_total)")
    cur.execute(f"""
        SELECT
            SUM(fatalities_aboard) AS total_fat,
            SUM(aboard_total)      AS total_aboard,
            AVG(CAST(fatalities_aboard AS REAL) / aboard_total) AS avg_rate
        FROM {TABLE}
        WHERE fatalities_aboard IS NOT NULL AND aboard_total IS NOT NULL AND aboard_total > 0
    """)
    r = cur.fetchone()
    print(f"  Aggregate fatality rate : {fmt_pct(r['total_fat'], r['total_aboard'])}")
    print(f"  Average per-flight rate : {r['avg_rate'] * 100:.1f}%" if r['avg_rate'] else "  N/A")

    # Survival rate
    sub("Survival Rate ((aboard_total - fatalities_aboard) / aboard_total)")
    cur.execute(f"""
        SELECT
            SUM(aboard_total - fatalities_aboard) AS survivors,
            SUM(aboard_total)                     AS total_aboard
        FROM {TABLE}
        WHERE fatalities_aboard IS NOT NULL AND aboard_total IS NOT NULL AND aboard_total > 0
    """)
    r = cur.fetchone()
    print(f"  Aggregate survival rate : {fmt_pct(r['survivors'], r['total_aboard'])}")

    # Ground casualties
    sub("Ground Casualties")
    cur.execute(f"""
        SELECT
            COUNT(*)        AS crashes_with_ground,
            SUM(ground)     AS total_ground,
            MAX(ground)     AS max_ground,
            AVG(ground)     AS avg_ground
        FROM {TABLE}
        WHERE ground IS NOT NULL AND ground > 0
    """)
    r = cur.fetchone()
    print(f"  Crashes with ground fatalities : {r['crashes_with_ground']:,}")
    print(f"  Total ground fatalities        : {r['total_ground']:,}")
    print(f"  Max ground fatalities (single) : {r['max_ground']:,}")
    print(f"  Avg ground fatalities          : {r['avg_ground']:.2f}" if r['avg_ground'] else "  N/A")

    # Crew vs passenger fatality ratio
    sub("Crew vs Passenger Fatality Ratio")
    cur.execute(f"""
        SELECT
            SUM(fatalities_passengers) AS pax_fat,
            SUM(fatalities_crew)       AS crew_fat
        FROM {TABLE}
        WHERE fatalities_passengers IS NOT NULL AND fatalities_crew IS NOT NULL
    """)
    r = cur.fetchone()
    total = (r['pax_fat'] or 0) + (r['crew_fat'] or 0)
    print(f"  Passenger fatalities : {r['pax_fat']:,}  ({fmt_pct(r['pax_fat'], total)})")
    print(f"  Crew fatalities      : {r['crew_fat']:,}  ({fmt_pct(r['crew_fat'], total)})")

    # Deadliest single crashes
    sub("Top 10 Deadliest Single Crashes")
    cur.execute(f"""
        SELECT date, operator, ac_type, location, fatalities_total
        FROM {TABLE}
        WHERE fatalities_total IS NOT NULL
        ORDER BY fatalities_total DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    print_table(
        ["Date", "Operator", "Aircraft", "Location", "Fatalities"],
        [(row["date"], (row["operator"] or "")[:28], (row["ac_type"] or "")[:22],
          (row["location"] or "")[:28], row["fatalities_total"]) for row in rows],
        [12, 30, 24, 30, 10],
    )


# ── 3. Trend Analysis ─────────────────────────────────────────────────────────

def trend_analysis(conn: sqlite3.Connection) -> None:
    section("3. TREND ANALYSIS")
    cur = conn.cursor()

    # Crashes per decade
    sub("Crashes & Fatalities per Decade")
    cur.execute(f"""
        SELECT
            (CAST(SUBSTR(date, 1, 4) AS INTEGER) / 10) * 10 AS decade,
            COUNT(*)                                         AS crashes,
            SUM(fatalities_total)                            AS fatalities
        FROM {TABLE}
        WHERE date IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """)
    rows = cur.fetchall()
    print_table(
        ["Decade", "Crashes", "Total Fatalities"],
        [(f"{r['decade']}s", f"{r['crashes']:,}", f"{r['fatalities']:,}" if r['fatalities'] else "N/A") for r in rows],
        [10, 10, 18],
    )

    # Crashes per year (bar chart using ASCII)
    sub("Crashes per Year (1940–2018, ASCII bar)")
    cur.execute(f"""
        SELECT SUBSTR(date, 1, 4) AS year, COUNT(*) AS crashes
        FROM {TABLE}
        WHERE date IS NOT NULL AND CAST(SUBSTR(date, 1, 4) AS INTEGER) >= 1940
        GROUP BY year
        ORDER BY year
    """)
    year_rows = cur.fetchall()
    max_crashes = max(r["crashes"] for r in year_rows) if year_rows else 1
    bar_width   = 40
    for r in year_rows:
        bar = "█" * round(r["crashes"] / max_crashes * bar_width)
        print(f"  {r['year']} │{bar:<{bar_width}} {r['crashes']}")

    # Top 15 operators by crash count
    sub("Top 15 Operators by Crash Count")
    cur.execute(f"""
        SELECT operator, COUNT(*) AS crashes, SUM(fatalities_total) AS fatalities
        FROM {TABLE}
        WHERE operator IS NOT NULL
        GROUP BY operator
        ORDER BY crashes DESC
        LIMIT 15
    """)
    rows = cur.fetchall()
    print_table(
        ["Operator", "Crashes", "Total Fatalities"],
        [(r["operator"][:40], f"{r['crashes']:,}", f"{r['fatalities']:,}" if r["fatalities"] else "N/A") for r in rows],
        [42, 10, 18],
    )

    # Most dangerous aircraft types (min 10 incidents)
    sub("Top 15 Aircraft Types by Fatality Rate (min 10 incidents)")
    cur.execute(f"""
        SELECT
            ac_type,
            COUNT(*)                                                         AS incidents,
            SUM(fatalities_aboard)                                           AS total_fat,
            SUM(aboard_total)                                                AS total_aboard,
            CAST(SUM(fatalities_aboard) AS REAL) / NULLIF(SUM(aboard_total), 0) AS fat_rate
        FROM {TABLE}
        WHERE ac_type IS NOT NULL
          AND fatalities_aboard IS NOT NULL
          AND aboard_total IS NOT NULL
        GROUP BY ac_type
        HAVING incidents >= 10
        ORDER BY fat_rate DESC
        LIMIT 15
    """)
    rows = cur.fetchall()
    print_table(
        ["Aircraft Type", "Incidents", "Fatalities", "Aboard", "Rate"],
        [(r["ac_type"][:38], r["incidents"], r["total_fat"], r["total_aboard"],
          f"{r['fat_rate'] * 100:.1f}%") for r in rows],
        [40, 10, 12, 10, 7],
    )


# ── 4. Geographic Analysis ────────────────────────────────────────────────────

def geographic_analysis(conn: sqlite3.Connection) -> None:
    section("4. GEOGRAPHIC ANALYSIS")
    cur = conn.cursor()

    # Top 20 locations by crash count (last token after last comma ≈ country)
    sub("Top 20 Countries / Regions by Crash Count (last field in location)")
    cur.execute(f"SELECT location FROM {TABLE} WHERE location IS NOT NULL")
    from collections import Counter
    country_counter: Counter = Counter()
    for row in cur.fetchall():
        parts = [p.strip() for p in row["location"].split(",")]
        country_counter[parts[-1]] += 1

    print_table(
        ["Country / Region", "Crashes"],
        [(loc, f"{cnt:,}") for loc, cnt in country_counter.most_common(20)],
        [40, 10],
    )

    # Top 20 specific crash sites
    sub("Top 20 Specific Crash Locations")
    cur.execute(f"""
        SELECT location, COUNT(*) AS crashes
        FROM {TABLE}
        WHERE location IS NOT NULL
        GROUP BY location
        ORDER BY crashes DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    print_table(
        ["Location", "Crashes"],
        [(r["location"][:52], f"{r['crashes']:,}") for r in rows],
        [54, 10],
    )


# ── 5. Data Quality ───────────────────────────────────────────────────────────

def data_quality(conn: sqlite3.Connection) -> None:
    section("5. DATA QUALITY")
    cur = conn.cursor()

    # Mismatched aboard totals
    sub("Rows Where aboard_total ≠ aboard_passengers + aboard_crew")
    cur.execute(f"""
        SELECT date, operator, aboard_total, aboard_passengers, aboard_crew
        FROM {TABLE}
        WHERE aboard_total IS NOT NULL
          AND aboard_passengers IS NOT NULL
          AND aboard_crew IS NOT NULL
          AND aboard_total != aboard_passengers + aboard_crew
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        print_table(
            ["Date", "Operator", "Total", "Pax", "Crew"],
            [(r["date"], (r["operator"] or "")[:30], r["aboard_total"], r["aboard_passengers"], r["aboard_crew"]) for r in rows],
            [12, 32, 7, 7, 7],
        )
        cur.execute(f"""
            SELECT COUNT(*) FROM {TABLE}
            WHERE aboard_total IS NOT NULL AND aboard_passengers IS NOT NULL
              AND aboard_crew IS NOT NULL
              AND aboard_total != aboard_passengers + aboard_crew
        """)
        total_mismatch = cur.fetchone()[0]
        print(f"\n  Total mismatched rows: {total_mismatch:,}")
    else:
        print("  No mismatches found.")

    # Mismatched fatality totals
    sub("Rows Where fatalities_aboard ≠ fatalities_passengers + fatalities_crew")
    cur.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE fatalities_aboard IS NOT NULL
          AND fatalities_passengers IS NOT NULL
          AND fatalities_crew IS NOT NULL
          AND fatalities_aboard != fatalities_passengers + fatalities_crew
    """)
    n = cur.fetchone()[0]
    print(f"  Total mismatched rows: {n:,}")

    # Fatalities exceed aboard count
    sub("Rows Where fatalities_aboard > aboard_total")
    cur.execute(f"""
        SELECT date, operator, aboard_total, fatalities_aboard
        FROM {TABLE}
        WHERE fatalities_aboard IS NOT NULL
          AND aboard_total IS NOT NULL
          AND fatalities_aboard > aboard_total
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        print_table(
            ["Date", "Operator", "Aboard", "Fatalities"],
            [(r["date"], (r["operator"] or "")[:30], r["aboard_total"], r["fatalities_aboard"]) for r in rows],
            [12, 32, 8, 12],
        )
    else:
        print("  No rows where fatalities exceed aboard count.")

    # Duplicate (date, operator, route)
    sub("Potential Duplicates — same (date, operator, route)")
    cur.execute(f"""
        SELECT date, operator, route, COUNT(*) AS occurrences
        FROM {TABLE}
        WHERE date IS NOT NULL AND operator IS NOT NULL AND route IS NOT NULL
        GROUP BY date, operator, route
        HAVING occurrences > 1
        ORDER BY occurrences DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        print_table(
            ["Date", "Operator", "Route", "Count"],
            [(r["date"], (r["operator"] or "")[:28], (r["route"] or "")[:28], r["occurrences"]) for r in rows],
            [12, 30, 30, 7],
        )
    else:
        print("  No duplicate (date, operator, route) combinations found.")

    # Registration reuse across aircraft types
    sub("Registrations Used Across Multiple Aircraft Types (top 10)")
    cur.execute(f"""
        SELECT registration, COUNT(DISTINCT ac_type) AS type_count, GROUP_CONCAT(DISTINCT ac_type) AS types
        FROM {TABLE}
        WHERE registration IS NOT NULL AND ac_type IS NOT NULL
        GROUP BY registration
        HAVING type_count > 1
        ORDER BY type_count DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        print_table(
            ["Registration", "# Types", "Aircraft Types"],
            [(r["registration"], r["type_count"], r["types"][:50]) for r in rows],
            [14, 9, 52],
        )
    else:
        print("  No registrations reused across multiple aircraft types.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main_analysis(db_path: str, output_file: str = "data_profile.txt") -> None:
    with open(output_file, "w", encoding="utf-8") as f:
            with contextlib.redirect_stdout(f):
                db = BaseSQLite(db_path)
                conn = db.get_connection()
                conn.row_factory = sqlite3.Row
                print(f"\n{THICK}")
                print(f"  PLANE CRASHES DATASET — ANALYSIS & PROFILING")
                print(f"  Source: {db.db_path.resolve()}")
                print(THICK)

                profile_nulls(conn)
                descriptive_stats(conn)
                trend_analysis(conn)
                geographic_analysis(conn)
                data_quality(conn)

                conn.close()
                print(f"\n{THICK}\n  Analysis complete.\n{THICK}\n")


if __name__ == "__main__":
    pass
