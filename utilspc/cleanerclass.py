"""
cleandb.py
----------
Reads rawdata/plane_crashes_data.db, enforces consistent data types,
cleans all columns, and writes the result to cleaned_plane_crashes.db.

Cleaning rules per column
─────────────────────────
date         : 'DD-Mon-YY' → ISO 'YYYY-MM-DD' (TEXT).
               Python strptime maps YY 00-68 → 20xx, 69-99 → 19xx;
               years beyond 2018 (the dataset's last year) are shifted
               back 100 years to cover 1919-1999.
               Note: 1908-1918 entries share the same two-digit suffix as
               2008-2018 entries and are stored as 2008-2018 in the output
               due to format ambiguity inherent in the source data.
time         : strip leading 'c'/'c ' (≈ approximate), normalise bare
               'HHMM' integers to 'HH:MM', then validate HH:MM format.
               Unknown ('?') → NULL.
aboard       : 'N ▸ (passengers:X▸ crew:Y)' → three INTEGER columns:
               aboard_total, aboard_passengers, aboard_crew. '?' → NULL.
fatalities   : same pattern → fatalities_aboard, fatalities_passengers,
               fatalities_crew. '?' → NULL.
ground       : '?' → NULL; otherwise INTEGER.
Text fields  : strip surrounding whitespace; '?' → NULL.
               (location, operator, flight_no, route, ac_type,
                registration, cn_ln, summary)
"""

import os
import csv
import sqlite3
from pathlib import Path
from .cleanfun import (
    safe_sum,
    clean_text,
    parse_date,
    parse_time,
    parse_count_field,
    parse_ground,
    )


class BaseSQLite:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
    
    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)
    
    def exists_db(self) -> bool:
        if not self.db_path.is_file():
            return False
        return True
    



class DataSQLiteCleaner:
    def __init__(self, src_db_path: str, src_table: str, dst_db_path: str, dst_table: str) -> None:
        self.src_db = BaseSQLite(src_db_path)
        if not self.src_db.exists_db():
            raise FileNotFoundError(f"Source database not found: {src_db_path}")
        self.src_table = src_table
        
        # Extract destination directory and ensure it exists
        dst_dir = Path(dst_db_path).parent
        if not dst_dir.exists():
            dst_dir.mkdir(parents=True, exist_ok=True)
            print(f"Created destination directory: {dst_dir.resolve()}")
        self.dst_db = BaseSQLite(dst_db_path)
        if self.dst_db.exists_db():
            os.remove(self.dst_db.db_path)  # Remove existing destination DB to start fresh
            print(f"Existing destination database removed: {dst_db_path}. Added a new one.")
        self.dst_table = dst_table

    def get_create_table_query(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.dst_table} (
            date                TEXT,
            time                TEXT,
            location            TEXT,
            operator            TEXT,
            flight_no           TEXT,
            route               TEXT,
            ac_type             TEXT,
            registration        TEXT,
            cn_ln               TEXT,
            aboard_total        INTEGER,
            aboard_passengers   INTEGER,
            aboard_crew         INTEGER,
            fatalities_aboard    INTEGER,
            fatalities_passengers INTEGER,
            fatalities_crew     INTEGER,
            ground              INTEGER,
            fatalities_total    INTEGER,
            summary             TEXT
        )
        """
    
    def get_field_description(self) -> dict[str, str]:
        return {
            "date": "Date of the crash (ISO format YYYY-MM-DD)",
            "time": "Time of the crash (HH:MM, 24-hour format)",
            "location": "Location of the crash",
            "operator": "Airline or operator",
            "flight_no": "Flight number assigned by the aircraft operator",
            "route": "Complete or partial route flown prior to the accident",
            "ac_type": "Aircraft type",
            "registration": "ICAO registration of the aircraft",
            "cn_ln": "Construction or serial number / Line or fuselage number",
            "aboard_total": "Total number of people aboard",
            "aboard_passengers": "Number of passengers aboard",
            "aboard_crew": "Number of crew aboard",
            "fatalities_aboard": "Total number of fatalities aboard",
            "fatalities_passengers": "Number of passenger fatalities",
            "fatalities_crew": "Number of crew fatalities",
            "ground": "Number of ground fatalities (people killed on the ground)",
            "fatalities_total": "Total number of fatalities",
            "summary": "Brief description of the accident and cause if known",
        }
    
    def get_insert_row_query(self) -> str:
        return f"""
        INSERT INTO {self.dst_table} VALUES (
            :date, :time, :location, :operator, :flight_no, :route,
            :ac_type, :registration, :cn_ln,
            :aboard_total, :aboard_passengers, :aboard_crew,
            :fatalities_aboard, :fatalities_passengers, :fatalities_crew,
            :ground, :fatalities_total, :summary
        )
        """
    
    def clean_and_insert(self) -> None:
        with self.src_db.get_connection() as conn:
            # Source
            conn.row_factory = sqlite3.Row
            src_cur = conn.cursor()
            src_cur.execute(f"SELECT * FROM {self.src_table}")
            rows = src_cur.fetchall()

            # Destination
            dst_conn = self.dst_db.get_connection()
            dst_cur = dst_conn.cursor()
            dst_cur.execute(self.get_create_table_query())
            

            cleaned_rows = []
            skipped = 0

            for row in rows:
                try:
                    aboard_total, aboard_pax, aboard_crew = parse_count_field(row["aboard"])
                    fat_aboard, fat_pax, fat_crew         = parse_count_field(row["fatalities"])
                    fat_total = safe_sum(fat_aboard, parse_ground(row["ground"]))

                    cleaned_rows.append({
                        "date":                  parse_date(row["date"]),
                        "time":                  parse_time(row["time"]),
                        "location":              clean_text(row["location"]),
                        "operator":              clean_text(row["operator"]),
                        "flight_no":             clean_text(row["flight_no"]),
                        "route":                 clean_text(row["route"]),
                        "ac_type":               clean_text(row["ac_type"]),
                        "registration":          clean_text(row["registration"]),
                        "cn_ln":                 clean_text(row["cn_ln"]),
                        "aboard_total":          aboard_total,
                        "aboard_passengers":     aboard_pax,
                        "aboard_crew":           aboard_crew,
                        "fatalities_aboard":     fat_aboard,
                        "fatalities_passengers": fat_pax,
                        "fatalities_crew":       fat_crew,
                        "ground":                parse_ground(row["ground"]),
                        "fatalities_total":      fat_total,
                        "summary":               clean_text(row["summary"]),
                    })
                except Exception as exc:
                    skipped += 1
                    print(f"  [WARN] Skipped row (date={row['date']!r}): {exc}")

            dst_cur.executemany(self.get_insert_row_query(), cleaned_rows)
            dst_conn.commit()
            dst_conn.close()

        print(f"\nCleaning complete")
        print(f"  Source rows  : {len(rows)}")
        print(f"  Written rows : {len(cleaned_rows)}")
        print(f"  Skipped rows : {skipped}")
        print(f"  Output       : {self.dst_db.db_path.resolve()}")

        # Spot-check null counts
        check = self.dst_db.get_connection()
        cur = check.cursor()
        for col in ("date", "time", "aboard_total", "fatalities_aboard", "ground"):
            cur.execute(f"SELECT COUNT(*) FROM {self.dst_table} WHERE {col} IS NULL")
            n_null = cur.fetchone()[0]
            print(f"  NULL in {col:<22}: {n_null}")
        check.close()
        self.create_metadata_csv()

    def create_metadata_csv(self) -> None:
        # Build metadata.csv
        conn = self.dst_db.get_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {self.dst_table}")

        total_rows = cur.fetchone()[0]

        cur.execute(f"PRAGMA table_info({self.dst_table})")
        columns = [(row[1], row[2]) for row in cur.fetchall()]  # (name, type)

        metadata_rows = []
        for col_name, col_type in columns:
            cur.execute(f"SELECT COUNT(*) FROM {self.dst_table} WHERE {col_name} IS NULL")
            n_null = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {self.dst_table}")
            n_unique = cur.fetchone()[0]
            metadata_rows.append({
                "field": col_name,
                "data_type": col_type,
                "total_rows": total_rows,
                "num_na": n_null,
                "num_unique": n_unique,
                "description": self.get_field_description().get(col_name, ""),
            })

        conn.close()

        metadata_path = self.dst_db.db_path.parent / "metadata.csv"
        with open(metadata_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["field", "data_type", "total_rows", "num_na", "num_unique", "description"])
            writer.writeheader()
            writer.writerows(metadata_rows)
        print(f"  Metadata     : {metadata_path.resolve()}")


if __name__ == "__main__":
    pass
