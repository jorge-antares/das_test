# Plane Crashes Data Pipeline

A pipeline for cleaning, validating, and analysing the historical plane crashes dataset stored in an SQLite database.

## Project Structure

```
.
├── main.py                  # Entry point: runs cleaning then analysis
├── rawdata/
│   ├── plane_crashes_data.db              # Source SQLite database
│   └── plane_crashes_field_descriptions.csv
├── utilspc/
│   ├── cleanfun.py          # Low-level cleaning functions
│   └── cleanerclass.py      # BaseSQLite, DataSQLiteCleaner classes
├── analytics/
│   ├── analysis.py          # Data profiling and analysis
│   └── validation.py        # Schema and data integrity checks
└── output/
    └── metadata.csv         # Generated column-level metadata
```

## Pipeline Overview

### 1. Cleaning (`utilspc/`)

`DataSQLiteCleaner` reads the raw database, applies the following transformations, and writes to a new SQLite file:

| Column | Rule |
|---|---|
| `date` | `DD-Mon-YY` → ISO `YYYY-MM-DD`; years > 2018 shifted back 100 years |
| `time` | Strips leading `c`/`c ` prefix; normalises bare `HHMM` → `HH:MM`; `?` → NULL |
| `aboard` | `N (passengers:X crew:Y)` split into `aboard_total`, `aboard_passengers`, `aboard_crew` |
| `fatalities` | Same pattern → `fatalities_aboard`, `fatalities_passengers`, `fatalities_crew` |
| `ground` | `?` → NULL; otherwise INTEGER |
| Text fields | Strip whitespace; `?` → NULL |

A `metadata.csv` is generated alongside the cleaned database with column-level stats (type, NULL count, unique count).

### 2. Validation (`analytics/validation.py`)

Checks performed:
- Schema: expected columns and declared types
- Python-level type consistency
- Date format (`YYYY-MM-DD`) and range (1908–2018)
- Time format (`HH:MM`)
- Non-negative numeric values
- Cross-total consistency (e.g. `aboard_passengers + aboard_crew = aboard_total`)
- Potential duplicate rows by `(date, operator, route)`

### 3. Analysis (`analytics/analysis.py`)

Produces a structured text report covering:
- NULL distribution per column
- Fatality and survival rates, ground casualties, crew vs passenger split
- Crashes and fatalities per year and decade (ASCII bar chart included)
- Top operators and most dangerous aircraft types by fatality rate
- Geographic breakdown by country/region
- Data quality checks (mismatched totals, duplicate rows, registration reuse)

## Usage

```bash
python main.py
```

This runs the full pipeline: cleans `rawdata/plane_crashes_data.db` into `output2/cleaned_plane_crashes.db` and writes the analysis report to `data_profile.txt`.

To run analysis only:

```python
from analytics.analysis import main_analysis

main_analysis("output2/cleaned_plane_crashes.db", output_file="report.txt")
```

To run validation only:

```python
from analytics.validation import run_validation
from pathlib import Path

report = run_validation(Path("output2/cleaned_plane_crashes.db"))
report.print_summary()
```

## Requirements

- Python 3.10+
- Standard libraries only (No external dependencies)
