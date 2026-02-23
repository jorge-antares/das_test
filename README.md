# Plane Crashes Data Pipeline

A Python project for cleaning, validating, and analysing the historical plane crashes dataset.

## Project Structure

```
.
├── analytics
│   ├── analysis.py
│   └── validation.py
├── cleaning.txt
├── main.py
├── presentation
│   ├── buildtex.sh
│   ├── img
│   │   └── structure.png
│   ├── presentation.pdf
│   └── presentation.tex
├── rawdata
│   ├── plane_crashes_data.db
│   └── plane_crashes_field_descriptions.csv
├── README.md
├── test.py
└── utilspc
    ├── cleanerclass.py
    └── cleanfun.py

6 directories, 14 files
```

## Usage

```bash
python main.py
```
This will create a directory named `output` where the following files:
- `cleaned_plane_crashes_data.db`: The cleaned dataset in SQLite format.
- `metadata.csv`: Folowing the provided template, this file contains metadata about the cleaned dataset.
- `validation_report.txt`: A report detailing the results of the data validation process.
- `data_profile.txt`: A comprehensive profile of the cleaned dataset.

## Requirements

- Python 3.10+
- No external dependencies needed
