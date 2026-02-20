from utilspc.cleanerclass import DataSQLiteCleaner
from analytics.analysis import main_analysis
from analytics.validation import run_validation


# Checks to original DB
#report_original = run_validation("rawdata/plane_crashes_data.db", "plane_crashes_data")
#report_original.print_summary()
"""
# Create cleaned DB
db = DataSQLiteCleaner(
        src_db_path="rawdata/plane_crashes_data.db",
        src_table="plane_crashes_data",
        dst_db_path="output/cleaned_plane_crashes.db",
        dst_table="data",
    )
db.clean_and_insert()
"""
# Validation checks
report = run_validation("output/cleaned_plane_crashes.db", "data")
report.print_summary()

# Run analysis
#main_analysis("output/cleaned_plane_crashes.db")
