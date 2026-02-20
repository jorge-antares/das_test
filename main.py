from utilspc.cleanerclass import DataSQLiteCleaner
from analytics.analysis import main_analysis


# Create cleaned DB
db = DataSQLiteCleaner(
        src_db_path="rawdata/plane_crashes_data.db",
        src_table="plane_crashes_data",
        dst_db_path="output2/cleaned_plane_crashes.db",
        dst_table="data",
    )
db.clean_and_insert()


# Run analysis
main_analysis("output2/cleaned_plane_crashes.db")
