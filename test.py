"""
Simple validity tests
"""

from utilspc.cleanfun import check_no_duplicates
from utilspc.cleanerclass import BaseSQLite


db = BaseSQLite("rawdata/plane_crashes_data.db")
with db.get_connection() as conn:
    if check_no_duplicates(conn, "plane_crashes_data"):
        print(f"No duplicates found in '{db.db_path}' table.")
    else:
        print(f"Duplicates exist in '{db.db_path}' table.")