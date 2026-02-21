"""
Simple validity test
"""

import sys
from utilspc.cleanfun import check_no_duplicates
from utilspc.cleanerclass import BaseSQLite


def test_no_duplicates() -> bool:
    db = BaseSQLite("rawdata/plane_crashes_data.db")
    with db.get_connection() as conn:
        return check_no_duplicates(conn, "plane_crashes_data")


if __name__ == "__main__":
    if not test_no_duplicates():
        print("FAIL: duplicates found in 'plane_crashes_data'.")
        sys.exit(1)
    print("PASS: no duplicates found in 'plane_crashes_data'.")