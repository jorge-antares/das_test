"""
Simple validity test
"""

import sys
from utilspc.cleanfun import check_no_duplicates
from utilspc.cleanerclass import BaseSQLite


def test_no_duplicates() -> bool:
    db = BaseSQLite("output/cleaned_plane_crashes.db")
    with db.get_connection() as conn:
        return check_no_duplicates(conn, "data")


if __name__ == "__main__":
    if not test_no_duplicates():
        print("FAIL: duplicates found in 'data'.")
        sys.exit(1)
    print("PASS: no duplicates found in 'data'.")