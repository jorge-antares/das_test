import sys
from utilspc.cleanerclass import BaseSQLite
from utilspc.cleanfun import get_unique_values



if __name__ == "__main__":
    db_path = "rawdata/plane_crashes_data.db"
    table_name = "plane_crashes_data"
    field = sys.argv[1]
    if not field:
        print("Please provide a field name as an argument.")
        sys.exit(1)
    db = BaseSQLite(db_path)
    conn = db.get_connection()
    cur = conn.cursor()
    unique_values = get_unique_values(cur, table_name, field)
    print(f"Unique values for field '{field}':")
    for value in unique_values:
        print(value)
    # Save values as CSV
    with open(f"unique/unique_{field}.csv", "w") as f:
        for value in unique_values:
            f.write(f"{value}\n")
    print(f"Unique values for field '{field}' saved to 'unique/unique_{field}.csv'.")