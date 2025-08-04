import pandas as pd

def run_validation(conn, file):
    # Parse the uploaded Excel file
    df = pd.read_excel(file)

    # Example: Check for required columns
    if 'CustomerID' not in df.columns:
        return "Missing required column: 'CustomerID'."

    # Example: Use the DB connection
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM grower")
    grower_count = cursor.fetchone()[0]

    return f"✅ File contains {len(df)} rows. Grower table has {grower_count} rows."
