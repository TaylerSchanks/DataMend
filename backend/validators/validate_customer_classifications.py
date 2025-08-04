import pandas as pd
import pyodbc
import tempfile
import os
from openpyxl import Workbook


def get_table_metadata(server, database, username, password, table_name):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    query = f"""
        SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
    """
    cursor.execute(query)
    columns = cursor.fetchall()
    conn.close()

    skip_fields = {"CustClassKey", "MasterClassNamesKey"}

    return [{
        'name': col[0].lower(),
        'required': col[1] == 'NO' and col[0] not in skip_fields,
        'type': col[2]
    } for col in columns if col[0] not in skip_fields]



def validate_dataframe(df, metadata):
    valid_rows = []
    error_rows = []
    warning_rows = []

    for idx, row in df.iterrows():
        row_errors = []
        row_warnings = []
        for col_meta in metadata:
            col_name = col_meta['name'].lower()
            is_required = col_meta['required']
            expected_type = col_meta['type']
            value = row.get(col_name)

            if is_required and (pd.isnull(value) or str(value).strip() == ''):
                row_errors.append(f"Missing required field: {col_name}")
            elif not pd.isnull(value):
                try:
                    if expected_type in ['int', 'bigint', 'smallint']:
                        int(value)
                    elif expected_type in ['float', 'decimal', 'numeric', 'money']:
                        float(value)
                    elif expected_type == 'bit':
                        if str(value).lower() not in ['0', '1', 'true', 'false']:
                            raise ValueError()
                    elif expected_type in ['date', 'datetime', 'smalldatetime']:
                        pd.to_datetime(value)
                except Exception:
                    if is_required:
                        row_errors.append(f"Invalid {expected_type} in required field: {col_name}")
                    else:
                        row_warnings.append(f"Invalid {expected_type} in optional field: {col_name}")

        if row_errors:
            error_rows.append(row.to_dict() | {"ValidationErrors": "; ".join(row_errors)})
        elif row_warnings:
            warning_rows.append(row.to_dict() | {"ValidationWarnings": "; ".join(row_warnings)})
        else:
            valid_rows.append(row)

    return pd.DataFrame(valid_rows), pd.DataFrame(error_rows), pd.DataFrame(warning_rows)


def run_validation(conn, uploaded_file):
    # Save uploaded file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        uploaded_file.save(tmp.name)
        file_path = tmp.name

    # Load file
    if uploaded_file.filename.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    # Normalize column names (trim only, no lowercase)
    df.columns = [col.strip().lower() for col in df.columns]

    # Ensure required columns are present (case-sensitive)
    required_columns = {"customerid", "classificationname"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
      raise ValueError(f"Missing required column(s): {', '.join(missing_columns)}")

    # Rename for internal use (keep lowercase)
    df.rename(columns={
   "customerid": "growid",
   "classificationname": "classificationname"
}, inplace=True)

    print("Renamed DF Columns:", df.columns.tolist())


    # Get DB connection info
    server = conn.getinfo(pyodbc.SQL_SERVER_NAME)
    database = conn.getinfo(pyodbc.SQL_DATABASE_NAME)
    username = "Agvance"
    password = "AgvSQL2000"

    # Pull table schema
    metadata = get_table_metadata(server, database, username, password, 'CustClass')

    # Override required flags for fields we’re skipping
    for col in metadata:
        if col['name'] in {"CustClassKey", "MasterClassNamesKey"}:
            col['required'] = False

    print("Incoming DataFrame Columns:", df.columns.tolist())
    print("Metadata Columns:", [m['name'] for m in metadata])


    # Schema validation
    valid_df, error_df, warning_df = validate_dataframe(df, metadata)
    print("Validated rows:", len(valid_df))
    print("Errors found:", len(error_df))
    print("Warnings found:", len(warning_df))


    # Pull valid GrowIDs from grower table
    cursor = conn.cursor()
    cursor.execute("SELECT growid FROM grower")
    valid_growids = {str(row[0]).strip() for row in cursor.fetchall()}

    print("Valid DF Columns:", valid_df.columns.tolist())

    # Validate ClassificationName using JOIN logic
    validated_classnames = set()
    for name in valid_df["classificationname"].dropna().unique():
        cursor.execute("""
            SELECT TOP 1 1
            FROM dbo.MasterClassNames M
            INNER JOIN dbo.CustClass C ON C.MasterClassNamesKey = M.MasterClassNamesKey
            WHERE M.ClassName = ?
        """, name)
        if cursor.fetchone():
            validated_classnames.add(name.strip().lower())

        


    # Final validation loop
    final_valid_rows = []
    for _, row in valid_df.iterrows():
        growid = str(row.get("growid", "")).strip()
        classname = str(row.get("classificationname", "")).strip().lower()
        row_dict = row.to_dict()
        row_issues = []
        row_warnings = []

        if growid not in valid_growids:
            row_issues.append(f"Invalid GrowID: {growid}")

        if classname not in validated_classnames:
            row_warnings.append(f"[{row.get('classificationname')}] needs added to Agvance")

        if row_issues:
            row_dict["ValidationErrors"] = "; ".join(row_issues)
            error_df = pd.concat([error_df, pd.DataFrame([row_dict])], ignore_index=True)
        elif row_warnings:
            row_dict["ValidationWarnings"] = "; ".join(row_warnings)
            warning_df = pd.concat([warning_df, pd.DataFrame([row_dict])], ignore_index=True)
        else:
            final_valid_rows.append(row)

    # Final valid DataFrame
    final_valid_df = pd.DataFrame(final_valid_rows)

    # Write results to Excel file
    output_path = os.path.join(tempfile.gettempdir(), "validation_result.xlsx")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        if not final_valid_df.empty:
            final_valid_df.to_excel(writer, index=False, sheet_name="Valid")
        if not error_df.empty:
            error_df.to_excel(writer, index=False, sheet_name="Errors")
        if not warning_df.empty:
            warning_df.to_excel(writer, index=False, sheet_name="Warnings")

    return {
        "message": "✅ Validation completed with cross-checks.",
        "download_link": f"/download?path={output_path}"
    }
