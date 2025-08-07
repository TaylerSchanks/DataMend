import pandas as pd
import pyodbc
import tempfile
import os
import re
from datetime import datetime

def get_table_metadata(server, database, username, password, table_name):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
    """)
    columns = cursor.fetchall()
    conn.close()

    skip_fields = {"SalesmanKey", "UniqueID", "LastModifiedUTC", "SalesmenGUID"}
    return [{
        'name': col[0].lower(),
        'required': col[1] == 'NO' and col[0] not in skip_fields,
        'type': col[2]
    } for col in columns if col[0] not in skip_fields]

def run_validation(conn, uploaded_file):
    print("✅ Loaded validation module")
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        uploaded_file.save(tmp.name)
        file_path = tmp.name

    df = pd.read_csv(file_path) if uploaded_file.filename.endswith('.csv') else pd.read_excel(file_path)
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        "salesperson id": "id",
        "first name": "firstname",
        "last name": "lastname"
    }, inplace=True)

    # Get metadata for table structure
    server = conn.getinfo(pyodbc.SQL_SERVER_NAME)
    database = conn.getinfo(pyodbc.SQL_DATABASE_NAME)
    metadata = get_table_metadata(server, database, "Agvance", "AgvSQL2000", "salesmen")

    # Ensure required columns exist
    required_columns = {col['name'] for col in metadata if col['required']}
    missing_required_columns = required_columns - set(df.columns)
    if missing_required_columns:
        return {
            "message": (
                "❌ Upload failed: Your file is missing one or more required column headers.\n\n"
                f"Missing column(s): {', '.join(sorted(missing_required_columns))}\n"
                "Please update your file and try again."
            ),
            "file_type": "none",
            "download_link": None
        }

    # Fetch existing salesman IDs from DB
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM salesmen")
    existing_ids = {str(row[0]).strip().lower() for row in cursor.fetchall() if row[0] is not None}

    # Prep validation containers
    original_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = original_count - len(df)

    valid_rows, warning_rows, error_rows = [], [], []
    seen_ids = set()
    sql_statements = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        row_errors, row_warnings = [], []

        for col_meta in metadata:
            col_name = col_meta['name']
            is_required = col_meta['required']
            expected_type = col_meta['type']
            value = row_dict.get(col_name)

            if is_required and (pd.isnull(value) or str(value).strip() == ''):
                row_errors.append(f"Missing required field: {col_name}")
            elif not pd.isnull(value):
                try:
                    if expected_type in ['int', 'bigint', 'smallint']:
                        int(value)
                    elif expected_type in ['float', 'decimal', 'numeric', 'money']:
                        float(value)
                    elif expected_type == 'bit':
                        if isinstance(value, (int, float)):
                            if int(value) not in [0, 1]:
                                raise ValueError()
                        else:
                            if str(value).strip().lower() not in ['0', '1', 'true', 'false']:
                                raise ValueError()
                    elif expected_type in ['date', 'datetime', 'smalldatetime']:
                        pd.to_datetime(value)
                except Exception:
                    if is_required:
                        row_errors.append(f"Invalid {expected_type} in required field: {col_name}")
                    else:
                        row_warnings.append(f"Invalid {expected_type} in optional field: {col_name}")

        # Salesman ID checks
        salesperson_id = str(row_dict.get("id", "")).strip().lower()
        if not re.fullmatch(r"[A-Za-z0-9]+", salesperson_id):
            row_errors.append("Salesman ID contains invalid characters. Only letters and numbers allowed.")
        if salesperson_id in seen_ids:
            row_errors.append(f"Duplicate Salesman ID found in uploaded file: '{salesperson_id}'")
        else:
            seen_ids.add(salesperson_id)
        if salesperson_id in existing_ids:
            row_errors.append("Salesman ID already exists in Agvance")

        # Name field validation
        for name_field in ['firstname', 'lastname']:
            name_val = str(row_dict.get(name_field, "")).strip()
            if name_val and not re.fullmatch(r"[A-Za-z\s\-']+", name_val):
                row_errors.append(f"Invalid characters in {name_field}")

        if row_errors:
            row_dict["ValidationErrors"] = "; ".join(row_errors)
            error_rows.append(row_dict)
        elif row_warnings:
            row_dict["ValidationWarnings"] = "; ".join(row_warnings)
            warning_rows.append(row_dict)
        else:
            def escape_sql(val): return str(val).replace("'", "''")
            fields = [col for col in row.index if pd.notnull(row[col])]
            values = []
            for col in fields:
                val = row[col]
                dtype = next((m['type'] for m in metadata if m['name'] == col), '').lower()
                if pd.isnull(val):
                    values.append("NULL")
                elif dtype == 'bit':
                    values.append("1" if str(val).strip().lower() in ['true', '1'] else "0")
                else:
                    values.append(f"'{escape_sql(str(val).strip())}'")
            insert_sql = f"INSERT INTO Salesmen ({', '.join(fields)}) VALUES ({', '.join(values)})"
            sql_statements.append(insert_sql)
            valid_rows.append(row_dict)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Excel output if warnings or errors
    if error_rows or warning_rows:
        filename = f"validation_result_{timestamp}.xlsx"
        output_path = os.path.join(tempfile.gettempdir(), filename)
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            if valid_rows:
                pd.DataFrame(valid_rows).to_excel(writer, index=False, sheet_name="Valid")
            if error_rows:
                pd.DataFrame(error_rows).to_excel(writer, index=False, sheet_name="Errors")
            if warning_rows:
                pd.DataFrame(warning_rows).to_excel(writer, index=False, sheet_name="Warnings")
        return {
            "message": f"⚠️ Salesman validation complete with issues. {duplicates_removed} duplicate row(s) removed.",
            "file_type": "excel",
            "download_link": f"/download/tempfile?name={filename}"
        }
    
    print(f"✅ Passed validation — generating SQL insert file")
    print(f"🧾 {len(sql_statements)} SQL statements generated")


    # SQL file if clean
    if sql_statements:
        filename = f"salesman_inserts_{timestamp}.sql"
        sql_path = os.path.join(tempfile.gettempdir(), filename)
        with open(sql_path, "w") as f:
            f.write("-- SQL INSERT STATEMENTS GENERATED BY DataMend\n\n")
            for stmt in sql_statements:
                f.write(stmt + "\n")
        return {
            "message": f"✅ Salesman validation complete. {duplicates_removed} duplicate row(s) removed.",
            "file_type": "sql",
            "download_link": f"/download/tempfile?name={filename}"
        }

    return {
        "message": f"❗ No valid data or errors to report. {duplicates_removed} duplicate row(s) removed.",
        "file_type": "none",
        "download_link": None
    }
