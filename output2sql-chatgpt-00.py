# output2sql-chatgpt-00.py
#
import os
import glob
import logging
import pandas as pd
import pyodbc
from datetime import datetime
from sqlalchemy.sql.sqltypes import Integer, String, Float, Boolean, DateTime

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    filename=f"{timestamp}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# SQL Server connection
connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=MORNINGSTAR\\DCW_PROTO;'
    'DATABASE=DCW_PLANT;'
    'UID=dcwpydev;'
    'PWD=AL7DD308AZVRMY2Y76KG'
)

def list_data_files():
    files = sorted(glob.glob("*.csv") + glob.glob("*.json"))
    for idx, file in enumerate(files, 1):
        print(f"{idx}. {file}")
    return files

def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    else:
        return String

def generate_create_table_sql(df, table_name):
    lines = [f"CREATE TABLE {table_name} ("]
    for col in df.columns:
        sql_type = infer_sql_type(df[col].dtype).__name__.upper()
        if sql_type == "STRING":
            sql_type = "VARCHAR(255)"
        lines.append(f"    [{col}] {sql_type},")
    lines[-1] = lines[-1].rstrip(',')  # Remove trailing comma
    lines.append(");")
    return "\n".join(lines)

def upload_to_sql(df, table_name):
    cursor = connection.cursor()
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(f'[{col}]' for col in df.columns)}) VALUES ({placeholders})"
    count = 0
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_sql, tuple(row))
            if (count + 1) % 100 == 0:
                connection.commit()
            count += 1
        except Exception as e:
            logging.error(f"Error on row {index}: {e}")
    connection.commit()
    return count

def main():
    print("Scanning for .csv and .json files...\n")
    files = list_data_files()
    if not files:
        print("No .csv or .json files found.")
        return

    choice = int(input("\nEnter the number of the file to process: ")) - 1
    selected_file = files[choice]
    table_name = os.path.splitext(os.path.basename(selected_file))[0]

    if selected_file.endswith(".csv"):
        df = pd.read_csv(selected_file)
    else:
        df = pd.read_json(selected_file, lines=True)

    print(f"\nDetected structure:\n{df.dtypes}\n")
    input("Press Enter to continue...")

    create_sql = generate_create_table_sql(df, table_name)
    print("\nGenerated SQL:")
    print(create_sql)

    with open(f"{table_name}.sql", "w") as f:
        f.write(create_sql)

    proceed = input("\nHas the table been created on the SQL Server? (y/n): ").lower()
    if proceed != 'y':
        return

    print(f"\n{len(df)} records found.")
    continue_upload = input("Do you want to proceed with uploading the data? (y/n): ").lower()
    if continue_upload != 'y':
        return

    uploaded_count = upload_to_sql(df, table_name)
    print(f"\nUpload complete: {uploaded_count} records uploaded.")
    logging.info(f"{uploaded_count} records uploaded to {table_name}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Unhandled exception occurred")
        print(f"An error occurred: {e}")
