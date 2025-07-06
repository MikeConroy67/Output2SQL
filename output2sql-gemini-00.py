# output2sql-gemini-00.py
#
import os
import csv
import json
import pandas as pd
import pyodbc
import logging
from datetime import datetime
import time

# --- Configuration ---
LOG_FILE_NAME = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
SQL_CONNECTION_STRING = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=MORNINGSTAR\\DCW_PROTO;'
    'DATABASE=DCW_PLANT;'
    'UID=dcwpydev;'
    'PWD=AL7DD308AZVRMY2Y76KG'
)

# --- Logging Setup ---
def setup_logging():
    """Sets up logging to a file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE_NAME),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging started. Log file: {LOG_FILE_NAME}")

# --- File Operations ---
def list_data_files():
    """
    Reads the current working directory and provides a numbered list
    (alphabetical by filename) of all .csv and .json files.
    """
    logging.info("Scanning current directory for .csv and .json files...")
    files = [f for f in os.listdir('.') if f.endswith(('.csv', '.json'))]
    files.sort()

    if not files:
        logging.warning("No .csv or .json files found in the current directory.")
        return []

    print("\nAvailable .csv and .json files:")
    for i, file in enumerate(files):
        print(f"{i + 1}. {file}")
    logging.info(f"Found {len(files)} data files.")
    return files

def select_file(files):
    """Asks the user to select a file from the list."""
    while True:
        try:
            choice = int(input("Enter the number of the file you want to process: "))
            if 1 <= choice <= len(files):
                selected_file = files[choice - 1]
                logging.info(f"User selected file: {selected_file}")
                return selected_file
            else:
                print("Invalid choice. Please enter a number within the range.")
        except ValueError:
            print("Invalid input. Please enter a number.")

# --- Schema Inference ---
def infer_schema(file_path):
    """
    Reads the file and determines the record format (column names and inferred types).
    Returns a dictionary mapping column names to inferred Python types.
    """
    logging.info(f"Inferring schema for file: {file_path}")
    schema = {}
    file_extension = os.path.splitext(file_path)[1].lower()

    try:
        if file_extension == '.csv':
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                # Use csv.Sniffer to detect delimiter
                try:
                    dialect = csv.Sniffer().sniff(f.read(1024))
                    f.seek(0) # Go back to the beginning of the file
                    reader = csv.reader(f, dialect)
                except csv.Error:
                    # Fallback if sniffing fails (e.g., single column or unusual format)
                    f.seek(0)
                    reader = csv.reader(f)

                header = next(reader)
                sample_data = []
                for _ in range(min(10, sum(1 for row in f))): # Read up to 10 sample rows
                    sample_data.append(next(reader))
                f.seek(0) # Reset file pointer for later full read

                # Initialize schema with string type for all columns
                for col_name in header:
                    schema[col_name] = str

                # Infer types based on sample data
                for row in sample_data:
                    for i, value in enumerate(row):
                        col_name = header[i]
                        current_type = schema[col_name]

                        # Try to cast to more specific types
                        if current_type is str: # Only try to upgrade if current is string
                            try:
                                int(value)
                                schema[col_name] = int
                            except ValueError:
                                try:
                                    float(value)
                                    schema[col_name] = float
                                except ValueError:
                                    # Check for boolean-like strings
                                    if value.lower() in ['true', 'false', 'y', 'n', 'yes', 'no']:
                                        schema[col_name] = bool
                                    else:
                                        schema[col_name] = str # Remains string if nothing else fits
                                # If it's already a float, don't downgrade to int
                                if current_type is float and type(value) is int:
                                    schema[col_name] = float

        elif file_extension == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list) and data:
                    # Assume first object in list represents the schema
                    sample_record = data[0]
                    for key, value in sample_record.items():
                        schema[key] = type(value)
                elif isinstance(data, dict):
                    # If it's a single JSON object
                    for key, value in data.items():
                        schema[key] = type(value)
                else:
                    raise ValueError("Unsupported JSON structure. Expected a list of objects or a single object.")
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        logging.info(f"Inferred schema for {file_path}: {schema}")
        return schema

    except Exception as e:
        logging.error(f"Error inferring schema for {file_path}: {e}", exc_info=True)
        print(f"Error inferring schema: {e}")
        return None

def display_schema(schema):
    """Displays the inferred record format to the user."""
    if not schema:
        return

    print("\n--- Inferred Record Format ---")
    for column, py_type in schema.items():
        print(f"  {column}: {py_type.__name__}")
    print("----------------------------")
    logging.info("Displayed inferred schema to user.")

# --- SQL Snippet Generation ---
def get_sql_data_type(py_type):
    """Maps Python types to SQL Server data types."""
    if py_type is int:
        return "INT"
    elif py_type is float:
        return "FLOAT"
    elif py_type is bool:
        return "BIT"
    elif py_type is str:
        return "NVARCHAR(255)" # Default string length, can be adjusted
    elif py_type is datetime:
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)" # Fallback for unknown types or complex objects

def generate_create_table_sql(file_path, schema):
    """
    Creates a SQL snippet to create a table using the filename for the table name.
    """
    if not schema:
        return None, None

    table_name = os.path.splitext(os.path.basename(file_path))[0].replace('.', '_').replace('-', '_')
    logging.info(f"Generating CREATE TABLE SQL for table: {table_name}")

    columns_sql = []
    for column_name, py_type in schema.items():
        sql_type = get_sql_data_type(py_type)
        # Sanitize column name for SQL (e.g., replace spaces, special chars)
        sanitized_column_name = f"[{column_name.replace(' ', '_').replace('.', '_')}]"
        columns_sql.append(f"{sanitized_column_name} {sql_type}")

    create_table_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns_sql) + "\n);"
    sql_file_name = f"{table_name}.sql"

    try:
        with open(sql_file_name, 'w', encoding='utf-8') as f:
            f.write(create_table_sql)
        logging.info(f"SQL CREATE TABLE snippet written to {sql_file_name}")
        print(f"\nSQL CREATE TABLE snippet generated and saved to {sql_file_name}")
        print("\n--- Generated SQL CREATE TABLE Snippet ---")
        print(create_table_sql)
        print("------------------------------------------")
        return create_table_sql, table_name
    except Exception as e:
        logging.error(f"Error writing SQL snippet to file: {e}", exc_info=True)
        print(f"Error writing SQL snippet: {e}")
        return None, None

# --- Data Loading and Upload ---
def read_data_to_dataframe(file_path, schema):
    """
    Reads the data from the file and creates a compatible pandas DataFrame.
    """
    logging.info(f"Reading data from {file_path} into DataFrame...")
    file_extension = os.path.splitext(file_path)[1].lower()
    df = None

    try:
        if file_extension == '.csv':
            # Read CSV, ensuring all columns are read as strings initially
            # Then convert to inferred types using .astype()
            df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
            for col, py_type in schema.items():
                if col in df.columns:
                    if py_type is int:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64') # Use Int64 for nullable integers
                    elif py_type is float:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif py_type is bool:
                        df[col] = df[col].astype(str).str.lower().map({'true': True, 'y': True, 'yes': True, 'false': False, 'n': False, 'no': False}).astype('boolean') # Use boolean for nullable booleans
                    elif py_type is datetime:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                else:
                    logging.warning(f"Column '{col}' from schema not found in CSV file '{file_path}'.")

        elif file_extension == '.json':
            df = pd.read_json(file_path, encoding='utf-8')
            # Ensure types match inferred schema if possible, though pandas often does well with JSON
            for col, py_type in schema.items():
                if col in df.columns:
                    if py_type is int:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    elif py_type is float:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif py_type is bool:
                        df[col] = df[col].astype(str).str.lower().map({'true': True, 'y': True, 'yes': True, 'false': False, 'n': False, 'no': False}).astype('boolean')
                    elif py_type is datetime:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                else:
                    logging.warning(f"Column '{col}' from schema not found in JSON file '{file_path}'.")
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        logging.info(f"Successfully read {len(df)} records into DataFrame from {file_path}.")
        return df

    except Exception as e:
        logging.error(f"Error reading data into DataFrame from {file_path}: {e}", exc_info=True)
        print(f"Error reading data into DataFrame: {e}")
        return None

def upload_dataframe_to_sql(df, table_name):
    """
    Uploads the DataFrame data to the specified SQL Server table,
    committing records every 100th record.
    """
    if df is None or df.empty:
        logging.warning("No data in DataFrame to upload.")
        print("No data to upload.")
        return 0

    logging.info(f"Attempting to connect to SQL Server and upload {len(df)} records to table '{table_name}'.")
    cnxn = None
    cursor = None
    uploaded_count = 0

    try:
        cnxn = pyodbc.connect(SQL_CONNECTION_STRING)
        cursor = cnxn.cursor()

        # Prepare the INSERT statement
        columns = ', '.join([f'[{col.replace(" ", "_").replace(".", "_")}]' for col in df.columns])
        placeholders = ', '.join(['?' for _ in df.columns])
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        logging.info(f"Prepared INSERT statement: {insert_sql}")

        for index, row in df.iterrows():
            try:
                # Convert pandas nullable types to Python native types for pyodbc
                values = [None if pd.isna(x) else x for x in row.values]
                cursor.execute(insert_sql, *values)
                uploaded_count += 1

                if uploaded_count % 100 == 0:
                    cnxn.commit()
                    logging.info(f"Committed {uploaded_count} records to SQL Server.")
                    print(f"Uploaded {uploaded_count} records...")

            except pyodbc.Error as db_err:
                logging.error(f"Error inserting row {index} into SQL: {db_err}. Row data: {row.to_dict()}", exc_info=True)
                print(f"Error inserting row {index}: {db_err}")
                # Decide whether to continue or break on error
                # For now, we'll log and continue, but a more robust solution might skip or retry
            except Exception as e:
                logging.error(f"Unexpected error processing row {index}: {e}. Row data: {row.to_dict()}", exc_info=True)
                print(f"Unexpected error processing row {index}: {e}")

        cnxn.commit() # Commit any remaining records
        logging.info(f"Successfully uploaded total of {uploaded_count} records to table '{table_name}'.")
        print(f"\nSuccessfully uploaded a total of {uploaded_count} records to table '{table_name}'.")
        return uploaded_count

    except pyodbc.Error as e:
        logging.error(f"SQL Server connection or operation error: {e}", exc_info=True)
        print(f"SQL Server connection or operation error: {e}")
        return 0
    except Exception as e:
        logging.error(f"An unexpected error occurred during upload: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")
        return 0
    finally:
        if cursor:
            cursor.close()
        if cnxn:
            cnxn.close()
        logging.info("SQL Server connection closed.")

# --- Main Program Flow ---
def main():
    setup_logging()
    logging.info("Starting output2sql.py program.")

    # 1) Read the current working directory and provide a numbered list
    files = list_data_files()
    if not files:
        print("Exiting. No data files found.")
        logging.info("No data files found. Exiting.")
        return

    selected_file = select_file(files)
    if not selected_file:
        print("No file selected. Exiting.")
        logging.info("No file selected. Exiting.")
        return

    # 2) Read the file and determine the record format and display it
    inferred_schema = infer_schema(selected_file)
    if not inferred_schema:
        print("Could not determine file schema. Exiting.")
        logging.error("Could not determine file schema. Exiting.")
        return

    display_schema(inferred_schema)

    # 3) Ask the user to press any key to continue
    input("\nPress Enter to continue with SQL snippet generation...")
    logging.info("User pressed Enter to continue.")

    # 4) Create a SQL snippet to create a table
    create_sql, table_name = generate_create_table_sql(selected_file, inferred_schema)
    if not create_sql:
        print("Could not generate SQL CREATE TABLE snippet. Exiting.")
        logging.error("Could not generate SQL CREATE TABLE snippet. Exiting.")
        return

    # 5) Ask the user if the table has been created on the sql server
    while True:
        table_created_response = input("\nHas the table been created on the SQL server? (y/n): ").lower()
        if table_created_response in ['y', 'n']:
            break
        else:
            print("Invalid response. Please enter 'y' or 'n'.")

    if table_created_response == 'n':
        print("Please create the table on the SQL server first using the generated SQL snippet.")
        print("Exiting without data upload.")
        logging.info("User indicated table not created. Exiting without data upload.")
        return

    # If 'y', read the data and create a compatible dataframe
    df = read_data_to_dataframe(selected_file, inferred_schema)
    if df is None:
        print("Failed to read data into DataFrame. Exiting.")
        logging.error("Failed to read data into DataFrame. Exiting.")
        return

    # 6) Advise the user of the number of records read into the dataframe
    print(f"\n{len(df)} records read into the DataFrame.")
    logging.info(f"{len(df)} records read into DataFrame.")

    while True:
        proceed_upload = input("Do you want to proceed with uploading this data to the SQL table? (y/n): ").lower()
        if proceed_upload in ['y', 'n']:
            break
        else:
            print("Invalid response. Please enter 'y' or 'n'.")

    # 7) If the user responds 'y', upload the data
    if proceed_upload == 'y':
        uploaded_count = upload_dataframe_to_sql(df, table_name)
        logging.info(f"Data upload process completed. {uploaded_count} records uploaded.")
        print(f"\nProgram finished. Total records uploaded: {uploaded_count}")
    else:
        print("User chose not to upload data. Exiting.")
        logging.info("User chose not to upload data. Exiting.")

    logging.info("output2sql.py program finished.")

if __name__ == "__main__":
    main()