import pandas as pd
import os
import time
from sqlalchemy import create_engine, text # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore
from dotenv import load_dotenv
from datetime import datetime
import sys

# Ensure parent directory is in path for engines import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engines.spline_engine import push_spline_lineage

# Load environment variables
load_dotenv()

# Configuration
DB_NAME = "governance_db"
DB_USER = os.getenv("MARIADB_USER", "root")
DB_PASS = os.getenv("MARIADB_PASS", "") 
DB_HOST = os.getenv("MARIADB_HOST", "127.0.0.1")
DB_PORT = os.getenv("MARIADB_PORT", "3307")

# Path to data folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Connection string for MariaDB
BASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}"
DB_URL = f"{BASE_URL}/{DB_NAME}"

def create_database_if_not_exists():
    """Attempt to create the database if it doesn't exist."""
    if not DB_PASS and DB_USER == "root":
        print("INFO: No MariaDB password detected for 'root' user.")
    
    print(f"STATUS: Checking for database '{DB_NAME}' at {DB_HOST}:{DB_PORT}...")
    engine = create_engine(BASE_URL)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
            print(f"SUCCESS: Database '{DB_NAME}' is ready.")
    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        exit(1)

def clean_email(email):
    if pd.isna(email) or email == "(NULL)":
        return None
    email = str(email).replace("#", "@")
    if "@" not in email:
        if "gmail.com" in email:
            email = email.replace("gmail.com", "@gmail.com")
        elif "yahoo.com" in email:
            email = email.replace("yahoo.com", "@yahoo.com")
    return email

def process_table(filename, table_name, id_col):
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        print(f"ERROR: {file_path} not found.")
        return None

    print(f"STATUS: Processing {table_name} from {file_path}...")
    start_time = time.time()
    df = pd.read_csv(file_path)
    
    # Transform
    df = df.drop_duplicates(subset=[id_col], keep='first')
    if 'email' in df.columns:
        df['email'] = df['email'].apply(clean_email)
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').abs()
    
    date_cols = [c for c in df.columns if 'date' in c]
    for col in date_cols:
        df[col] = df[col].replace("(NULL)", pd.NA)
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Load
    engine = create_engine(DB_URL)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    
    duration = time.time() - start_time
    print(f"SUCCESS: {table_name} loaded ({len(df)} rows).")
    
    # Push Lineage to Spline
    push_spline_lineage(
        integration_name="Local MariaDB",
        tables_data={table_name: df.columns.tolist()},
        output_table=table_name,
        duration_seconds=duration
    )
    return df

def run_etl():
    create_database_if_not_exists()
    
    # Process Orders
    process_table("raw_orders.csv", "orders", "order_id")
    
    # Process Transactions
    process_table("raw_transactions.csv", "transactions", "transaction_id")

    print("STATUS: ETL Pipeline completed successfully.")

if __name__ == "__main__":
    run_etl()
