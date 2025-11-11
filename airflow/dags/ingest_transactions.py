"""
Airflow DAG to ingest sales transaction data from Postgres source into DuckDB.
"""
from datetime import datetime
from pathlib import Path
import sys
import duckdb
from airflow.sdk import dag, task

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.warehouse import ensure_warehouse_exists, WAREHOUSE_PATH

PROJECT_ROOT = Path(__file__).parent.parent.parent
SOURCE_PATH = PROJECT_ROOT / "sources" / "postgres" / "transactions.csv"

@dag(
    dag_id="ingest_transactions",
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "postgres"],
)
def ingest_transactions():
    """DAG to ingest sales transactions from Postgres source into DuckDB."""
    
    @task()
    def extract_transactions():
        """Extract transactions from CSV source."""
        print(f"Extracting transactions from {SOURCE_PATH}")
        return str(SOURCE_PATH)
    
    @task()
    def load_to_duckdb(warehouse_path: str, source_path: str):
        """Load transactions into DuckDB raw layer."""
        print(f"Loading transactions into DuckDB at {warehouse_path}")
        
        conn = duckdb.connect(warehouse_path)
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.transactions AS
            SELECT * FROM read_csv_auto('{source_path}')
        """)
        
        result = conn.execute("SELECT COUNT(*) FROM raw.transactions").fetchone()
        print(f"Loaded {result[0]} transaction records into raw.transactions")
        
        conn.close()
        return result[0]
    
    # Define task dependencies
    warehouse_path = ensure_warehouse_exists()
    source_path = extract_transactions()
    load_to_duckdb(warehouse_path, source_path)

dag_instance = ingest_transactions()

if __name__ == "__main__":
    print("Testing ingest_transactions DAG...")
    dag_instance.test()
