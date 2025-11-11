"""
Airflow DAG to ingest user data from Postgres source into DuckDB.
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
SOURCE_PATH = PROJECT_ROOT / "sources" / "postgres" / "users.csv"

@dag(
    dag_id="ingest_users",
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "postgres"],
)
def ingest_users():
    """DAG to ingest user data from Postgres source into DuckDB."""
    
    @task()
    def extract_users():
        """Extract users from CSV source."""
        print(f"Extracting users from {SOURCE_PATH}")
        return str(SOURCE_PATH)
    
    @task()
    def load_to_duckdb(warehouse_path: str, source_path: str):
        """Load users into DuckDB raw layer."""
        print(f"Loading users into DuckDB at {warehouse_path}")
        
        conn = duckdb.connect(warehouse_path)
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.users AS
            SELECT * FROM read_csv_auto('{source_path}')
        """)
        
        result = conn.execute("SELECT COUNT(*) FROM raw.users").fetchone()
        print(f"Loaded {result[0]} user records into raw.users")
        
        conn.close()
        return result[0]
    
    # Define task dependencies
    warehouse_path = ensure_warehouse_exists()
    source_path = extract_users()
    load_to_duckdb(warehouse_path, source_path)

dag_instance = ingest_users()

if __name__ == "__main__":
    print("Testing ingest_users DAG...")
    dag_instance.test()
