"""
Airflow DAG to ingest product catalog data from Postgres source into DuckDB.
"""
from datetime import datetime
from pathlib import Path
import sys
import duckdb
from airflow.sdk import dag, task

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.warehouse import ensure_warehouse_exists, WAREHOUSE_PATH

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
SOURCE_PATH = PROJECT_ROOT / "sources" / "postgres" / "products.csv"

@dag(
    dag_id="ingest_products",
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "postgres"],
)
def ingest_products():
    """DAG to ingest product catalog from Postgres source into DuckDB."""
    
    @task()
    def extract_products():
        """Extract products from CSV source."""
        print(f"Extracting products from {SOURCE_PATH}")
        return str(SOURCE_PATH)
    
    @task()
    def load_to_duckdb(warehouse_path: str, source_path: str):
        """Load products into DuckDB raw layer."""
        print(f"Loading products into DuckDB at {warehouse_path}")
        
        # Connect to DuckDB
        conn = duckdb.connect(warehouse_path)
        
        # Load CSV into DuckDB
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.products AS
            SELECT * FROM read_csv_auto('{source_path}')
        """)
        
        # Show record count
        result = conn.execute("SELECT COUNT(*) FROM raw.products").fetchone()
        print(f"Loaded {result[0]} product records into raw.products")
        
        conn.close()
        return result[0]
    
    # Define task dependencies
    warehouse_path = ensure_warehouse_exists()
    source_path = extract_products()
    load_to_duckdb(warehouse_path, source_path)

dag_instance = ingest_products()

if __name__ == "__main__":
    print("Testing ingest_products DAG...")
    dag_instance.test()
