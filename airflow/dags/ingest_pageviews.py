"""
Airflow DAG to ingest pageview/analytics event data into DuckDB.
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
SOURCE_PATH = PROJECT_ROOT / "sources" / "analytics" / "pageviews.csv"

@dag(
    dag_id="ingest_pageviews",
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "analytics"],
)
def ingest_pageviews():
    """DAG to ingest pageview events from analytics source into DuckDB."""
    
    @task()
    def extract_pageviews():
        """Extract pageviews from CSV source."""
        print(f"Extracting pageviews from {SOURCE_PATH}")
        return str(SOURCE_PATH)
    
    @task()
    def load_to_duckdb(warehouse_path: str, source_path: str):
        """Load pageviews into DuckDB raw layer."""
        print(f"Loading pageviews into DuckDB at {warehouse_path}")
        
        conn = duckdb.connect(warehouse_path)
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.pageviews AS
            SELECT * FROM read_csv_auto('{source_path}')
        """)
        
        result = conn.execute("SELECT COUNT(*) FROM raw.pageviews").fetchone()
        print(f"Loaded {result[0]} pageview records into raw.pageviews")
        
        conn.close()
        return result[0]
    
    # Define task dependencies
    warehouse_path = ensure_warehouse_exists()
    source_path = extract_pageviews()
    load_to_duckdb(warehouse_path, source_path)

dag_instance = ingest_pageviews()

if __name__ == "__main__":
    print("Testing ingest_pageviews DAG...")
    dag_instance.test()
