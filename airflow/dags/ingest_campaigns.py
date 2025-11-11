"""
Airflow DAG to ingest marketing campaign data from Salesforce source into DuckDB.
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
SOURCE_PATH = PROJECT_ROOT / "sources" / "salesforce" / "campaigns.csv"

@dag(
    dag_id="ingest_campaigns",
    start_date=datetime(2020, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "salesforce"],
)
def ingest_campaigns():
    """DAG to ingest marketing campaigns from Salesforce source into DuckDB."""
    
    @task()
    def extract_campaigns():
        """Extract campaigns from CSV source."""
        print(f"Extracting campaigns from {SOURCE_PATH}")
        return str(SOURCE_PATH)
    
    @task()
    def load_to_duckdb(warehouse_path: str, source_path: str):
        """Load campaigns into DuckDB raw layer."""
        print(f"Loading campaigns into DuckDB at {warehouse_path}")
        
        conn = duckdb.connect(warehouse_path)
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.campaigns AS
            SELECT * FROM read_csv_auto('{source_path}')
        """)
        
        result = conn.execute("SELECT COUNT(*) FROM raw.campaigns").fetchone()
        print(f"Loaded {result[0]} campaign records into raw.campaigns")
        
        conn.close()
        return result[0]
    
    # Define task dependencies
    warehouse_path = ensure_warehouse_exists()
    source_path = extract_campaigns()
    load_to_duckdb(warehouse_path, source_path)

dag_instance = ingest_campaigns()

if __name__ == "__main__":
    print("Testing ingest_campaigns DAG...")
    dag_instance.test()
