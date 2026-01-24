import duckdb

DB_PATH = "warehouse/warehouse.duckdb"

def get_conn():
    return duckdb.connect(DB_PATH)
