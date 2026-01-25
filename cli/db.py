import duckdb

DB_PATH = "warehouse/data.duckdb"

def get_conn():
    return duckdb.connect(DB_PATH)
