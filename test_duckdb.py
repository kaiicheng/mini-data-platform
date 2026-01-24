# testing

# import duckdb
# con = duckdb.connect("warehouse/warehouse.duckdb")
# print(con.execute("SHOW TABLES").fetchall())



# testing creating a duckdb database and table

import duckdb

con = duckdb.connect("warehouse/data.duckdb")

# check existing tables
print("Before:", con.execute("SHOW TABLES").fetchall())

# create a demo fact table
con.execute("""
  CREATE TABLE fct_orders AS
  SELECT 1 AS order_id, 100 AS amount
  UNION ALL
  SELECT 2, 250;
""")

# check tables again
print("After:", con.execute("SHOW TABLES").fetchall())
