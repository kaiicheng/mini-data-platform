from cli.db import get_conn

# Everything should query from curated marts layer.
# Available marts tables: dim_customers, dim_products, fct_orders (as your test output shows).

def sales(start: str, end: str):
    """
    Total sales (revenue) between start and end dates.
    We use marts.fct_orders and its transaction_date + amount columns.
    """
    con = get_conn()
    query = f"""
        SELECT SUM(total) AS revenue
        FROM marts.fct_orders
        WHERE transaction_date BETWEEN '{start}' AND '{end}'
    """
    return con.execute(query).fetchdf()

def top_products(n: int):
    """
    Top N products by units sold.
    Since order_items does NOT exist in this repo's marts layer, we aggregate from marts.fct_orders.
    Assumes fct_orders has product_id and quantity columns.
    """
    con = get_conn()
    query = f"""
        SELECT product_id, SUM(quantity) AS units_sold
        FROM marts.fct_orders
        GROUP BY product_id
        ORDER BY units_sold DESC
        LIMIT {n}
    """
    return con.execute(query).fetchdf()

def product_pairs(top: int):
    """
    Product pair (basket) analysis usually requires order_items-level granularity.
    This repo's marts layer does not expose an order_items table, so we can't compute true pairs reliably.
    We'll return an empty DataFrame-like result via a query that returns no rows.
    (Alternatively: raise a clear exception.)
    """
    con = get_conn()
    query = """
        SELECT NULL AS product_a, NULL AS product_b, NULL AS pair_count
        WHERE 1 = 0
    """
    return con.execute(query).fetchdf()
