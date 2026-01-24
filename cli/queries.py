from cli.db import get_conn

def sales(start: str, end: str):
    con = get_conn()
    query = f"""
        SELECT
            SUM(order_total) AS revenue
        FROM fct_orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
    """
    return con.execute(query).fetchdf()


def top_products(n: int):
    con = get_conn()
    query = f"""
        SELECT
            product_id,
            SUM(quantity) AS units_sold
        FROM order_items
        GROUP BY product_id
        ORDER BY units_sold DESC
        LIMIT {n}
    """
    return con.execute(query).fetchdf()


def product_pairs(top: int):
    con = get_conn()
    query = f"""
        SELECT
            a.product_id AS product_a,
            b.product_id AS product_b,
            COUNT(*) AS pair_count
        FROM order_items a
        JOIN order_items b
            ON a.order_id = b.order_id
           AND a.product_id < b.product_id
        GROUP BY product_a, product_b
        ORDER BY pair_count DESC
        LIMIT {top}
    """
    return con.execute(query).fetchdf()
