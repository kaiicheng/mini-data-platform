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
