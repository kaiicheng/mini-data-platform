import typer
from cli.queries import sales, top_products
from cli.agent import DataAgent

app = typer.Typer()

@app.command()
def sales_cmd(
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date (YYYY-MM-DD)")
):
    """Get sales revenue between two dates"""
    try:
        df = sales(start, end)
        revenue = df.iloc[0]['revenue']
        print(f"💰 Sales revenue from {start} to {end}: ${revenue:,.2f}")
    except Exception as e:
        print(f"❌ Error: {e}")

@app.command()
def top_products_cmd(
    n: int = typer.Option(5, help="Top N products")
):
    """Get top N products by units sold"""
    try:
        df = top_products(n)
        print(f"📦 Top {n} products:")
        for i, (_, row) in enumerate(df.iterrows(), 1):
            print(f"  {i}. Product {row['product_id']}: {row['units_sold']:,} units")
    except Exception as e:
        print(f"❌ Error: {e}")

@app.command()
def schema():
    """Show database schema information"""
    try:
        agent = DataAgent()
        print(agent.get_schema_summary())
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    app()



# # test database connection and basic queries
# from cli.db import get_conn

# def test_connection():
#     try:
#         con = get_conn()

#         # 1️⃣ list schemas
#         schemas = con.execute("""
#             SELECT schema_name
#             FROM information_schema.schemata
#             ORDER BY schema_name
#         """).fetchall()
#         print("Available schemas:", schemas)

#         # 2️⃣ list tables in marts
#         tables = con.execute("""
#             SELECT table_name
#             FROM information_schema.tables
#             WHERE table_schema = 'marts'
#             ORDER BY table_name
#         """).fetchall()
#         print("Marts tables:", tables)

#         return True
#     except Exception as e:
#         print(f"Database connection error: {e}")
#         return False


# if __name__ == "__main__":
#     test_connection()




# import typer
# from cli.queries import sales, top_products, product_pairs

# app = typer.Typer()

# @app.command()
# def sales_cmd(
#     start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
#     end: str = typer.Option(..., help="End date (YYYY-MM-DD)")
# ):
#     df = sales(start, end)
#     print(df)

# @app.command()
# def top_products_cmd(
#     n: int = typer.Option(5, help="Top N products")
# ):
#     df = top_products(n)
#     print(df)

# @app.command()
# def product_pairs_cmd(
#     top: int = typer.Option(10, help="Top product pairs")
# ):
#     df = product_pairs(top)
#     print(df)

# if __name__ == "__main__":
#     app()


