import typer
from cli.queries import sales, top_products, product_pairs

app = typer.Typer()

@app.command()
def sales_cmd(
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date (YYYY-MM-DD)")
):
    df = sales(start, end)
    print(df)

@app.command()
def top_products_cmd(
    n: int = typer.Option(5, help="Top N products")
):
    df = top_products(n)
    print(df)

@app.command()
def product_pairs_cmd(
    top: int = typer.Option(10, help="Top product pairs")
):
    df = product_pairs(top)
    print(df)

if __name__ == "__main__":
    app()
