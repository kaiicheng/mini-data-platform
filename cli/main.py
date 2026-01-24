# simple testing version

import typer
from cli.queries import sales

def main(
        start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
        end: str = typer.Option(..., help="End date (YYYY-MM-DD)"),
    ):
    # Query sales revenue between start and end date.
    df = sales(start, end)
    print(df)

if __name__ == "__main__":
    typer.run(main)
