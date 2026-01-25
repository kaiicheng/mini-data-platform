import typer
from cli.queries import sales, top_products, product_pairs
from cli.agent import DataAgent

app = typer.Typer()

# basic commands
@app.command()
def test():
    """Test database connection and show schema"""
    try:
        from cli.db import get_conn
        con = get_conn()
        
        schemas = con.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            ORDER BY schema_name
        """).fetchall()
        print("✅ Available schemas:", [s[0] for s in schemas])

        tables = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'marts'
            ORDER BY table_name
        """).fetchall()
        print("✅ Marts tables:", [t[0] for t in tables])
        
        # Show row counts for each table
        for table in tables:
            count = con.execute(f"SELECT COUNT(*) FROM marts.{table[0]}").fetchone()[0]
            print(f"   📊 {table[0]}: {count:,} rows")
        
    except Exception as e:
        print(f"❌ Database connection error: {e}")

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
def product_pairs_cmd(
    top: int = typer.Option(10, help="Top product pairs")
):
    """Get top product pairs that are bought together"""
    try:
        df = product_pairs(top)
        print(f"🔗 Top {top} product pairs:")
        for i, (_, row) in enumerate(df.iterrows(), 1):
            print(f"  {i}. Products {row['product_a']} & {row['product_b']}: {row['pair_count']} times")
    except Exception as e:
        print(f"❌ Error: {e}")

# agent feature
@app.command()
def ask(question: str = typer.Argument(..., help="Ask a question about the data")):
    """Ask a natural language question about the data platform"""
    try:
        agent = DataAgent()
        response = agent.process_question(question)
        print(response)
    except Exception as e:
        print(f"❌ Error: {e}")

@app.command() 
def chat():
    """Start an interactive chat session with the data agent"""
    try:
        agent = DataAgent()
        print("🤖 Welcome to the Mini Data Platform Agent!")
        print("Ask me anything about your data. Type 'help' for examples or 'exit' to quit.\n")
        
        while True:
            try:
                question = input("❓ Your question: ")
                if question.lower() in ['exit', 'quit', 'bye']:
                    print("👋 Goodbye!")
                    break
                elif question.lower() in ['help', 'h']:
                    print(agent._provide_help())
                    continue
                elif question.lower() == 'schema':
                    print(agent.get_schema_summary())
                    continue
                
                if question.strip():
                    response = agent.process_question(question)
                    print(f"{response}\n")
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except EOFError:
                break
    except Exception as e:
        print(f"❌ Error starting chat: {e}")

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
