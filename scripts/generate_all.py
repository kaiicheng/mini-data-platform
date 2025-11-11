"""
Master script to generate all synthetic data in the correct dependency order.

Order:
1. Products (foundational)
2. Users (foundational)
3. Marketing/Ad Spend (influences sales)
4. Page Views (user browsing behavior)
5. Sales Transactions (purchases)
"""
from pathlib import Path
from generate_products import generate_products
from generate_users import generate_users
from generate_marketing import generate_marketing
from generate_pageviews import generate_pageviews
from generate_sales import generate_sales

def main():
    base_path = Path(__file__).parent.parent / "sources"
    
    print("=" * 60)
    print("Generating synthetic data for mini-data-platform")
    print("=" * 60)
    
    # Step 1: Generate products
    print("\n[1/5] Generating products...")
    products = generate_products(base_path / "postgres" / "products.csv", num_products=100)
    
    # Step 2: Generate users
    print("\n[2/5] Generating users...")
    users = generate_users(base_path / "postgres" / "users.csv", num_users=5000)
    
    # Step 3: Generate marketing/ad spend data
    print("\n[3/5] Generating marketing data...")
    campaigns = generate_marketing(
        base_path / "salesforce" / "campaigns.csv",
        products=products
    )
    
    # Step 4: Generate page views
    print("\n[4/5] Generating page views...")
    generate_pageviews(
        base_path / "analytics" / "pageviews.csv",
        products=products,
        users=users,
        campaigns=campaigns
    )
    
    # Step 5: Generate sales transactions
    print("\n[5/5] Generating sales transactions...")
    generate_sales(
        base_path / "postgres" / "transactions.csv",
        products=products,
        users=users,
        campaigns=campaigns
    )
    
    print("\n" + "=" * 60)
    print("✓ Data generation complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
