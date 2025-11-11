"""
Generate product catalog data for Postgres source.
This should be run first as other datasets depend on products.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Product categories and attributes
CATEGORIES = {
    "Electronics": ["Laptop", "Phone", "Tablet", "Headphones", "Smartwatch", "Camera", "Speaker"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Sweater", "Shorts"],
    "Home & Garden": ["Coffee Maker", "Blender", "Vacuum", "Lamp", "Pillow", "Rug", "Plant Pot"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Running Shoes", "Bicycle", "Tennis Racket", "Basketball"],
    "Books": ["Fiction Novel", "Cookbook", "Self-Help", "Biography", "Technical Manual", "Children's Book"],
}

BRANDS = ["ProBrand", "ValueCo", "Premium", "EcoGoods", "TechFirst", "ClassicMake", "ModernLine"]

def generate_products(output_path: Path, num_products: int = 100):
    """Generate product catalog with some intentional data quality issues."""
    
    products = []
    product_id = 1
    
    for category, product_types in CATEGORIES.items():
        for product_type in product_types:
            for _ in range(random.randint(1, 3)):  # 1-3 variants per product type
                brand = random.choice(BRANDS)
                
                # Base price with variation
                base_prices = {
                    "Electronics": random.randint(100, 1500),
                    "Clothing": random.randint(20, 200),
                    "Home & Garden": random.randint(30, 300),
                    "Sports": random.randint(25, 500),
                    "Books": random.randint(10, 50),
                }
                price = base_prices[category]
                
                # Cost (for margin analysis)
                cost = round(price * random.uniform(0.3, 0.7), 2)
                
                # Created date (products added over time)
                created_at = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1800))
                
                # Introduce data quality issues
                product_name = f"{brand} {product_type}"
                
                # 5% chance of null/missing brand
                if random.random() < 0.05:
                    brand = None
                
                # 3% chance of negative price (data error)
                if random.random() < 0.03:
                    price = -price
                
                # 2% chance of null category
                category_value = None if random.random() < 0.02 else category
                
                # Stock levels
                stock = random.randint(0, 500)
                
                # Some products discontinued (null stock, old created_at)
                if random.random() < 0.1 and created_at < datetime(2021, 1, 1):
                    stock = None
                
                products.append({
                    "product_id": product_id,
                    "product_name": product_name,
                    "brand": brand,
                    "category": category_value,
                    "price": price,
                    "cost": cost,
                    "stock_quantity": stock,
                    "created_at": created_at.isoformat(),
                    "updated_at": (created_at + timedelta(days=random.randint(0, 100))).isoformat(),
                })
                
                product_id += 1
                
                if len(products) >= num_products:
                    break
            if len(products) >= num_products:
                break
        if len(products) >= num_products:
            break
    
    # Write to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "product_id", "product_name", "brand", "category", 
            "price", "cost", "stock_quantity", "created_at", "updated_at"
        ])
        writer.writeheader()
        writer.writerows(products)
    
    print(f"Generated {len(products)} products -> {output_path}")
    return products

if __name__ == "__main__":
    output = Path(__file__).parent.parent / "sources" / "postgres" / "products.csv"
    generate_products(output)
