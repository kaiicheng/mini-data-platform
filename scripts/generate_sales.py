"""
Generate sales transaction data for Postgres source.
This should correlate with pageviews, campaigns, and user behavior.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
ORDER_STATUS = ["completed", "pending", "cancelled", "refunded"]

def generate_sales(output_path: Path, products: list, users: list, campaigns: list):
    """Generate sales transactions that correlate with user segments and campaigns."""
    
    transactions = []
    transaction_id = 1
    
    # Build campaign effectiveness map
    campaign_dates = defaultdict(list)
    for campaign in campaigns:
        start = datetime.fromisoformat(campaign["start_date"]).date()
        end = datetime.fromisoformat(campaign["end_date"]).date()
        product_id = campaign["product_id"]
        campaign_dates[product_id].append((start, end, campaign["campaign_id"]))
    
    # Generate ~10k transactions over 5 years
    start_date = datetime(2020, 1, 1)
    
    # Customer purchase frequency based on segment
    segment_frequency = {
        "high_value": 12,      # ~12 purchases over 5 years
        "medium_value": 5,     # ~5 purchases
        "low_value": 2,        # ~2 purchases
        "churned": 1,          # only 1 purchase
    }
    
    for user in users:
        user_created = datetime.fromisoformat(user["created_at"])
        segment = user["customer_segment"]
        num_purchases = random.randint(
            max(0, segment_frequency[segment] - 2),
            segment_frequency[segment] + 2
        )
        
        for _ in range(num_purchases):
            # Transaction date after user signup
            days_after_signup = random.randint(0, (datetime(2024, 12, 31) - user_created).days)
            transaction_date = user_created + timedelta(days=days_after_signup)
            
            # Don't create future transactions
            if transaction_date > datetime(2024, 12, 31):
                continue
            
            # Number of items in order (1-5)
            num_items = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.25, 0.15, 0.07, 0.03])[0]
            
            # Select products (with potential for related products)
            ordered_products = []
            first_product = random.choice(products)
            ordered_products.append(first_product)
            
            # If buying multiple items, might buy from same category
            for _ in range(num_items - 1):
                if random.random() < 0.6 and first_product["category"]:
                    # 60% chance to buy from same category
                    same_category = [p for p in products if p["category"] == first_product["category"]]
                    if same_category:
                        ordered_products.append(random.choice(same_category))
                    else:
                        ordered_products.append(random.choice(products))
                else:
                    ordered_products.append(random.choice(products))
            
            # Calculate order total
            subtotal = sum(p["price"] for p in ordered_products if p["price"] > 0)
            
            # Tax (8%)
            tax = round(subtotal * 0.08, 2)
            
            # Shipping (free over $50, otherwise $8)
            shipping = 0 if subtotal > 50 else 8.00
            
            total = subtotal + tax + shipping
            
            # Discount code (10% of orders)
            discount = 0
            discount_code = None
            if random.random() < 0.10:
                discount = round(subtotal * random.uniform(0.05, 0.25), 2)
                discount_code = random.choice(["SAVE10", "WELCOME", "FLASH20", "VIP15"])
                total -= discount
            
            payment_method = random.choice(PAYMENT_METHODS)
            
            # Order status (90% completed, 5% cancelled, 3% refunded, 2% pending)
            status = random.choices(
                ORDER_STATUS,
                weights=[0.90, 0.02, 0.05, 0.03]
            )[0]
            
            # Check if any products were part of active campaigns
            attribution_campaign_id = None
            for product in ordered_products:
                if product["product_id"] in campaign_dates:
                    for start, end, cid in campaign_dates[product["product_id"]]:
                        if start <= transaction_date.date() <= end:
                            # 40% chance of campaign attribution
                            if random.random() < 0.4:
                                attribution_campaign_id = cid
                            break
                if attribution_campaign_id:
                    break
            
            # Data quality issues
            # 5% chance of null payment method
            if random.random() < 0.05:
                payment_method = None
            
            # 2% chance of negative total (data error)
            if random.random() < 0.02:
                total = -total
            
            # 3% chance of duplicate transaction_id (data error)
            if random.random() < 0.03:
                tid = random.randint(1, max(1, transaction_id - 100))
            else:
                tid = transaction_id
            
            # Create transaction record for each item (line items)
            for product in ordered_products:
                transactions.append({
                    "transaction_id": tid,
                    "user_id": user["user_id"],
                    "product_id": product["product_id"],
                    "transaction_date": transaction_date.isoformat(),
                    "quantity": 1,  # Simplified: 1 per line item
                    "price": product["price"],
                    "subtotal": subtotal,
                    "tax": tax,
                    "shipping": shipping,
                    "discount": discount,
                    "discount_code": discount_code,
                    "total": total,
                    "payment_method": payment_method,
                    "status": status,
                    "campaign_id": attribution_campaign_id,
                    "created_at": transaction_date.isoformat(),
                })
            
            transaction_id += 1
    
    # Write to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "transaction_id", "user_id", "product_id", "transaction_date",
            "quantity", "price", "subtotal", "tax", "shipping", "discount",
            "discount_code", "total", "payment_method", "status", 
            "campaign_id", "created_at"
        ])
        writer.writeheader()
        writer.writerows(transactions)
    
    print(f"Generated {len(transactions)} transaction line items -> {output_path}")
    return transactions

if __name__ == "__main__":
    # For standalone testing
    from generate_products import generate_products
    from generate_users import generate_users
    from generate_marketing import generate_marketing
    
    base = Path(__file__).parent.parent / "sources"
    products = generate_products(base / "postgres" / "products.csv")
    users = generate_users(base / "postgres" / "users.csv")
    campaigns = generate_marketing(base / "salesforce" / "campaigns.csv", products)
    
    output = base / "postgres" / "transactions.csv"
    generate_sales(output, products, users, campaigns)
