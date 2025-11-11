"""
Generate page view/analytics events data.
This should correlate with marketing campaigns and predict sales.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

PAGE_TYPES = ["home", "product_detail", "category", "cart", "checkout", "search", "account"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]

def generate_pageviews(output_path: Path, products: list, users: list, campaigns: list):
    """Generate pageview events that correlate with campaigns and lead to sales."""
    
    pageviews = []
    event_id = 1
    
    # Create campaign influence map
    campaign_map = {}
    for campaign in campaigns:
        start = datetime.fromisoformat(campaign["start_date"]).date()
        end = datetime.fromisoformat(campaign["end_date"]).date()
        product_id = campaign["product_id"]
        
        if product_id not in campaign_map:
            campaign_map[product_id] = []
        campaign_map[product_id].append((start, end, campaign["campaign_id"]))
    
    # Generate ~50k pageviews over 5 years
    start_date = datetime(2020, 1, 1)
    
    for _ in range(50000):
        # Random timestamp
        event_time = start_date + timedelta(
            days=random.randint(0, 1825),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # 70% of pageviews are from registered users, 30% anonymous
        if random.random() < 0.7 and users:
            user = random.choice(users)
            user_id = user["user_id"]
            # Only include users who have signed up by this time
            if datetime.fromisoformat(user["created_at"]) > event_time:
                user_id = None
        else:
            user_id = None
        
        page_type = random.choice(PAGE_TYPES)
        
        # If product page, pick a product
        product_id = None
        campaign_id = None
        
        if page_type == "product_detail":
            product = random.choice(products)
            product_id = product["product_id"]
            
            # Check if there's an active campaign for this product at this time
            if product_id in campaign_map:
                for start, end, cid in campaign_map[product_id]:
                    if start <= event_time.date() <= end:
                        # 60% chance this view is attributed to the campaign
                        if random.random() < 0.6:
                            campaign_id = cid
                        break
        
        device = random.choice(DEVICES)
        browser = random.choice(BROWSERS)
        
        # Session duration in seconds (1 min to 30 min)
        session_duration = random.randint(60, 1800)
        
        # Data quality issues
        # 5% chance of null user_agent fields
        if random.random() < 0.05:
            device = None
            browser = None
        
        # 2% chance of negative session duration
        if random.random() < 0.02:
            session_duration = -session_duration
        
        # 3% chance of future timestamp (data error)
        if random.random() < 0.03:
            event_time = datetime(2030, 1, 1)
        
        pageviews.append({
            "event_id": event_id,
            "event_time": event_time.isoformat(),
            "user_id": user_id,
            "session_id": f"session_{random.randint(1, 100000)}",
            "page_type": page_type,
            "product_id": product_id,
            "campaign_id": campaign_id,
            "device": device,
            "browser": browser,
            "session_duration_seconds": session_duration,
        })
        
        event_id += 1
    
    # Write to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "event_id", "event_time", "user_id", "session_id", 
            "page_type", "product_id", "campaign_id", "device", 
            "browser", "session_duration_seconds"
        ])
        writer.writeheader()
        writer.writerows(pageviews)
    
    print(f"Generated {len(pageviews)} pageview events -> {output_path}")
    return pageviews

if __name__ == "__main__":
    # For standalone testing
    from generate_products import generate_products
    from generate_users import generate_users
    from generate_marketing import generate_marketing
    
    base = Path(__file__).parent.parent / "sources"
    products = generate_products(base / "postgres" / "products.csv")
    users = generate_users(base / "postgres" / "users.csv")
    campaigns = generate_marketing(base / "salesforce" / "campaigns.csv", products)
    
    output = base / "analytics" / "pageviews.csv"
    generate_pageviews(output, products, users, campaigns)
