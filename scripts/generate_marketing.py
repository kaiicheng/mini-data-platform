"""
Generate marketing/ad spend data for Salesforce source.
This should reference products and influence later sales/pageviews.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

CAMPAIGN_TYPES = ["Social Media", "Search", "Display", "Email", "Video", "Influencer"]
PLATFORMS = ["Facebook", "Google", "Instagram", "YouTube", "TikTok", "LinkedIn", "Twitter"]

def generate_marketing(output_path: Path, products: list):
    """Generate marketing campaign data that references products."""
    
    campaigns = []
    campaign_id = 1
    
    # Generate campaigns over 5 years
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Create ~500 campaigns
    for _ in range(500):
        # Random campaign duration (7-90 days)
        campaign_start = start_date + timedelta(days=random.randint(0, 1825))
        campaign_end = campaign_start + timedelta(days=random.randint(7, 90))
        
        # Pick 1-5 products to promote
        promoted_products = random.sample(products, k=random.randint(1, min(5, len(products))))
        
        campaign_type = random.choice(CAMPAIGN_TYPES)
        platform = random.choice(PLATFORMS)
        
        # Budget varies by campaign type
        budget_ranges = {
            "Social Media": (1000, 50000),
            "Search": (2000, 100000),
            "Display": (500, 30000),
            "Email": (200, 10000),
            "Video": (5000, 150000),
            "Influencer": (3000, 80000),
        }
        
        budget = random.randint(*budget_ranges[campaign_type])
        
        # Actual spend (sometimes over/under budget)
        spend_multiplier = random.uniform(0.7, 1.2)
        actual_spend = round(budget * spend_multiplier, 2)
        
        # Impressions and clicks
        impressions = int(actual_spend * random.uniform(50, 500))
        clicks = int(impressions * random.uniform(0.01, 0.08))  # 1-8% CTR
        
        # Data quality issues
        # 5% chance of null spend
        if random.random() < 0.05:
            actual_spend = None
        
        # 3% chance of negative impressions (data error)
        if random.random() < 0.03:
            impressions = -impressions
        
        # 2% chance of clicks > impressions (impossible, data error)
        if random.random() < 0.02:
            clicks = impressions * 2
        
        for product in promoted_products:
            campaigns.append({
                "campaign_id": campaign_id,
                "campaign_name": f"{campaign_type} - {platform} - {campaign_start.strftime('%b%Y')}",
                "campaign_type": campaign_type,
                "platform": platform,
                "product_id": product["product_id"],
                "start_date": campaign_start.date().isoformat(),
                "end_date": campaign_end.date().isoformat(),
                "budget": budget,
                "actual_spend": actual_spend,
                "impressions": impressions,
                "clicks": clicks,
                "created_at": (campaign_start - timedelta(days=random.randint(1, 14))).isoformat(),
            })
            campaign_id += 1
    
    # Write to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "campaign_id", "campaign_name", "campaign_type", "platform",
            "product_id", "start_date", "end_date", "budget", "actual_spend",
            "impressions", "clicks", "created_at"
        ])
        writer.writeheader()
        writer.writerows(campaigns)
    
    print(f"Generated {len(campaigns)} campaign records -> {output_path}")
    return campaigns

if __name__ == "__main__":
    # For standalone testing
    from generate_products import generate_products
    products = generate_products(
        Path(__file__).parent.parent / "sources" / "postgres" / "products.csv"
    )
    output = Path(__file__).parent.parent / "sources" / "salesforce" / "campaigns.csv"
    generate_marketing(output, products)
