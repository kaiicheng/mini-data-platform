"""
Generate user/customer data for Postgres source.
This should be run second as transactions depend on users.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis", "Seattle",
    "Denver", "Boston", "Portland", "Nashville", "Miami", "Atlanta",
]

STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "NC", "WA", "CO", "MA", "OR", "TN", "GA"]

def generate_email(first_name, last_name, user_id):
    """Generate email with occasional duplicates/issues."""
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com"]
    
    # 5% chance of duplicate/malformed email
    if random.random() < 0.05:
        return f"user{random.randint(1, 100)}@{random.choice(domains)}"
    
    # 2% chance of missing domain
    if random.random() < 0.02:
        return f"{first_name.lower()}.{last_name.lower()}"
    
    return f"{first_name.lower()}.{last_name.lower()}{user_id}@{random.choice(domains)}"

def generate_users(output_path: Path, num_users: int = 5000):
    """Generate user data with some intentional data quality issues."""
    
    users = []
    
    for user_id in range(1, num_users + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        
        # User signup date over 5 years
        created_at = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825))
        
        email = generate_email(first_name, last_name, user_id)
        
        # 3% chance of null email
        if random.random() < 0.03:
            email = None
        
        city = random.choice(CITIES)
        state = random.choice(STATES)
        
        # 10% chance of missing city/state
        if random.random() < 0.10:
            city = None
            state = None
        
        # Age distribution
        age = random.randint(18, 75)
        
        # 5% chance of unrealistic age
        if random.random() < 0.05:
            age = random.choice([0, -5, 150, 999])
        
        # Customer segment based on behavior (will correlate with sales later)
        segments = ["high_value", "medium_value", "low_value", "churned"]
        weights = [0.1, 0.3, 0.5, 0.1]
        segment = random.choices(segments, weights=weights)[0]
        
        users.append({
            "user_id": user_id,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "city": city,
            "state": state,
            "age": age,
            "customer_segment": segment,
            "created_at": created_at.isoformat(),
            "last_login": (created_at + timedelta(days=random.randint(0, 100))).isoformat(),
        })
    
    # Write to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "user_id", "email", "first_name", "last_name", 
            "city", "state", "age", "customer_segment", "created_at", "last_login"
        ])
        writer.writeheader()
        writer.writerows(users)
    
    print(f"Generated {len(users)} users -> {output_path}")
    return users

if __name__ == "__main__":
    output = Path(__file__).parent.parent / "sources" / "postgres" / "users.csv"
    generate_users(output)
