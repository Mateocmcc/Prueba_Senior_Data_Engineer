import pandas as pd
import random
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_users(n=10):
    users = []
    for i in range(n):
        users.append({
            "user_id": i + 1,
            "name": fake.name(),
            "email": fake.email(),
            "created_at": datetime.now()
        })
    return pd.DataFrame(users)

def generate_transactions(users, n=30):
    transactions = []
    for i in range(n):
        user_id = random.choice(users["user_id"].tolist())
        transactions.append({
            "transaction_id": i + 1,
            "user_id": user_id,
            "amount": round(random.uniform(10, 1000), 2),
            "status": random.choice(["success", "failed"]),
            "created_at": datetime.now().isoformat()
        })
    return pd.DataFrame(transactions)

def generate_details(transactions):
    details = []
    for i, row in transactions.iterrows():
        details.append({
            "detail_id": i + 1,
            "transaction_id": row["transaction_id"],
            "payment_method": random.choice(["card", "pse", "cash"]),
            "channel": random.choice(["web", "mobile", "api"]),
            "processing_time_ms": random.randint(100, 2000)
        })
    return pd.DataFrame(details)

def run():
    users = generate_users()
    transactions = generate_transactions(users)
    details = generate_details(transactions)

    users.to_json("data/source/user.json", orient="records", lines=True, mode="w")
    transactions.to_json("data/source/transactions.json", orient="records", lines=True, mode="w")
    details.to_json("data/source/transaction_details.json", orient="records", lines=True, mode="w")

    print("Datos generados en source")

if __name__ == "__main__":
    run()
