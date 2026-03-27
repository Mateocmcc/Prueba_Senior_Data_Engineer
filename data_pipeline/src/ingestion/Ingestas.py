import os
import pandas as pd
from datetime import datetime
from src.config.setting import BRONZE_PATH

SOURCE_PATH = "data/source"

def read_json(filename):
    return pd.read_json(f"{SOURCE_PATH}/{filename}", lines=True)

def save_to_bronze(df, table_name):
    date_str = datetime.now().strftime("%Y-%m-%d")
    path = f"{BRONZE_PATH}/{table_name}/{date_str}"

    os.makedirs(path, exist_ok=True)

    file_path = f"{path}/data.json"

    df.to_json(file_path, orient="records", lines=True)

def run():
    users = read_json("user.json")
    transactions = read_json("transactions.json")
    details = read_json("transaction_details.json")

    save_to_bronze(users, "users")
    save_to_bronze(transactions, "transactions")
    save_to_bronze(details, "transaction_details")

    print("✅ Ingesta completada a Bronze")

if __name__ == "__main__":
    run()