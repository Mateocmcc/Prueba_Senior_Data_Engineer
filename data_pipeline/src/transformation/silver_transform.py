import pandas as pd
import os
from src.config.setting import BRONZE_PATH
from src.calidad.validacion_calidad import run_quality_checks

SILVER_PATH = "./data/silver"

def read_bronze(entity):
    path = f"{BRONZE_PATH}/{entity}"
    all_files = []

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".json"):
                full_path = os.path.join(root, file)
                df = pd.read_json(full_path,lines=True)
                all_files.append(df)

    return pd.concat(all_files, ignore_index=True)

def clean_users(df):
    df = df.drop_duplicates(subset=["user_id"])
    df = df.dropna(subset=["user_id", "email"])
    return df

def clean_transactions(df):
    df = df.drop_duplicates(subset=["transaction_id"])
    df = df[df["amount"] > 0]
    df["status"] = df["status"].str.lower()
    return df

def clean_transaction_details(df):
    df = df.drop_duplicates(subset=["detail_id"])
    return df

def save_silver(df, entity):
    path = f"{SILVER_PATH}/{entity}"
    os.makedirs(path, exist_ok=True)
    df.to_parquet(f"{path}/data.parquet", index=False)

def run():
    users = read_bronze("users")
    transactions = read_bronze("transactions")
    details = read_bronze("transaction_details")

    users_clean = clean_users(users)
    transactions_clean = clean_transactions(transactions)
    details_clean = clean_transaction_details(details)

    # Integridad referencial
    transactions_clean = transactions_clean[
        transactions_clean["user_id"].isin(users_clean["user_id"])
    ]

    details_clean = details_clean[
        details_clean["transaction_id"].isin(transactions_clean["transaction_id"])
    ]

    run_quality_checks(users_clean, transactions_clean, details_clean)

    save_silver(users_clean, "users")
    save_silver(transactions_clean, "transactions")
    save_silver(details_clean, "transaction_details")

    print("Silver listo")

if __name__ == "__main__":
    run()