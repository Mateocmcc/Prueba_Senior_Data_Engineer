import pandas as pd

SILVER_PATH = "./data/silver"
GOLD_PATH = "./data/gold"

def read_silver(entity):
    return pd.read_parquet(f"{SILVER_PATH}/{entity}/data.parquet")

def save_gold(df, name):
    import os
    os.makedirs(GOLD_PATH, exist_ok=True)
    df.to_parquet(f"{GOLD_PATH}/{name}.parquet", index=False)

def run():
    print(" Generando capa Gold")

    users = read_silver("users")
    transactions = read_silver("transactions")
    details = read_silver("transaction_details")

    # Join completo
    df = transactions.merge(users, on="user_id", how="inner")
    df = df.merge(details, on="transaction_id", how="left")

    # KPIs generales
    total_transactions = len(df)
    total_amount = df["amount"].sum()
    success_rate = (df["status"] == "success").mean()
    
    kpis = pd.DataFrame([{
    "total_transactions": total_transactions,
    "total_amount": total_amount,
    "success_rate": success_rate
}])

    # Métricas por usuario
    user_metrics = df.groupby("user_id").agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        success_rate=("status", lambda x: (x == "success").mean())
    ).reset_index()

    # Por método de pago
    payment_metrics = df.groupby("payment_method").agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum")
    ).reset_index()

    # Por canal
    channel_metrics = df.groupby("channel").agg(
        total_transactions=("transaction_id", "count"),
        avg_processing_time=("processing_time_ms", "mean")
    ).reset_index()

    # Guardar
    save_gold(kpis, "kpis_generales")
    save_gold(user_metrics, "user_metrics")
    save_gold(payment_metrics, "payment_metrics")
    save_gold(channel_metrics, "channel_metrics")

    print("✅ Gold listo")

if __name__ == "__main__":
    run()
