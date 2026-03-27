import pandas as pd

def check_not_null(df, column):
    nulls = df[column].isnull().sum()
    if nulls > 0:
        raise ValueError(f"Nulls encontrados en {column}: {nulls}")

def check_positive(df, column):
    if (df[column] <= 0).any():
        raise ValueError(f"Valores negativos o cero en {column}")

def check_duplicates(df, column):
    duplicates = df.duplicated(subset=[column]).sum()
    if duplicates > 0:
        raise ValueError(f"Duplicados en {column}: {duplicates}")

def check_row_count(df, min_rows=1):
    if len(df) < min_rows:
        raise ValueError("DataFrame vacío")

def run_quality_checks(users, transactions, details):
    print("Ejecutando validaciones de calidad...")

    # Users
    check_not_null(users, "user_id")
    check_duplicates(users, "user_id")

    # Transactions
    check_not_null(transactions, "transaction_id")
    check_duplicates(transactions, "transaction_id")
    check_positive(transactions, "amount")

    # Details
    check_not_null(details, "detail_id")
    check_duplicates(details, "detail_id")

    # Conteo
    check_row_count(users)
    check_row_count(transactions)
    check_row_count(details)

    print("Calidad OK")