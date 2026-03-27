import duckdb

def run():
    
    con = duckdb.connect()

    # Crear vistas desde parquet
    con.execute("""
        CREATE VIEW user_metrics AS 
        SELECT * FROM 'data/gold/user_metrics.parquet'
    """)

    con.execute("""
        CREATE VIEW payment_metrics AS 
        SELECT * FROM 'data/gold/payment_metrics.parquet'
    """)

    con.execute("""
        CREATE VIEW channel_metrics AS 
        SELECT * FROM 'data/gold/channel_metrics.parquet'
    """)

    con.execute("""
        CREATE VIEW kpis_generales AS 
        SELECT * FROM 'data/gold/kpis_generales.parquet'
    """)

    # Query 1: Top usuarios
    print("Top usuarios por monto:")
    result1 = con.execute("""
        SELECT *
        FROM user_metrics
        ORDER BY total_amount DESC
        
    """).fetchdf()
    print(result1)

    # Query 2: Métodos de pago
    print("Métricas por método de pago:")
    result2 = con.execute("""
        SELECT *
        FROM payment_metrics
    """).fetchdf()
    print(result2)

    # Query 3: Canales
    print("Métricas por canal:")
    result3 = con.execute("""
        SELECT *
        FROM channel_metrics
    """).fetchdf()
    print(result3)

    #Query 4: KPIs generales
    print("KPIs generales:")
    result4 = con.execute("""
        SELECT *
        FROM kpis_generales
    """).fetchdf()
    print(result4)

if __name__ == "__main__":
    run()