from src.Data.datos import run as run_datos
from src.ingestion.Ingestas import run as run_ingestion
from src.transformation.silver_transform import run as run_silver
from src.transformation.gold_transform import run as run_gold
from src.Visualizacion.query_gold import run as run_Visualizacion

def run_pipeline():
    print("Pipeline iniciado")

    run_datos()
    run_ingestion()
    run_silver()
    run_gold()
    run_Visualizacion()

    print("Pipeline finalizado")

if __name__ == "__main__":
    run_pipeline()
