from src.ingestion.Ingestas import run as run_ingestion
from src.transformation.silver_transform import run as run_silver
from src.transformation.gold_transform import run as run_gold

def run_pipeline():
    print("Pipeline iniciado")

    run_ingestion()
    run_silver()
    run_gold()

    print("Pipeline finalizado")

if __name__ == "__main__":
    run_pipeline()
