# Data Pipeline Tipo Medallion - Bronze → Silver → Gold

Este proyecto implementa un **pipeline de datos** que centraliza, transforma y expone información de transacciones financieras usando Python y buenas prácticas de ingeniería de datos.  
Se incluyen **capas Bronze, Silver y Gold**, con validaciones de calidad y trazabilidad completa.


## Requisitos

- Python 3.10+  
- Instalar las dependencias (librerias)
- Configurar las variables de entorno

## Ejecución
- Creación de datos muestra - COMANDO - python src/Data/datos.py
- Ejecutar la ingesta de datos (Bronze) - COMANDO: python -m src.ingestion.Ingestas
- Transformar datos (silver) - COMANDO: python -m src.transformation.silver_transform
- Generar metricas (Gold) - COMANDO: python -m src.transformation.gold_transform
- Visualizar datos (resumen de Kpis) - COMANDO: python -m src.Visualizacion.query_gold

## Notas importantes:
- Cada capa genera datos en su respectiva carpeta:
- data/bronze/ → datos crudos
- data/silver/ → datos limpios, transformados y con validaciones de calidad
- data/gold/ → métricas y resultados finales
- Gold se puede consultar con DuckDB o exportar a SQL/Excel.
- Los datos incluidos son de ejemplo; para probar con datos reales reemplaza los archivos en data/source/.

## Arquitectura Propuesta:

- Bronze: capa de ingesta, datos crudos sin procesar.
- Silver: datos limpios, sin duplicados, reglas de negocio aplicadas.
- Gold: métricas agregadas, KPIs y tablas listas para análisis.
- Consumo: los usuarios pueden consultar las métricas con SQL, DuckDB, etc.

## Decisiones técnicas
- Python + Pandas: para ingesta, transformación y limpieza de datos, suficiente para datasets medianos.
- Generación de datos aleatorios con las caracteristicas solicitadas (campos).
- DuckDB: motor SQL embebido, ideal para exponer Gold de forma rápida y ligera.
- JSON → Parquet:
  Guardar Bronze en JSON crudo (fácil de inspeccionar).
  Silver y Gold en Parquet (eficiente, columnar y comprimido).
- Modularidad:
   src/ingestion/ → scripts de carga de informacion
   src/transformation/ → scripts Silver/Gold
   src/calidad/ → validaciones 
- Variables de entorno: rutas configurables para Bronze/Silver/Gold (.env).
- Trazabilidad completa: desde Bronze → Silver → Gold, sin pérdida de datos.
- Validaciones de calidad: null checks, duplicados, integridad referencial.
