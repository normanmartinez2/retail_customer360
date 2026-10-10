# Retail Customer 360

Repositorio fuente de datasets sintéticos para los laboratorios de Databricks.

Empieza por [LAB 02](LAB02.md). Importa [el notebook](labs/lab02_customer_ingestion.py) en Databricks Free Edition y ejecuta sus celdas en orden.

- `data/crm/`: CSV de 1.220 registros y contrato con checksum y métricas esperadas.
- `tools/generate_customers.py`: generador determinista sin dependencias adicionales.
- `labs/`: notebooks Databricks.

Para futuros datos: `data/<sistema>/<dataset>_labNN_vN.csv` y manifest adyacente; referencias raw fijadas a commit para reproducibilidad. Solo datos ficticios en este repositorio público.
