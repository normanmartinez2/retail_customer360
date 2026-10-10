# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 02 — Customer Data Ingestion con PySpark y Delta Lake
# MAGIC Ejecuta las celdas en orden. Consulta LAB02.md para explicaciones y ejercicios.

# COMMAND ----------
import csv
import io
import json
import hashlib
import os
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

REPO = 'normanmartinez2/retail_customer360'
REF = 'feab369722cbe23060b989c6e4a767abdf82cb6e'  # Versión publicada y reproducible del dataset.
BASE_URL = f'https://raw.githubusercontent.com/{REPO}/{REF}/data/crm'
CSV_URL = f'{BASE_URL}/customers_lab02_v1.csv'
MANIFEST_URL = f'{BASE_URL}/customers_lab02_v1.manifest.json'
TABLE = 'retail_customer360.bronze.customers_raw'
VOLUME = '/Volumes/retail_customer360/bronze/source_files'
LOCAL_PATH = f'{VOLUME}/crm/customers_lab02_v1.csv'
MANIFEST_PATH = f'{VOLUME}/crm/customers_lab02_v1.manifest.json'
MANUAL_UPLOAD = False  # True solo si subiste ambos archivos de GitHub al volumen.

# COMMAND ----------
spark.sql('CREATE CATALOG IF NOT EXISTS retail_customer360')
spark.sql('CREATE SCHEMA IF NOT EXISTS retail_customer360.bronze')
spark.sql('CREATE VOLUME IF NOT EXISTS retail_customer360.bronze.source_files')
os.makedirs(f'{VOLUME}/crm', exist_ok=True)

# COMMAND ----------
if not MANUAL_UPLOAD:
    # GET desde la URL raw: no es un conector Salesforce ni una lectura HTTPS nativa de Spark.
    response = requests.get(CSV_URL, timeout=(10, 60))
    response.raise_for_status()
    manifest_response = requests.get(MANIFEST_URL, timeout=(10, 60))
    manifest_response.raise_for_status()
    payload = response.content
    manifest = manifest_response.json()
else:
    with open(LOCAL_PATH, 'rb') as f:
        payload = f.read()
    with open(MANIFEST_PATH, encoding='utf-8') as f:
        manifest = json.load(f)

actual_sha = hashlib.sha256(payload).hexdigest()
assert actual_sha == manifest['sha256'], 'Checksum distinto: comprueba versión y descarga.'
parsed = list(csv.reader(io.StringIO(payload.decode('utf-8'), newline='')))
assert parsed[0] == manifest['columns'], 'Cabecera distinta al contrato.'
assert len(parsed) - 1 == manifest['rows'] == 1220
assert all(len(row) == len(manifest['columns']) for row in parsed[1:])
if not MANUAL_UPLOAD:
    with open(LOCAL_PATH, 'wb') as f:
        f.write(payload)
    with open(MANIFEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
print({'source_url': CSV_URL, 'rows': manifest['rows'], 'sha256': actual_sha})

# COMMAND ----------
# Bronze conserva los valores recibidos como strings; conversiones de negocio en Silver.
schema = StructType([StructField(name, StringType(), True) for name in manifest['columns']])
df_raw = (spark.read.format('csv')
    .schema(schema)
    .option('header', True)
    .option('enforceSchema', False)
    .option('encoding', 'UTF-8')
    .option('sep', ',')
    .option('quote', '"')
    .option('escape', '"')
    .option('multiLine', True)
    .option('mode', 'FAILFAST')
    .option('ignoreLeadingWhiteSpace', False)
    .option('ignoreTrailingWhiteSpace', False)
    .load(LOCAL_PATH))
assert df_raw.count() == manifest['rows']
df_raw.printSchema()
display(df_raw.limit(20))

# COMMAND ----------
def missing(name):
    return F.col(name).isNull() | (F.trim(F.col(name)) == '')

df_profile = (df_raw
    .withColumn('_dob', F.expr('try_cast(date_of_birth AS DATE)'))
    .withColumn('_created', F.expr('try_cast(created_at AS TIMESTAMP)'))
    .withColumn('_updated', F.expr('try_cast(updated_at AS TIMESTAMP)')))
email_ok = F.trim(F.col('email')).rlike(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')
checks = {
    'missing_customer_id': missing('customer_id'),
    'missing_email': missing('email'),
    'invalid_nonempty_email': ~missing('email') & ~email_ok,
    'invalid_birth_dates': ~missing('date_of_birth') & F.col('_dob').isNull(),
    'future_birth_dates': F.col('_dob') > F.to_date(F.lit('2026-10-10')),
    'updated_before_created': F.col('_updated') < F.col('_created'),
    'invalid_updated_at': ~missing('updated_at') & F.col('_updated').isNull(),
    'multiline_notes': F.col('notes').contains('\n'),
}
metrics = df_profile.agg(*[
    F.sum(F.when(condition, 1).otherwise(0)).alias(name)
    for name, condition in checks.items()
]).first().asDict()
duplicates = (df_raw.filter(~missing('customer_id'))
    .groupBy('customer_id').count().filter('count > 1'))
metrics['duplicate_key_groups'] = duplicates.count()
metrics['excess_rows_by_nonempty_key'] = duplicates.agg(
    F.sum(F.col('count')-1).alias('excess')).first()['excess'] or 0
metrics['exact_duplicate_excess'] = df_raw.count() - df_raw.dropDuplicates().count()
for name, expected in manifest['expected'].items():
    assert metrics[name] == expected, f'{name}: {metrics[name]} != {expected}'
print(metrics)
display(duplicates.orderBy('customer_id'))
display(df_profile.filter(checks['invalid_birth_dates']))

# COMMAND ----------
# Metadatos constantes para este lote; UUID identifica una ejecución, hash identifica contenido.
from datetime import datetime, timezone
from uuid import uuid4
batch_id = str(uuid4())
ingested_at = datetime.now(timezone.utc)
df_bronze = (df_raw
    .withColumn('_ingested_at', F.lit(ingested_at))
    .withColumn('_batch_id', F.lit(batch_id))
    .withColumn('_source_system', F.lit('CRM_SIMULATED'))
    .withColumn('_source_url', F.lit(CSV_URL))
    .withColumn('_source_ref', F.lit(REF))
    .withColumn('_source_sha256', F.lit(actual_sha)))

# Evita reingestar el mismo archivo durante repeticiones secuenciales del laboratorio.
# No es una garantía frente a ejecuciones concurrentes; ejecuta un solo notebook a la vez.
already_loaded = spark.catalog.tableExists(TABLE) and (
    spark.table(TABLE).filter(F.col('_source_sha256') == actual_sha).limit(1).count() > 0)
if already_loaded:
    print('Este contenido ya fue ingerido; no se vuelve a insertar.')
else:
    df_bronze.write.format('delta').mode('append').saveAsTable(TABLE)

# COMMAND ----------
loaded = spark.table(TABLE).filter(F.col('_source_sha256') == actual_sha)
assert loaded.count() == manifest['rows']
# Comprueba valores y multiplicidad; los duplicados intencionales se conservan.
assert loaded.select(*manifest['columns']).exceptAll(df_raw).limit(1).count() == 0
assert df_raw.exceptAll(loaded.select(*manifest['columns'])).limit(1).count() == 0
print('Validación de contenido Bronze completada.')
display(spark.sql(f'DESCRIBE DETAIL {TABLE}'))
display(spark.sql(f'DESCRIBE HISTORY {TABLE}'))

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT _source_sha256, COUNT(*) AS records, COUNT(DISTINCT _batch_id) AS batches
# MAGIC FROM retail_customer360.bronze.customers_raw
# MAGIC GROUP BY _source_sha256;
