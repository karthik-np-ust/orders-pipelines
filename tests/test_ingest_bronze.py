# Databricks notebook source
# MAGIC %md
# MAGIC # Test: ingest_bronze
# MAGIC Writes a landing CSV into a test volume, runs `notebooks/ingest_bronze`, checks bronze keeps raw strings, then drops the schemas.

# COMMAND ----------

import uuid

from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
test_env = "test_" + (dbutils.widgets.get("run_id") or uuid.uuid4().hex[:8])
schemas = [f"{catalog}.{test_env}_{layer}" for layer in ("bronze", "silver", "gold")]
params = {"catalog": catalog, "env": test_env, "layout": "schema"}

landing_csv = """order_id,order_ts,store_id,product_id,qty,unit_price,updated_at,notes
ORD-000001,2026-10-01 09:00:00,S001,P001,1,10.00,2026-10-01 09:05:00,
ORD-000002,2026-10-01 10:00:00,S001,P002,2,7.50,2026-10-01 10:05:00,
ORD-000003,2026-10-01 11:00:00,S002,P003,1,20.00,2026-10-01 11:05:00,
"""

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {schemas[0]}.landing")
    landing_path = "/Volumes/" + schemas[0].replace(".", "/") + "/landing/orders"
    dbutils.fs.put(f"{landing_path}/orders.csv", landing_csv, overwrite=True)

    dbutils.notebook.run("../notebooks/ingest_bronze", 600, params)

    bronze = spark.read.table(f"{schemas[0]}.orders")
    assert bronze.count() == 3, bronze.count()
    types = dict(bronze.dtypes)
    assert types["qty"] == "string" and types["unit_price"] == "string", types
    assert "_ingested_at" in types and "_source_file" in types, types
    print("test_ingest_bronze: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
