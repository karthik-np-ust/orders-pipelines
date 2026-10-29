# Databricks notebook source
# MAGIC %md
# Test: transform_silver fix
# Seeds bronze rows in an isolated schema, runs notebooks/transform_silver, checks the silver contract,
# then drops the schemas. This test validates that the typo fix (unit_prce -> unit_price) resolves
# the UNRESOLVED_COLUMN error and the pipeline succeeds.

import uuid
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
test_env = "test_" + (dbutils.widgets.get("run_id") or uuid.uuid4().hex[:8])
schemas = [f"{catalog}.{test_env}_{layer}" for layer in ("bronze", "silver", "gold")]
params = {"catalog": catalog, "env": test_env, "layout": "schema"}

bronze_columns = ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at", "notes"]
bronze_rows = [
    ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    ("ORD-000002", "2026-10-01 10:00:00", "S001", "P002", "2", "7.50", "2026-10-01 10:05:00", None),
    ("ORD-000003", "2026-10-01 11:00:00", "S002", "P003", "1", "20.00", "2026-10-01 11:05:00", None),
    ("ORD-000004", "2026-10-01 12:00:00", "S002", "P001", "3", "10.00", "2026-10-01 12:05:00", None),
    ("ORD-000005", "2026-10-01 13:00:00", "S003", "P004", "1", "5.25", "2026-10-01 13:05:00", None),
    ("ORD-000006", "2026-10-01 14:00:00", "S003", "P005", "4", "2.00", "2026-10-01 14:05:00", None),
]

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    dbutils.notebook.run("../notebooks/transform_silver", 600, params)

    silver = spark.read.table(f"{schemas[1]}.orders")
    types = dict(silver.dtypes)
    assert types["qty"] == "int", types
    assert types["unit_price"] == "decimal(10,2)", types
    assert types["order_ts"] == "timestamp", types
    assert [c for c, _ in silver.dtypes] == ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at"]
    assert silver.count() == 6, silver.count()
    print("test_transform_silver_fix: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")