# Databricks notebook source
# MAGIC %md
# MAGIC # Test: transform_silver unit_price column resolution
# MAGIC Seeds bronze rows with a `unit_price` column in an isolated schema, runs `notebooks/transform_silver`,
# MAGIC verifies the silver table reads the column correctly (proving the `unit_prce` typo fix), then drops schemas.

# COMMAND ----------

import uuid
from decimal import Decimal
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType, TimestampType
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
test_env = "test_" + (dbutils.widgets.get("run_id") or uuid.uuid4().hex[:8])
schemas = [f"{catalog}.{test_env}_{layer}" for layer in ("bronze", "silver", "gold")]
params = {"catalog": catalog, "env": test_env, "layout": "schema"}

# Bronze schema matches what ingest_bronze produces: all strings + metadata columns
bronze_columns = [
    "order_id", "order_ts", "store_id", "product_id",
    "qty", "unit_price", "updated_at", "notes",
    "_ingested_at", "_source_file"
]
bronze_rows = [
    ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None, datetime.now(), "file1.csv"),
    ("ORD-000002", "2026-10-01 10:00:00", "S001", "P002", "2", "7.50", "2026-10-01 10:05:00", None, datetime.now(), "file1.csv"),
    ("ORD-000003", "2026-10-01 11:00:00", "S002", "P003", "1", "20.00", "2026-10-01 11:05:00", None, datetime.now(), "file2.csv"),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Seed bronze table with unit_price column (string type, as ingest_bronze writes)
    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    # Run the silver transform - this would fail with UNRESOLVED_COLUMN if the typo existed
    dbutils.notebook.run("../notebooks/transform_silver", 600, params)

    # Verify silver output: unit_price must be decimal(10,2) and values preserved
    silver = spark.read.table(f"{schemas[1]}.orders")
    types = dict(silver.dtypes)
    assert types["unit_price"] == "decimal(10,2)", f"unit_price type mismatch: {types}"
    assert types["qty"] == "int", types
    assert types["order_ts"] == "timestamp", types
    assert [c for c, _ in silver.dtypes] == ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at"]
    assert silver.count() == 3, silver.count()

    # Check values survived the cast correctly
    rows = {r["order_id"]: r for r in silver.collect()}
    assert float(rows["ORD-000001"]["unit_price"]) == 10.00
    assert float(rows["ORD-000002"]["unit_price"]) == 7.50
    assert float(rows["ORD-000003"]["unit_price"]) == 20.00
    assert rows["ORD-000001"]["qty"] == 1
    assert rows["ORD-000002"]["qty"] == 2

    # Re-run is idempotent (upsert)
    dbutils.notebook.run("../notebooks/transform_silver", 600, params)
    assert spark.read.table(f"{schemas[1]}.orders").count() == 3
    
    print("test_transform_silver_unit_price_column: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
