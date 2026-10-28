# Databricks notebook source
# MAGIC %md
# MAGIC # Test: transform_silver DQ assertion
# MAGIC Verifies the DQ null-check in `notebooks/transform_silver` correctly catches null `order_id` and `store_id` keys (regression for the `.iull()` typo that raised `TypeError: 'Column' object is not callable`), and that valid rows still merge into silver unchanged.

# COMMAND ----------

import uuid

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

# COMMAND ----------

bronze_columns = ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at", "notes"]
bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # --- Happy path: valid rows merge into silver unchanged ---
    valid_rows = [
        ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
        ("ORD-000002", "2026-10-01 10:00:00", "S001", "P002", "2", "7.50", "2026-10-01 10:05:00", None),
        ("ORD-000003", "2026-10-01 11:00:00", "S002", "P003", "1", "20.00", "2026-10-01 11:05:00", None),
    ]
    spark.createDataFrame(valid_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    dbutils.notebook.run("../notebooks/transform_silver", 600, params)

    silver = spark.read.table(f"{schemas[1]}.orders")
    assert silver.count() == 3, silver.count()
    assert sorted(r["order_id"] for r in silver.select("order_id").collect()) == ["ORD-000001", "ORD-000002", "ORD-000003"]
    print("happy path: PASSED")

    # --- DQ regression: null order_id must raise DQAssertionError (not TypeError) ---
    null_order_id_rows = [
        (None, "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    ]
    spark.createDataFrame(null_order_id_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")
    try:
        dbutils.notebook.run("../notebooks/transform_silver", 600, params)
        raise AssertionError("Expected DQAssertionError for null order_id")
    except Exception as e:
        assert "DQAssertionError: 1 row(s) with null order_id" in str(e), f"Unexpected error: {e}"
    print("DQ null order_id: PASSED")

    # --- DQ regression: null store_id must raise DQAssertionError (not TypeError) ---
    null_store_id_rows = [
        ("ORD-000008", "2026-10-01 09:00:00", None, "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    ]
    spark.createDataFrame(null_store_id_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")
    try:
        dbutils.notebook.run("../notebooks/transform_silver", 600, params)
        raise AssertionError("Expected DQAssertionError for null store_id")
    except Exception as e:
        assert "DQAssertionError: 1 row(s) with null store_id" in str(e), f"Unexpected error: {e}"
    print("DQ null store_id: PASSED")

    print("test_transform_silver_dq: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
