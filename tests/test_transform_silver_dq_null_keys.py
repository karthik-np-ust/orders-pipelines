# Databricks notebook source
# MAGIC %md
# MAGIC # Test: transform_silver DQ assertion for null key columns
# MAGIC Seeds bronze rows with null order_id and store_id, runs `notebooks/transform_silver`,
# MAGIC verifies the DQ assertion raises DQAssertionError with correct counts, then drops schemas.
# MAGIC This proves the fix for the .iull() -> .isNull() typo works.

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

bronze_columns = ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at", "notes"]
# Rows with null key columns to trigger DQAssertionError
bronze_rows = [
    ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    # null order_id
    (None, "2026-10-01 10:00:00", "S002", "P002", "2", "7.50", "2026-10-01 10:05:00", None),
    # null store_id
    ("ORD-000003", "2026-10-01 11:00:00", None, "P003", "1", "20.00", "2026-10-01 11:05:00", None),
    # both null
    (None, "2026-10-01 12:00:00", None, "P004", "3", "10.00", "2026-10-01 12:05:00", None),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    # Run transform_silver - should raise DQAssertionError due to null keys
    error_raised = False
    try:
        dbutils.notebook.run("../notebooks/transform_silver", 600, params)
    except Exception as e:
        error_raised = True
        error_msg = str(e)
        # dbutils.notebook.run wraps failures in Py4JJavaError; check full message for DQAssertionError
        assert "DQAssertionError" in error_msg, f"Expected DQAssertionError in error message, got: {error_msg}"
        # Should report 2 rows with null order_id (row 2 and 4)
        assert "2 row(s) with null order_id" in error_msg, f"Expected 2 null order_id rows in error: {error_msg}"
        # Should report 2 rows with null store_id (row 3 and 4)
        assert "2 row(s) with null store_id" in error_msg, f"Expected 2 null store_id rows in error: {error_msg}"
        print(f"test_transform_silver_dq_null_keys: PASSED - {error_msg}")
    
    assert error_raised, "Expected DQAssertionError to be raised for null key columns"

finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")