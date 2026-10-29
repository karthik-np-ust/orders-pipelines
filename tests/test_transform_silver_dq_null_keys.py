# Databricks notebook source
# MAGIC %md
# MAGIC # Test: transform_silver DQ assertion for null keys
# MAGIC Seeds bronze rows with null order_id and store_id in an isolated schema,
# MAGIC runs `notebooks/transform_silver`, verifies it raises DQAssertionError
# MAGIC (not TypeError from the old .iull() typo), then drops the schemas.

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
# Row with null order_id
bronze_rows_null_order_id = [
    (None, "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
]
# Row with null store_id
bronze_rows_null_store_id = [
    ("ORD-000001", "2026-10-01 09:00:00", None, "P001", "1", "10.00", "2026-10-01 09:05:00", None),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])

    def run_and_check_dq_error(rows, expected_key):
        spark.createDataFrame(rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")
        try:
            dbutils.notebook.run("../notebooks/transform_silver", 600, params)
            raise AssertionError(f"Expected DQAssertionError for null {expected_key}, but notebook succeeded")
        except Exception as e:
            # The full exception string contains the nested DQAssertionError message
            error_msg = str(e)
            assert "DQAssertionError" in error_msg, f"Expected DQAssertionError, got: {error_msg}"
            assert expected_key in error_msg, f"Expected {expected_key} in error message, got: {error_msg}"
            print(f"Test passed: null {expected_key} raises DQAssertionError")

    # Test 1: null order_id should raise DQAssertionError
    run_and_check_dq_error(bronze_rows_null_order_id, "order_id")

    # Test 2: null store_id should raise DQAssertionError
    run_and_check_dq_error(bronze_rows_null_store_id, "store_id")

    print("test_transform_silver_dq_null_keys: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
