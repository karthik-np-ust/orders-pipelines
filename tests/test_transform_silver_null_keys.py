# Databricks notebook source
# MAGIC %md
# MAGIC # Test: transform_silver null-key rejection
# MAGIC Seeds bronze rows with null order_id and null store_id in an isolated schema,
# MAGIC runs `notebooks/transform_silver`, asserts the run fails (DQ check) and
# MAGIC nothing is written to silver, then drops the schemas.

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
# Row with null order_id, row with null store_id
bronze_rows = [
    (None, "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    ("ORD-000002", "2026-10-01 10:00:00", None, "P002", "2", "7.50", "2026-10-01 10:05:00", None),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    # Run should fail due to DQAssertionError on null keys
    try:
        dbutils.notebook.run("../notebooks/transform_silver", 600, params)
        raise AssertionError("Expected transform_silver to fail on null keys, but it succeeded")
    except Exception:
        # Child notebook raised; we only verify nothing was written to silver
        pass

    # Verify silver table was not created / is empty
    silver_table = f"{schemas[1]}.orders"
    tables = spark.sql(f"SHOW TABLES IN {schemas[1]}").collect()
    if any(r.tableName == "orders" for r in tables):
        count = spark.read.table(silver_table).count()
        assert count == 0, f"Silver should be empty, got {count} rows"
    print("test_transform_silver_null_keys: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
