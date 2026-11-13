# Databricks notebook source
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
bronze_rows = [
    ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    ("ORD-000002", "2026-10-01 10:00:00", "S001", "P002", "abc", "7.50", "2026-10-01 10:05:00", None),  # malformed qty
    ("ORD-000003", "2026-10-01 11:00:00", "S002", "P003", "2", "20.00", "2026-10-01 11:05:00", None),
]
# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    # Run the pipeline step that transforms bronze to silver
    dbutils.notebook.run("../notebooks/transform_silver", 600, params)

    silver = spark.read.table(f"{schemas[1]}.orders")
    # Verify schema types
    types = dict(silver.dtypes)
    assert types["qty"] == "int", f"qty type mismatch: {types['qty']}"
    # Verify row count matches input rows
    assert silver.count() == 3, f"expected 3 rows, got {silver.count()}"
    # Malformed qty should become NULL
    malformed = silver.filter("order_id = 'ORD-000002'").collect()[0]
    assert malformed["qty"] is None, f"expected NULL qty for malformed row, got {malformed['qty']}"
    # Good rows should retain correct integer qty
    good1 = silver.filter("order_id = 'ORD-000001'").collect()[0]
    assert good1["qty"] == 1, f"qty mismatch for ORD-000001: {good1['qty']}"
    good2 = silver.filter("order_id = 'ORD-000003'").collect()[0]
    assert good2["qty"] == 2, f"qty mismatch for ORD-000003: {good2['qty']}"
    print("test_transform_silver_malformed: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
# COMMAND ----------