# Databricks notebook source

# COMMAND ----------
import uuid
from datetime import datetime
from decimal import Decimal

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

# Bronze data includes a malformed qty value "abc"
bronze_columns = ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at", "notes"]
bronze_rows = [
    ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", None),
    ("ORD-000002", "2026-10-01 10:00:00", "S001", "P002", "2", "7.50", "2026-10-01 10:05:00", None),
    ("ORD-000003", "2026-10-01 11:00:00", "S002", "P003", "1", "20.00", "2026-10-01 11:05:00", None),
    ("ORD-000004", "2026-10-01 12:00:00", "S002", "P001", "3", "10.00", "2026-10-01 12:05:00", None),
    ("ORD-000005", "2026-10-01 13:00:00", "S003", "P004", "1", "5.25", "2026-10-01 13:05:00", None),
    ("ORD-000006", "2026-10-01 14:00:00", "S003", "P005", "4", "2.00", "2026-10-01 14:05:00", None),
    # Malformed qty – should become NULL in silver
    ("ORD-000007", "2026-10-01 15:00:00", "S004", "P006", "abc", "12.00", "2026-10-01 15:05:00", None),
]

# COMMAND ----------

try:
    # Create isolated schemas
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    bronze_schema = StructType([StructField(c, StringType()) for c in bronze_columns])
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    # Run the transform_silver notebook – should succeed despite malformed qty
    dbutils.notebook.run("../notebooks/transform_silver", 600, params)

    silver_df = spark.read.table(f"{schemas[1]}.orders")
    # Basic contract checks
    types = dict(silver_df.dtypes)
    assert types["qty"] == "int", f"qty type mismatch: {types['qty']}"
    assert types["unit_price"] == "decimal(10,2)", f"unit_price type mismatch: {types['unit_price']}"
    assert types["order_ts"] == "timestamp", f"order_ts type mismatch: {types['order_ts']}"
    assert [c for c, _ in silver_df.dtypes] == ["order_id", "order_ts", "store_id", "product_id", "qty", "unit_price", "updated_at"], "unexpected columns"
    assert silver_df.count() == len(bronze_rows), f"row count mismatch: {silver_df.count()} vs {len(bronze_rows)}"

    # The malformed row should have qty = NULL
    null_qty_count = silver_df.filter(silver_df["qty"].isNull()).count()
    assert null_qty_count == 1, f"expected 1 NULL qty, got {null_qty_count}"

    # All other rows should retain their integer qty values
    good_rows = silver_df.filter(silver_df["qty"].isNotNull()).collect()
    expected_qty = {"ORD-000001": 1, "ORD-000002": 2, "ORD-000003": 1, "ORD-000004": 3, "ORD-000005": 1, "ORD-000006": 4}
    for row in good_rows:
        assert row["order_id"] in expected_qty, f"unexpected order_id {row['order_id']}"
        assert row["qty"] == expected_qty[row["order_id"]], f"qty mismatch for {row['order_id']}"

    print("test_transform_silver_malformed: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
