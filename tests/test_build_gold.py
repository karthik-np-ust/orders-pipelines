# Databricks notebook source
# MAGIC %md
# MAGIC # Test: build_gold
# MAGIC Seeds typed silver rows in an isolated schema, runs `notebooks/build_gold`, checks the daily aggregates, then drops the schemas.

# COMMAND ----------

import uuid
from datetime import datetime
from decimal import Decimal

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

silver_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("order_ts", TimestampType()),
        StructField("store_id", StringType()),
        StructField("product_id", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
        StructField("updated_at", TimestampType()),
    ]
)
ts = datetime(2026, 10, 1, 9, 0)
silver_rows = [
    ("ORD-000001", ts, "S001", "P001", 1, Decimal("10.00"), ts),
    ("ORD-000002", ts, "S001", "P002", 2, Decimal("7.50"), ts),
    ("ORD-000003", ts, "S002", "P003", 1, Decimal("20.00"), ts),
    ("ORD-000004", ts, "S002", "P001", 3, Decimal("10.00"), ts),
    ("ORD-000005", ts, "S003", "P004", 1, Decimal("5.25"), ts),
    ("ORD-000006", ts, "S003", "P005", 4, Decimal("2.00"), ts),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.createDataFrame(silver_rows, silver_schema).write.mode("overwrite").saveAsTable(f"{schemas[1]}.orders")

    dbutils.notebook.run("../notebooks/build_gold", 600, params)

    gold = {r["store_id"]: r for r in spark.read.table(f"{schemas[2]}.daily_sales").collect()}
    assert set(gold) == {"S001", "S002", "S003"}, gold
    assert gold["S001"]["units"] == 3, gold["S001"]
    assert gold["S001"]["orders"] == 2, gold["S001"]
    assert float(gold["S001"]["revenue"]) == 25.0, gold["S001"]
    assert float(gold["S002"]["revenue"]) == 50.0, gold["S002"]
    print("test_build_gold: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
