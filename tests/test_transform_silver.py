# Databricks notebook source
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

# Bronze schema matches ingest_bronze output: all strings plus metadata columns
bronze_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("order_ts", StringType()),
        StructField("store_id", StringType()),
        StructField("product_id", StringType()),
        StructField("qty", StringType()),
        StructField("unit_price", StringType()),  # This column must be named unit_price (not unt_price)
        StructField("updated_at", StringType()),
        StructField("notes", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_source_file", StringType()),
    ]
)

ts = datetime(2026, 10, 1, 9, 0)
bronze_rows = [
    ("ORD-000001", "2026-10-01 09:00:00", "S001", "P001", "1", "10.00", "2026-10-01 09:05:00", "", ts, "orders.csv"),
    ("ORD-000002", "2026-10-01 10:00:00", "S001", "P002", "2", "7.50", "2026-10-01 10:05:00", "", ts, "orders.csv"),
    ("ORD-000003", "2026-10-01 11:00:00", "S002", "P003", "1", "20.00", "2026-10-01 11:05:00", "", ts, "orders.csv"),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.createDataFrame(bronze_rows, bronze_schema).write.mode("overwrite").saveAsTable(f"{schemas[0]}.orders")

    dbutils.notebook.run("../notebooks/transform_silver", 600, params)

    silver = spark.read.table(f"{schemas[1]}.orders").orderBy("order_id").collect()
    assert len(silver) == 3, f"Expected 3 rows, got {len(silver)}"
    
    # Verify column types and values
    row1 = silver[0]
    assert row1["order_id"] == "ORD-000001"
    assert row1["store_id"] == "S001"
    assert row1["product_id"] == "P001"
    assert row1["qty"] == 1
    assert row1["unit_price"] == Decimal("10.00"), f"unit_price type: {type(row1['unit_price'])}, value: {row1['unit_price']}"
    assert row1["order_ts"] == ts
    assert row1["updated_at"] == ts
    
    row2 = silver[1]
    assert row2["unit_price"] == Decimal("7.50")
    
    row3 = silver[2]
    assert row3["unit_price"] == Decimal("20.00")
    
    # Verify schema matches silver_schema from transform_silver
    silver_types = dict(spark.read.table(f"{schemas[1]}.orders").dtypes)
    assert silver_types["unit_price"] == "decimal(10,2)", f"unit_price type: {silver_types['unit_price']}"
    assert silver_types["qty"] == "int"
    assert silver_types["order_ts"] == "timestamp"
    assert silver_types["updated_at"] == "timestamp"
    
    print("test_transform_silver: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
