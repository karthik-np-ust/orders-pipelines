# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: type, check and upsert orders
# MAGIC Types the bronze strings, fails on null keys (`DQAssertionError`) and MERGEs into `silver.orders` by `order_id`.
# MAGIC
# MAGIC Widgets: `catalog`, `env`, `layout` (see `ingest_bronze`).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType, TimestampType
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("env", "dev")
dbutils.widgets.dropdown("layout", "catalog", ["catalog", "schema"])

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
layout = dbutils.widgets.get("layout")

bronze_orders = (f"{catalog}.{env}_bronze" if layout == "schema" else f"{catalog}.bronze") + ".orders"
silver_orders = (f"{catalog}.{env}_silver" if layout == "schema" else f"{catalog}.silver") + ".orders"

# Column contract for silver.orders.
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

# COMMAND ----------

# Data quality: keys must be present. Never remove or weaken this check.
bronze = spark.read.table(bronze_orders)
for key_column in ["order_id", "store_id"]:
    null_keys = bronze.filter(F.col(key_column).isNull()).count()
    if null_keys:
        raise Exception(f"DQAssertionError: {null_keys} row(s) with null {key_column}")

# COMMAND ----------

orders = bronze.select(
    F.col("order_id"),
    F.to_timestamp("order_ts").alias("order_ts"),
    F.col("store_id"),
    F.col("product_id"),
    F.expr("try_cast(qty AS INT)").alias("qty"),
    F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
    F.to_timestamp("updated_at").alias("updated_at"),
)

# COMMAND ----------

DeltaTable.createIfNotExists(spark).tableName(silver_orders).addColumns(silver_schema).execute()
(
    DeltaTable.forName(spark, silver_orders).alias("t")
    .merge(orders.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
print(f"silver: merged into {silver_orders}")