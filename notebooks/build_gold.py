# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: daily sales per store
# MAGIC Aggregates `silver.orders` into `gold.daily_sales` (revenue, units, orders, average unit price).
# MAGIC
# MAGIC Widgets: `catalog`, `env`, `layout` (see `ingest_bronze`).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("env", "dev")
dbutils.widgets.dropdown("layout", "catalog", ["catalog", "schema"])

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
layout = dbutils.widgets.get("layout")

silver_orders = (f"{catalog}.{env}_silver" if layout == "schema" else f"{catalog}.silver") + ".orders"
gold_daily_sales = (f"{catalog}.{env}_gold" if layout == "schema" else f"{catalog}.gold") + ".daily_sales"

# COMMAND ----------

daily_sales = (
    spark.read.table(silver_orders)
    .groupBy(F.to_date("order_ts").alias("order_date"), "store_id")
    .agg(
        F.sum(F.col("qty") * F.col("unit_price")).alias("revenue"),
        F.sum("qty").alias("units"),
        F.count("*").alias("orders"),
    )
    .withColumn("avg_unit_price", F.col("revenue") / F.col("units"))
)

# COMMAND ----------

daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(gold_daily_sales)
print(f"gold: wrote {spark.read.table(gold_daily_sales).count()} rows to {gold_daily_sales}")
