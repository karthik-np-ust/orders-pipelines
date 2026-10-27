# Databricks notebook source
# MAGIC %md
# MAGIC # Sales report: top stores by revenue
# MAGIC Reads `gold.daily_sales`, writes `gold.sales_report` ordered by revenue.
# MAGIC
# MAGIC Widgets: `catalog`, `env`, `layout` (see `ingest_bronze`), plus `sleep_seconds`
# MAGIC (fault scenario 1: a value above the runtime threshold makes the run "slow").

# COMMAND ----------

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("env", "dev")
dbutils.widgets.dropdown("layout", "catalog", ["catalog", "schema"])
dbutils.widgets.text("sleep_seconds", "0")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
layout = dbutils.widgets.get("layout")
sleep_seconds = int(dbutils.widgets.get("sleep_seconds") or "0")

gold_schema = f"{catalog}.{env}_gold" if layout == "schema" else f"{catalog}.gold"
gold_daily_sales = f"{gold_schema}.daily_sales"
gold_sales_report = f"{gold_schema}.sales_report"

# COMMAND ----------

report = (
    spark.read.table(gold_daily_sales)
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("revenue"), F.sum("orders").alias("orders"))
    .orderBy(F.desc("revenue"))
)
report.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(gold_sales_report)

# COMMAND ----------

if sleep_seconds > 0:
    print(f"sales_report: simulating slow work for {sleep_seconds}s")
    time.sleep(sleep_seconds)
print(f"sales_report: wrote {spark.read.table(gold_sales_report).count()} rows to {gold_sales_report}")
