# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest landing CSVs
# MAGIC Loads every CSV in the landing volume as raw strings (no schema inference); typing happens in silver.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `catalog` | Unity Catalog catalog |
# MAGIC | `env` | environment name (`dev`, `qa`, `prod`, or `test_<id>` from the test notebooks) |
# MAGIC | `layout` | `catalog`: `<catalog>.bronze.x` &nbsp; `schema`: `<catalog>.<env>_bronze.x` |

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

bronze_schema = f"{catalog}.{env}_bronze" if layout == "schema" else f"{catalog}.bronze"
bronze_orders = f"{bronze_schema}.orders"
landing_path = "/Volumes/" + bronze_schema.replace(".", "/") + "/landing/orders"

# COMMAND ----------

orders_raw = (
    spark.read.option("header", True).option("inferSchema", False).csv(landing_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

orders_raw.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(bronze_orders)
print(f"bronze: wrote {spark.read.table(bronze_orders).count()} rows to {bronze_orders}")
