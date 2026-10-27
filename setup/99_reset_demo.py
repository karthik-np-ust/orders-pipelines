# Databricks notebook source
# MAGIC %md
# MAGIC # Reset the demo between fault scenarios
# MAGIC For each environment: clean data back in the landing volume, `partner-<env>/api_key` restored,
# MAGIC silver/gold tables dropped so the next run starts fresh.
# MAGIC
# MAGIC Git history (release faults) is reset from the agent repo: `python scripts/inject_fault.py --help`.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()

DUMMY_KEY = "demo-not-a-real-key"

# COMMAND ----------

dbutils.widgets.text("envs", "dev,qa,prod")
dbutils.widgets.dropdown("layout", "catalog", ["catalog", "schema"])

envs = [e.strip() for e in dbutils.widgets.get("envs").split(",") if e.strip()]
layout = dbutils.widgets.get("layout")

# COMMAND ----------

for env in envs:
    dbutils.notebook.run("./10_seed_scenario", 600, {"env": env, "layout": layout, "scenario": "clean"})
    w.secrets.put_secret(scope=f"partner-{env}", key="api_key", string_value=DUMMY_KEY)
    prefix = f"workspace.{env}_" if layout == "schema" else f"{env}."
    for table in (f"{prefix}silver.orders", f"{prefix}gold.daily_sales", f"{prefix}gold.sales_report"):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"[{env}] data, secret and tables reset")

print("reset done")
