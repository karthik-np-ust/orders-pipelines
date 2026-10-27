# Databricks notebook source
# MAGIC %md
# MAGIC # One-time demo setup
# MAGIC For each environment (dev, qa, prod) this creates:
# MAGIC - catalog `<env>` with schemas `bronze`, `silver`, `gold` (or schemas `workspace.<env>_*` with `layout = schema`)
# MAGIC - the volume `bronze.landing`
# MAGIC - secret scope `partner-<env>` with a dummy `api_key` (read by `partner_feed`)
# MAGIC - clean synthetic orders in the landing volume (via `10_seed_scenario`)
# MAGIC
# MAGIC Safe to re-run. If `CREATE CATALOG` is not allowed in your workspace, use `layout = schema`
# MAGIC and set the jobs' `catalog` parameter to `workspace` and `layout` to `schema`.

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

existing_scopes = {s.name for s in w.secrets.list_scopes()}

for env in envs:
    if layout == "catalog":
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {env}")
        schemas = [f"{env}.{layer}" for layer in ("bronze", "silver", "gold")]
    else:
        schemas = [f"workspace.{env}_{layer}" for layer in ("bronze", "silver", "gold")]
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {schemas[0]}.landing")

    scope = f"partner-{env}"
    if scope not in existing_scopes:
        w.secrets.create_scope(scope=scope)
    w.secrets.put_secret(scope=scope, key="api_key", string_value=DUMMY_KEY)

    dbutils.notebook.run("./10_seed_scenario", 600, {"env": env, "layout": layout, "scenario": "clean"})
    print(f"[{env}] schemas {', '.join(schemas)}, volume landing, secret scope {scope}, clean data")

print("setup done")
