# Databricks notebook source
# MAGIC %md
# MAGIC # Test: partner_feed
# MAGIC Runs `notebooks/partner_feed` against an isolated schema (no flakiness, no fake token) and checks the output, then drops the schemas.
# MAGIC Uses an existing secret scope (default `partner-dev`, created by `setup/00_setup_demo`).

# COMMAND ----------

import uuid

from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("secret_scope", "partner-dev")

catalog = dbutils.widgets.get("catalog")
test_env = "test_" + (dbutils.widgets.get("run_id") or uuid.uuid4().hex[:8])
schemas = [f"{catalog}.{test_env}_{layer}" for layer in ("bronze", "silver", "gold")]
params = {
    "catalog": catalog,
    "env": test_env,
    "layout": "schema",
    "secret_scope": dbutils.widgets.get("secret_scope"),
    "flaky_rate": "0",
    "leak_fake_token": "false",
}

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    dbutils.notebook.run("../notebooks/partner_feed", 600, params)

    feed = spark.read.table(f"{schemas[0]}.partner_feed")
    assert feed.count() == 10, feed.count()
    assert [c for c, _ in feed.dtypes] == ["store_id", "footfall", "loaded_at"], feed.dtypes
    assert feed.filter("footfall < 0 OR footfall > 500").count() == 0
    print("test_partner_feed: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
