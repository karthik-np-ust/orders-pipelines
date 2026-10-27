# Databricks notebook source
# MAGIC %md
# MAGIC # Partner feed: simulated external API pull
# MAGIC Writes store footfall from a (simulated) partner API to `bronze.partner_feed`.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `catalog`, `env`, `layout` | see `ingest_bronze` |
# MAGIC | `secret_scope` | defaults to `partner-<env>`; tests point it at an existing scope |
# MAGIC | `flaky_rate` | `0.7` fails like a flaky API (scenario 2, transient) |
# MAGIC | `leak_fake_token` | `true` prints a FAKE token-like string, then fails (scenario 13, redaction) |
# MAGIC
# MAGIC A missing secret `partner-<env>/api_key` fails with "Secret does not exist" (scenario 11, infra).

# COMMAND ----------

import random
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# Obviously fake, but shaped like a Databricks token so the agent's redaction must catch it.
FAKE_TOKEN = "dapiFAKE00000000000000000000000000"

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("env", "dev")
dbutils.widgets.dropdown("layout", "catalog", ["catalog", "schema"])
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("flaky_rate", "0")
dbutils.widgets.text("leak_fake_token", "false")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")
layout = dbutils.widgets.get("layout")
secret_scope = dbutils.widgets.get("secret_scope") or f"partner-{env}"
flaky_rate = float(dbutils.widgets.get("flaky_rate") or "0")
leak_fake_token = dbutils.widgets.get("leak_fake_token").lower() == "true"

partner_feed = (f"{catalog}.{env}_bronze" if layout == "schema" else f"{catalog}.bronze") + ".partner_feed"

partner_feed_schema = StructType(
    [
        StructField("store_id", StringType()),
        StructField("footfall", IntegerType()),
        StructField("loaded_at", StringType()),
    ]
)

# COMMAND ----------

# Proves the secret exists; the value is never printed.
api_key = dbutils.secrets.get(scope=secret_scope, key="api_key")

if leak_fake_token:
    print(f"DEBUG calling partner API with Authorization: Bearer {FAKE_TOKEN}")
    raise RuntimeError(f"Partner API rejected credentials for token {FAKE_TOKEN}")

if random.random() < flaky_rate:
    raise ConnectionError("Connection reset by peer while calling partner API (HTTP 503)")

# COMMAND ----------

loaded_at = datetime.now(timezone.utc).isoformat()
footfall = spark.createDataFrame(
    [(f"S{n:03d}", random.randint(0, 500), loaded_at) for n in range(1, 11)],
    schema=partner_feed_schema,
)
footfall.write.format("delta").mode("overwrite").saveAsTable(partner_feed)
print(f"partner_feed: wrote {footfall.count()} rows to {partner_feed}")
