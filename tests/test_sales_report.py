# Databricks notebook source
# MAGIC %md
# MAGIC # Test: sales_report
# MAGIC Seeds `gold.daily_sales` in an isolated schema, runs `notebooks/sales_report`, checks the ranking, then drops the schemas.

# COMMAND ----------

import uuid
from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
test_env = "test_" + (dbutils.widgets.get("run_id") or uuid.uuid4().hex[:8])
schemas = [f"{catalog}.{test_env}_{layer}" for layer in ("bronze", "silver", "gold")]
params = {"catalog": catalog, "env": test_env, "layout": "schema", "sleep_seconds": "0"}

daily_sales_rows = [
    (date(2026, 10, 1), "S001", Decimal("25.00"), 3, 2),
    (date(2026, 10, 1), "S002", Decimal("50.00"), 4, 2),
    (date(2026, 10, 1), "S003", Decimal("13.25"), 5, 2),
]

# COMMAND ----------

try:
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    (
        spark.createDataFrame(daily_sales_rows, "order_date date, store_id string, revenue decimal(20,2), units bigint, orders bigint")
        .write.mode("overwrite").saveAsTable(f"{schemas[2]}.daily_sales")
    )

    dbutils.notebook.run("../notebooks/sales_report", 600, params)

    report = spark.read.table(f"{schemas[2]}.sales_report").orderBy("revenue", ascending=False).collect()
    assert [r["store_id"] for r in report] == ["S002", "S001", "S003"], report
    assert float(report[0]["revenue"]) == 50.0, report[0]
    print("test_sales_report: PASSED")
finally:
    for schema in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
