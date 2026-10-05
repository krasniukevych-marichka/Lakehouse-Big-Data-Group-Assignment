# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — Bronze layer
# MAGIC
# MAGIC Store the source data *as is*, the only thing we add is technical metadata:
# MAGIC
# MAGIC | column | meaning |
# MAGIC |---|---|
# MAGIC | `_ingested_at` | when this row was loaded into bronze |
# MAGIC | `_source` | fully qualified name of the source table |
# MAGIC | `_batch_id` | id of the load run (same for all tables in one run) |
# MAGIC
# MAGIC Source: `samples.tpch` (8 tables). Target: `<catalog>.<schema>.bronze_<table>`.

# COMMAND ----------

import re
import uuid

from pyspark.sql import functions as F

_user = spark.sql("SELECT current_user()").first()[0]
_default_schema = "lakehouse_" + re.sub(r"\W+", "_", _user.split("@")[0]).lower()

dbutils.widgets.text("target_catalog", "workspace", "Target catalog")
dbutils.widgets.text("target_schema", _default_schema, "Target schema")
dbutils.widgets.text("source_catalog", "samples", "Source catalog")
dbutils.widgets.text("source_schema", "tpch", "Source schema")

TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
SOURCE_CATALOG = dbutils.widgets.get("source_catalog")
SOURCE_SCHEMA = dbutils.widgets.get("source_schema")

SOURCE_TABLES = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem",
]
BATCH_ID = str(uuid.uuid4())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Source : {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print(f"Target : {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Batch  : {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load

# COMMAND ----------

def bronze_name(table: str) -> str:
    return f"{TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_{table}"


def load_bronze(table: str) -> dict:
    """Copy one source table into bronze unchanged, adding only metadata columns."""
    source = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}"
    target = bronze_name(table)

    df = (
        spark.table(source)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    spark.sql(
        f"COMMENT ON TABLE {target} IS "
        f"'Bronze: raw copy of {source}. No transformations, metadata columns only.'"
    )

    return {
        "table": table,
        "source_rows": spark.table(source).count(),
        "bronze_rows": spark.table(target).count(),
    }


results = [load_bronze(t) for t in SOURCE_TABLES]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: bronze must contain exactly what the source had

# COMMAND ----------

report = spark.createDataFrame(results).withColumn(
    "matches", F.col("source_rows") == F.col("bronze_rows")
).select("table", "source_rows", "bronze_rows", "matches")

display(report)

mismatched = [r["table"] for r in report.filter(~F.col("matches")).collect()]
assert not mismatched, f"Row count mismatch between source and bronze: {mismatched}"
print("OK: all bronze tables match the source row counts.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT _source, _batch_id, count(*) AS rows_received, max(_ingested_at) AS last_loaded
        FROM {bronze_name('supplier')} GROUP BY _source, _batch_id
        UNION ALL
        SELECT _source, _batch_id, count(*), max(_ingested_at)
        FROM {bronze_name('partsupp')} GROUP BY _source, _batch_id
        UNION ALL
        SELECT _source, _batch_id, count(*), max(_ingested_at)
        FROM {bronze_name('lineitem')} GROUP BY _source, _batch_id
    """)
)