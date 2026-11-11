# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Setup: schema and volume
# MAGIC
# MAGIC Creates a Unity Catalog **schema** and a **volume** (a governed folder) that hold:
# MAGIC
# MAGIC | path in the volume | what |
# MAGIC |---|---|
# MAGIC | `landing/` | change-event files exported on your laptop (`make export-databricks`) and uploaded here |
# MAGIC | `source_snapshot/` | the matching Postgres snapshot, used to prove the lake is exact |
# MAGIC | `lake/` | the Delta tables the pipeline writes: bronze, silver, quarantine, SCD2 (plus checkpoints) |
# MAGIC
# MAGIC Run this once. Free Edition's default catalog is `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse")
dbutils.widgets.text("volume", "files")
catalog, schema, volume = (dbutils.widgets.get(k) for k in ("catalog", "schema", "volume"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
root = f"/Volumes/{catalog}/{schema}/{volume}"
for sub in ("landing", "source_snapshot", "lake"):
    dbutils.fs.mkdirs(f"{root}/{sub}")
print("Volume ready:", root)
print("Next: upload export/landing/* to", f"{root}/landing", "and export/source_snapshot/* to", f"{root}/source_snapshot")
