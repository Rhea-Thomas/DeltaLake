# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Run the pipeline (bronze → quarantine / silver / SCD2)
# MAGIC
# MAGIC The **same code** as the local pipeline (`src/lakehouse/`), configured for serverless compute:
# MAGIC
# MAGIC * `BRONZE_SOURCE=files`: read the uploaded Kafka records from `landing/` instead of a Kafka broker
# MAGIC   (Free Edition cannot reach a broker on your laptop).
# MAGIC * `TRIGGER_MODE=available_now`: process everything waiting, then stop. Serverless allows no
# MAGIC   always-on trigger, so schedule this notebook as a job instead. Each run resumes from the
# MAGIC   checkpoints in `lake/_checkpoints`, so nothing is processed twice.
# MAGIC
# MAGIC If the silver step fails with an error about `foreachBatch` or a module that cannot be imported,
# MAGIC set the `silver_mode` widget to `batch` and run again (see the tutorial's troubleshooting section).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse")
dbutils.widgets.text("volume", "files")
dbutils.widgets.dropdown("silver_mode", "stream", ["stream", "batch"])
catalog, schema, volume, silver_mode = (
    dbutils.widgets.get(k) for k in ("catalog", "schema", "volume", "silver_mode"))
root = f"/Volumes/{catalog}/{schema}/{volume}"

# COMMAND ----------

import os
import sys
from pathlib import Path

# Configuration is read from the environment when the package is first imported,
# so set it before importing anything from `lakehouse`.
os.environ.update({
    "LAKE_ROOT": f"{root}/lake",
    "BRONZE_SOURCE": "files",
    "BRONZE_SOURCE_PATH": f"{root}/landing",
    "TRIGGER_MODE": "available_now",
    "SILVER_MODE": silver_mode,
})

# This notebook lives in <repo>/databricks/notebooks; the package is in <repo>/src.
repo_root = Path(os.getcwd()).resolve().parents[1]
sys.path.insert(0, str(repo_root / "src"))

import lakehouse  # noqa: E402

# On serverless compute the per-batch function may run in a separate Python
# worker that does not have this repo on its path. Shipping the package's code
# *with* the function (pickle by value) avoids "No module named 'lakehouse'".
try:
    from pyspark import cloudpickle
    cloudpickle.register_pickle_by_value(lakehouse)
except Exception as exc:  # noqa: BLE001
    print("note: could not register pickle-by-value:", exc)

print("repo:", repo_root, "| lake:", os.environ["LAKE_ROOT"], "| silver_mode:", silver_mode)

# COMMAND ----------

from lakehouse.jobs import pipeline  # noqa: E402

pipeline.main()

# COMMAND ----------

# Quick look: rows per layer
for name, rel in [("bronze", "bronze/cdc_events"), ("silver customers", "silver/customers"),
                  ("silver products", "silver/products"), ("silver orders", "silver/orders"),
                  ("SCD2 customers", "silver/dim_customers_scd2"), ("quarantine", "quarantine/cdc_rejects")]:
    print(f"{name:18s}", spark.read.format("delta").load(f"{root}/lake/{rel}").count())
