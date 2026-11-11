# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Verify against Postgres, then query
# MAGIC
# MAGIC 1. **Reconcile**: prove the Delta tables match the Postgres snapshot exactly (same checks as the
# MAGIC    local `make reconcile`, written in Spark).
# MAGIC 2. **Query**: temporary views over the Delta tables, then analytical SQL, including point-in-time
# MAGIC    lookups on the SCD2 history.
# MAGIC 3. *(Optional)* copy the tables into Unity Catalog so they show in Catalog Explorer and the SQL editor.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakehouse")
dbutils.widgets.text("volume", "files")
catalog, schema, volume = (dbutils.widgets.get(k) for k in ("catalog", "schema", "volume"))
root = f"/Volumes/{catalog}/{schema}/{volume}"

import os, sys
from pathlib import Path
os.environ["LAKE_ROOT"] = f"{root}/lake"
sys.path.insert(0, str(Path(os.getcwd()).resolve().parents[1] / "src"))

# COMMAND ----------

# MAGIC %md ## 1. Reconcile with the Postgres snapshot

# COMMAND ----------

import json
from lakehouse.reconcile_spark import reconcile

result = reconcile(spark, f"{root}/source_snapshot", f"{root}/landing")
print(json.dumps(result["facts"], indent=2))
print("RECONCILED: the lake matches Postgres exactly" if result["ok"] else "FAILED:\n" + "\n".join(result["failures"]))

# COMMAND ----------

# MAGIC %md ## 2. Query

# COMMAND ----------

for view, rel in [("bronze_cdc_events", "bronze/cdc_events"), ("silver_customers_all", "silver/customers"),
                  ("silver_products_all", "silver/products"), ("silver_orders_all", "silver/orders"),
                  ("dim_customers_scd2", "silver/dim_customers_scd2"), ("quarantine", "quarantine/cdc_rejects")]:
    spark.read.format("delta").load(f"{root}/lake/{rel}").createOrReplaceTempView(view)
for t in ("customers", "products", "orders"):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {t} AS SELECT * FROM silver_{t}_all WHERE NOT _is_deleted")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rows per layer
# MAGIC SELECT 'bronze' AS layer, count(*) AS row_count FROM bronze_cdc_events
# MAGIC UNION ALL SELECT 'silver customers (live)', count(*) FROM customers
# MAGIC UNION ALL SELECT 'silver orders (live)', count(*) FROM orders
# MAGIC UNION ALL SELECT 'SCD2 versions', count(*) FROM dim_customers_scd2
# MAGIC UNION ALL SELECT 'quarantine', count(*) FROM quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Why SCD2 matters: revenue by the tier each customer had WHEN THEY ORDERED
# MAGIC SELECT h.tier AS tier_at_order, count(*) AS orders, sum(o.quantity * o.unit_price) AS revenue
# MAGIC FROM orders o
# MAGIC JOIN dim_customers_scd2 h
# MAGIC   ON h.customer_id = o.customer_id AND NOT h.is_deleted
# MAGIC  AND h.valid_from <= o.ordered_at AND (h.valid_to IS NULL OR h.valid_to > o.ordered_at)
# MAGIC WHERE o.status <> 'cancelled'
# MAGIC GROUP BY h.tier ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point in time: the customer with the most history, as of the middle of that history
# MAGIC WITH target AS (
# MAGIC   SELECT customer_id, min(valid_from) + (max(valid_from) - min(valid_from)) / 2 AS as_of
# MAGIC   FROM dim_customers_scd2 GROUP BY customer_id ORDER BY count(*) DESC, customer_id LIMIT 1)
# MAGIC SELECT h.customer_id, t.as_of, h.email, h.country, h.tier, h.valid_from, h.valid_to
# MAGIC FROM dim_customers_scd2 h JOIN target t USING (customer_id)
# MAGIC WHERE NOT h.is_deleted AND h.valid_from <= t.as_of AND (h.valid_to IS NULL OR h.valid_to > t.as_of)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Why records were quarantined
# MAGIC SELECT source_table, split_part(err, ':', 1) AS rule, count(*) AS failures, any_value(err) AS example
# MAGIC FROM quarantine LATERAL VIEW explode(errors) e AS err
# MAGIC GROUP BY ALL ORDER BY failures DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. (Optional) Publish to Unity Catalog
# MAGIC
# MAGIC Delta files in a volume cannot be registered as tables, so this copies them into managed
# MAGIC tables. They then appear in Catalog Explorer and the SQL editor. Re-run after each pipeline run.

# COMMAND ----------

for t in ("customers", "products", "orders"):
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{schema}.silver_{t} AS SELECT * FROM {t}")
spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{schema}.dim_customers_scd2 AS SELECT * FROM dim_customers_scd2")
spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{schema}.quarantine AS SELECT * FROM quarantine")
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
