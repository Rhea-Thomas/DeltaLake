# Running the pipeline on Databricks

Status: **deployable definition, not yet run on a workspace.** Everything
below is prepared, but no Databricks deployment has been tested, so no
Databricks numbers are claimed anywhere in this repo.

## What carries over unchanged

* All transformation logic (`cdc.py`, `quality.py`, `silver.py`, `scd2.py`,
  `jobs/pipeline.py`) is plain PySpark DataFrame and Delta API code.
* `get_spark()` detects Databricks Runtime (`DATABRICKS_RUNTIME_VERSION`) and
  reuses the cluster session. DBR already includes Delta Lake and the Kafka
  source, so no Maven packages are needed.
* Every location comes from environment variables (`LAKE_ROOT`,
  `KAFKA_BOOTSTRAP`, `KAFKA_OPTIONS_JSON`, triggers). The bundle sets them on
  the job cluster.
* Exactly-once still comes from the checkpoints under `LAKE_ROOT/_checkpoints`
  plus the idempotent MERGEs and quarantine writes.

## What you have to provide

1. **Kafka reachable from the workspace.** The local Docker broker is not.
   Options are Confluent Cloud or Amazon MSK, with Debezium running against a
   Postgres the workspace can reach. Put the SASL/SSL options in
   `kafka_options_json`, and reference secrets with
   `{{secrets/<scope>/<key>}}` rather than inlining them.
2. **Storage.** `lake_root` should be a cloud path covered by a Unity Catalog
   external location, e.g. `s3://bucket/lakehouse`. The pipeline writes
   path-based Delta tables. You can register them afterwards with
   `CREATE TABLE ... LOCATION '<lake_root>/silver/customers'`.
3. **Cluster.** A dedicated (single-user) job cluster on DBR 15.4 LTS, which
   is Spark 3.5. The SCD2 step uses `localCheckpoint()`, which shared clusters
   and serverless do not support.

## Deploy

```bash
brew tap databricks/tap && brew install databricks   # the new CLI; the pip "databricks-cli" package has no bundle support
databricks auth login --host https://<workspace>.cloud.databricks.com
databricks bundle validate -t dev
databricks bundle deploy -t dev \
  --var kafka_bootstrap=<host:9092> \
  --var lake_root=s3://<bucket>/lakehouse
databricks bundle run -t dev lakehouse_pipeline
```

## Differences from the local run

* The job is **continuous**: Databricks restarts the stream on failure, so the
  in-process supervisor is disabled (`PIPELINE_SUPERVISE=0`).
* `reports/` latency metrics are written to `LAKE_ROOT/_metrics/*.jsonl`
  only when that path is a local or FUSE path. On `s3://` they are skipped;
  use the Spark streaming query metrics or a Delta metrics table instead.
* Natural next steps on Databricks: liquid clustering on the primary keys,
  predictive optimization or scheduled `OPTIMIZE`, and Unity Catalog tables
  instead of paths.
