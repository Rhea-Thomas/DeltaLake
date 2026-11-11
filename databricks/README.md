# Running the pipeline on Databricks

There are two routes. Neither has been run on a Databricks workspace yet,
so no Databricks results are claimed anywhere in this repo.

## Route A: Databricks Free Edition (no cost), notebooks over uploaded files

Free Edition is serverless-only, allows only the `AvailableNow` streaming
trigger, and restricts outbound internet, so it cannot read a Kafka broker on
your laptop. The CDC capture therefore stays local, and Databricks runs the
same Spark code over the captured events:

```
laptop:      Postgres → Debezium → Kafka → bronze ──make export-databricks──▶ export/landing/*.json.gz
                                                                               export/source_snapshot/*.json.gz
Databricks:  volume/landing ──01_run_pipeline──▶ bronze → quarantine / silver / SCD2  (Delta, in the volume)
             02_verify_and_query: exact reconciliation vs. the snapshot, then SQL
```

| Notebook | Does |
|---|---|
| [`notebooks/00_setup.py`](notebooks/00_setup.py) | creates the schema and the volume (`landing/`, `source_snapshot/`, `lake/`) |
| [`notebooks/01_run_pipeline.py`](notebooks/01_run_pipeline.py) | runs `lakehouse.jobs.pipeline` with `BRONZE_SOURCE=files`, `TRIGGER_MODE=available_now` |
| [`notebooks/02_verify_and_query.py`](notebooks/02_verify_and_query.py) | `lakehouse.reconcile_spark` against the Postgres snapshot, then queries, optionally publishes UC tables |

Serverless-specific settings in the code:
* **`TRIGGER_MODE=available_now`**: process what is waiting, then stop. Schedule
  the notebook as a job; checkpoints make every run continue where the last
  one ended.
* **`BRONZE_SOURCE=files`**: the exported Kafka records yield identical bronze
  rows.
* **Caching instead of checkpoints.** `localCheckpoint()` is replaced by
  `persist()` on Databricks (`LAKEHOUSE_MATERIALIZE`); the unit tests run both.
* **A fallback for `foreachBatch`.** If it is unavailable on serverless,
  `SILVER_MODE=batch` processes all of bronze in one idempotent batch instead.

**Tested locally in exactly this mode.** 539,863 exported events went through
bronze and silver in about 45 s and reconciled exactly with the snapshot, in
both stream and batch mode. An incremental export of 2,278 new events also
reconciled exactly, and a deliberately tampered snapshot was caught. What has
*not* been tested is Databricks' own runtime: serverless `foreachBatch`, the
`/Volumes` file system, and Git folders.

## Route B: a paid workspace with a cloud Kafka, continuous job

[`../databricks.yml`](../databricks.yml) is an Asset Bundle for a classic
dedicated job cluster (DBR 15.4 LTS), reading Kafka directly in a continuous
job. It needs:
* a Kafka the workspace can reach (Confluent Cloud, MSK, ...) with Debezium in
  front of a reachable Postgres;
* `kafka_options_json` for SASL/SSL, referencing secrets rather than inlining
  them;
* a `lake_root` on cloud storage covered by a Unity Catalog external location.

Deploy:

```bash
brew tap databricks/tap && brew install databricks
databricks auth login --host https://<workspace>.cloud.databricks.com
databricks bundle deploy -t dev --var kafka_bootstrap=<host:9092> --var lake_root=s3://<bucket>/lakehouse
databricks bundle run -t dev lakehouse_pipeline
```

A continuous job keeps a cluster running and bills for it the whole time.
