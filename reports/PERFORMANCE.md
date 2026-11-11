# Query performance: before vs after tuning

Same SQL, same data, pipeline stopped; median of 5 runs after 1 warm-up. Before: 2026-11-11 03:33 UTC; after: 2026-11-11 03:40 UTC.

Tuning applied: bronze re-partitioned by (source_table, event_date); OPTIMIZE compaction on all Spark-written tables; Z-ORDER BY customer_id on silver orders/customers and SCD2, product_id on products; gold fct_orders partitioned by order_date (dbt); VACUUM (7-day retention)

## Active data files per table

| table | before | after |
|---|---|---|
| bronze | 2846 | 3 |
| silver.orders | 4 | 1 |
| scd2 | 4 | 1 |
| quarantine | 341 | 1 |
| gold.fct_orders | 1 | 1 |

## Duckdb (ms)

| query | what it does | before | after | speed-up |
|---|---|---|---|---|
| `bronze_scan` | Full scan of bronze: events per table and op | 155 | 10 | **14.8x** |
| `bronze_window` | One table, 30-minute event-time window (partition + file-stats pruning) | 122 | 12 | **10.2x** |
| `silver_orders_agg` | Revenue by status over silver orders | 4 | 6 | **0.7x** |
| `silver_orders_customer` | All orders of one customer (Z-order on customer_id) | 4 | 5 | **0.7x** |
| `scd2_point_in_time` | SCD2 history + as-of lookup for one customer (Z-order on customer_id) | 4 | 5 | **0.7x** |
| `quarantine_reasons` | Quarantine failures per table (many tiny append files before tuning) | 19 | 4 | **4.5x** |
| `gold_star_join` | Star join: revenue by month x tier-at-order x category | 9 | 9 | **1.0x** |
| `gold_one_day` | Fact rows for one order date (partition pruning once partitioned) | 1 | 1 | **1.0x** |

## Trino (ms)

| query | what it does | before | after | speed-up |
|---|---|---|---|---|
| `bronze_scan` | Full scan of bronze: events per table and op | 318 | 30 | **10.7x** |
| `bronze_window` | One table, 30-minute event-time window (partition + file-stats pruning) | 118 | 22 | **5.5x** |
| `silver_orders_agg` | Revenue by status over silver orders | 28 | 22 | **1.3x** |
| `silver_orders_customer` | All orders of one customer (Z-order on customer_id) | 31 | 18 | **1.7x** |
| `scd2_point_in_time` | SCD2 history + as-of lookup for one customer (Z-order on customer_id) | 20 | 20 | **1.0x** |
| `quarantine_reasons` | Quarantine failures per table (many tiny append files before tuning) | 62 | 26 | **2.4x** |
| `gold_star_join` | Star join: revenue by month x tier-at-order x category | 70 | 62 | **1.1x** |
| `gold_one_day` | Fact rows for one order date (partition pruning once partitioned) | 16 | 16 | **1.1x** |

## What the numbers say

* **Compaction is the big win, where there were small files.** Bronze is
  written by a 1-second streaming trigger, so it had accumulated 2,846 files
  of a few KB each. Every query paid to open and read the footer of every one,
  and DuckDB and Trino also had to read a large Delta log. Compacted to 3 files
  (one per table partition), full scans got **10–15× faster** on both engines.
  Quarantine went from 341 one-per-batch files to 1, **2.4–4.5× faster**.
* **Silver and SCD2 barely moved, and DuckDB got slightly slower** (about 4 ms
  to about 6 ms). The streaming MERGE rewrites these small tables on every
  batch, so they were already at 4 files. Compacting to 1 file removes
  DuckDB's file-level parallelism, which at this size outweighs the saved file
  opens. These are millisecond queries either way.
* **Z-ordering had nothing to skip.** Z-ordering clusters rows so that each
  file's min/max statistics cover a narrow range of the key, letting engines
  skip files. Each table here fits in one file after compaction, so there is
  no other file to skip to. It starts paying off once a table spans many
  target-size files (Delta's default target is about 1 GB, so at roughly
  hundreds of MB to GB of data).
* **Date partitioning showed no gain, as expected.** All of the data was
  generated within about 3 hours of one day, so `event_date` and `order_date`
  each have a single value and pruning cannot remove anything. The layout is
  still the right one for production, where it limits time-bounded scans and
  compaction to the affected days. This dataset cannot show it.
* **Results were identical before and after** for every query on both engines.
  The benchmark compares result fingerprints, so the tuning changed only speed.

## Operational lessons from doing it

* Re-partitioning bronze added a column, which is a schema change to the
  silver query's *streaming source*. The silver query then failed on every
  restart with `DELTA_SCHEMA_CHANGED_WITH_VERSION`, while bronze kept
  ingesting. Fix: `maintenance relayout-bronze` now also restarts silver on a
  new checkpoint from the version after the rewrite (`skipChangeCommits` keeps
  the rewrite itself from being re-read). The recovered pipeline reconciled
  exactly with Postgres.
* OPTIMIZE commits are `dataChange=false`, so the streaming reader ignores
  them, and compaction can run on a schedule (the Airflow maintenance DAG)
  without disturbing the stream.
