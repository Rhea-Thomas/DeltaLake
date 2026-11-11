"""Delta table maintenance: compaction, Z-ordering, vacuum, layout changes.

    python -m lakehouse.maintenance stats              # files and bytes per table
    python -m lakehouse.maintenance optimize           # compact + Z-order (see LAYOUT)
    python -m lakehouse.maintenance vacuum --retain-hours 168
    python -m lakehouse.maintenance relayout-bronze    # one-off: partition bronze by (table, event_date)

Why each table gets what it gets (LAYOUT):

* bronze: append-only and partitioned by (source_table, event_date). Every
  1-second micro-batch adds a few small files per partition; compaction merges
  them. Queries filter by table and time, which partition pruning serves.
* silver current-state tables: rewritten by MERGE every batch, which leaves
  many small files. Compaction plus Z-ordering on the column analysts filter by
  (customer_id), so file min/max statistics let engines skip most files.
* SCD2: Z-ordered by customer_id, because the typical query is "history / as-of
  for one customer".
* gold (dbt/Trino) is rebuilt from scratch each run, so it is already
  compact; its layout (partitioning) is set in the dbt models.

OPTIMIZE only rewrites files (dataChange=false), so streaming readers ignore
its commits. It can still conflict with a concurrent streaming MERGE on the
same files; the losing side retries (here: up to 3 attempts with backoff).
"""
from __future__ import annotations

import argparse
import json
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import config
from .spark_session import get_spark

LAYOUT: dict[str, tuple[str, list[str] | None]] = {
    "bronze.cdc_events": (config.BRONZE_PATH, None),
    "silver.customers": (config.silver_path("customers"), ["customer_id"]),
    "silver.products": (config.silver_path("products"), ["product_id"]),
    "silver.orders": (config.silver_path("orders"), ["customer_id"]),
    "silver.dim_customers_scd2": (config.SCD2_CUSTOMERS_PATH, ["customer_id"]),
    "quarantine.cdc_rejects": (config.QUARANTINE_PATH, None),
}
GOLD = ["fct_orders", "fct_daily_sales", "dim_customer", "dim_product", "dim_date", "dim_order_status"]


def _with_retry(fn, what: str, attempts: int = 3):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # Delta ConcurrentModificationException and friends
            if "Concurrent" not in type(exc).__name__ + str(exc)[:300] or i == attempts:
                raise
            print(f"  {what}: concurrent write conflict, retry {i}/{attempts - 1}", flush=True)
            time.sleep(5 * i)


def stats(spark: SparkSession) -> dict:
    out = {}
    paths = {k: v[0] for k, v in LAYOUT.items()}
    paths.update({f"gold.{t}": config.lake_path("gold", t) for t in GOLD})
    for name, path in paths.items():
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        d = DeltaTable.forPath(spark, path).detail().first()
        out[name] = {"files": d["numFiles"], "mb": round(d["sizeInBytes"] / 1e6, 1),
                     "partitioned_by": list(d["partitionColumns"])}
    return out


def optimize(spark: SparkSession, zorder: bool = True) -> dict:
    report = {}
    for name, (path, cols) in LAYOUT.items():
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        t0 = time.time()
        opt = DeltaTable.forPath(spark, path).optimize()
        metrics = _with_retry(
            (lambda: opt.executeZOrderBy(*cols)) if (zorder and cols) else opt.executeCompaction, name
        ).select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        report[name] = {"files_removed": metrics[1], "files_added": metrics[0],
                        "zorder": cols if zorder else None, "seconds": round(time.time() - t0, 1)}
        print(f"  {name}: {metrics[1]} -> {metrics[0]} files ({report[name]['seconds']}s)", flush=True)
    return report


def vacuum(spark: SparkSession, retain_hours: float) -> None:
    if retain_hours < 168:
        # Below Delta's 7-day safety default: only safe when no reader or
        # writer (e.g. a stopped stream about to resume) needs older files.
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    for name, (path, _) in LAYOUT.items():
        if DeltaTable.isDeltaTable(spark, path):
            _with_retry(lambda: DeltaTable.forPath(spark, path).vacuum(retain_hours), name)
            print(f"  vacuumed {name} (retain {retain_hours}h)", flush=True)


def relayout_bronze(spark: SparkSession) -> None:
    """Rewrite bronze partitioned by (source_table, event_date). Stop the
    pipeline first. The silver stream reads bronze with skipChangeCommits, so
    it skips this rewrite commit instead of reprocessing the rewritten rows."""
    path = config.BRONZE_PATH
    df = spark.read.format("delta").load(path)
    if "event_date" in df.columns and DeltaTable.forPath(spark, path).detail().first()["partitionColumns"] == [
            "source_table", "event_date"]:
        print("bronze already partitioned by (source_table, event_date)")
        return
    if "event_date" not in df.columns:
        df = df.withColumn("event_date", F.to_date(F.expr("timestamp_millis(source_ts_ms)")))
    n = df.count()
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .partitionBy("source_table", "event_date")
       .save(path))
    print(f"bronze rewritten: {n} rows, partitioned by (source_table, event_date)")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["stats", "optimize", "compact", "vacuum", "relayout-bronze"])
    ap.add_argument("--retain-hours", type=float, default=168)
    args = ap.parse_args(argv)

    spark = get_spark("lakehouse-maintenance", master="local[4]")
    if args.command == "stats":
        print(json.dumps(stats(spark), indent=2))
    elif args.command == "optimize":
        optimize(spark, zorder=True)
    elif args.command == "compact":
        optimize(spark, zorder=False)
    elif args.command == "vacuum":
        vacuum(spark, args.retain_hours)
    elif args.command == "relayout-bronze":
        relayout_bronze(spark)
    spark.stop()


if __name__ == "__main__":
    main()
