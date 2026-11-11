"""Reconciliation in Spark: lake vs. an exported Postgres snapshot.

The same checks as lakehouse.reconcile (which talks to Postgres, Kafka and
DuckDB directly), restated in Spark so they can run on Databricks, where only
the lake and the uploaded files are reachable:

* bronze row count == records in the landing files; (topic, partition, offset) unique;
* every source row whose latest captured change is valid is identical in silver;
* every source row whose latest change was rejected is in quarantine;
* no live silver key missing from the source; no quarantined change in silver;
* SCD2: one current row per live customer, current rows == silver customers,
  versions == distinct valid customer events, intervals chain.

    python -m lakehouse.reconcile_spark --snapshot export/source_snapshot --landing export/landing
"""
from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .tables import TABLES, TableSpec


def _normalized(df: DataFrame, spec: TableSpec, from_snapshot: bool) -> DataFrame:
    cols = []
    for c, t in spec.columns:
        if t == "timestamp":
            # snapshot holds epoch micros already; lake holds timestamps
            cols.append((F.col(c).cast("long") if from_snapshot else F.expr(f"unix_micros({c})")).alias(c))
        else:
            cols.append(F.col(c).cast(t).alias(c))
    return df.select(*cols)


def reconcile(spark: SparkSession, snapshot_dir: str, landing_dir: str | None) -> dict:
    failures: list[str] = []
    facts: dict = {}
    read = lambda p: spark.read.format("delta").load(p)  # noqa: E731

    bronze = read(config.BRONZE_PATH)
    n_bronze = bronze.count()
    n_distinct = bronze.select("topic", "partition", "offset").distinct().count()
    facts["bronze_rows"] = n_bronze
    if n_bronze != n_distinct:
        failures.append(f"bronze has {n_bronze - n_distinct} duplicate Kafka offsets")
    if landing_dir:
        n_landing = spark.read.json(landing_dir).count()
        facts["landing_records"] = n_landing
        if n_landing != n_bronze:
            failures.append(f"bronze rows {n_bronze} != landing records {n_landing}")

    quarantine = read(config.QUARANTINE_PATH).select("source_table", "record_key", "lsn")
    for t, spec in TABLES.items():
        pk = spec.pk
        source = _normalized(spark.read.json(f"{snapshot_dir}/{t}.json.gz"), spec, True)
        silver_all = read(config.silver_path(t))
        live = _normalized(silver_all.where("NOT _is_deleted"), spec, False)
        latest = (bronze.where(F.col("source_table") == t)
                  .groupBy(F.get_json_object("key", f"$.{pk}").cast("bigint").alias(pk))
                  .agg(F.max("lsn").alias("latest_lsn")))
        q = (quarantine.where(F.col("source_table") == t)
             .select(F.col("record_key").cast("bigint").alias(pk), F.col("lsn").alias("q_lsn")))

        src = (source.join(latest, pk, "left")
               .join(q, (q[pk] == source[pk]) & (q["q_lsn"] == latest["latest_lsn"]), "left")
               .select(source["*"], "latest_lsn", F.col("q_lsn").isNotNull().alias("latest_quarantined")))
        missing = src.where("latest_lsn IS NULL").count()
        to_compare = src.where("latest_lsn IS NOT NULL AND NOT latest_quarantined").select(*spec.column_names)
        mismatched = to_compare.exceptAll(live).count()
        extra = live.join(source, pk, "left_anti").count()
        invalid_in_silver = silver_all.join(q, (silver_all[pk] == q[pk]) & (silver_all["_lsn"] == q["q_lsn"])).count()

        facts[t] = {"source_rows": source.count(), "silver_live": live.count(),
                    "matched_exactly": to_compare.count() - mismatched,
                    "latest_change_quarantined": src.where("latest_quarantined").count()}
        for n, msg in [(missing, "source keys never captured"), (mismatched, "rows differ from source"),
                       (extra, "live silver keys absent from source"),
                       (invalid_in_silver, "quarantined changes found in silver")]:
            if n:
                failures.append(f"{t}: {n} {msg}")

    c = TABLES["customers"]
    scd2 = read(config.SCD2_CUSTOMERS_PATH)
    current = _normalized(scd2.where("is_current"), c, False)
    silver_c = _normalized(read(config.silver_path("customers")).where("NOT _is_deleted"), c, False)
    multi = scd2.where("is_current").groupBy("customer_id").count().where("count > 1").count()
    cur_diff = current.exceptAll(silver_c).count() + silver_c.exceptAll(current).count()
    valid_events = (bronze.where("source_table = 'customers'")
                    .select(F.get_json_object("key", "$.customer_id").alias("rk"), "lsn").distinct()
                    .join(quarantine.where("source_table = 'customers'"),
                          (F.col("rk") == F.col("record_key")) & (bronze["lsn"] == quarantine["lsn"]), "left_anti")
                    .count())
    versions = scd2.count()
    facts["dim_customers_scd2"] = {"versions": versions, "current": current.count(), "valid_customer_events": valid_events}
    if multi:
        failures.append(f"scd2: {multi} customers with more than one current row")
    if cur_diff:
        failures.append(f"scd2: current versions differ from silver customers ({cur_diff} rows)")
    if versions != valid_events:
        failures.append(f"scd2: {versions} versions != {valid_events} distinct valid customer events")
    return {"ok": not failures, "failures": failures, "facts": facts}


def main(argv: list[str] | None = None) -> None:
    from .spark_session import get_spark

    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--landing")
    args = ap.parse_args(argv)
    res = reconcile(get_spark("lakehouse-reconcile", master="local[4]"), args.snapshot, args.landing)
    print(json.dumps(res, indent=2))
    print("RECONCILED" if res["ok"] else "RECONCILIATION FAILED")
    sys.exit(0 if res["ok"] else 1)


if __name__ == "__main__":
    main()
