"""Streaming pipeline: Kafka -> bronze -> (quality gate) -> silver / quarantine / SCD2.

Two Structured Streaming queries run in one Spark application:

1. **bronze**: Kafka topics ``src.public.*`` -> ``bronze/cdc_events`` (append-only,
   raw Debezium JSON, partitioned by source table). The Delta sink plus the
   query checkpoint (Kafka offsets) gives exactly-once delivery into bronze.

2. **silver**: streams *from the bronze Delta table* and, per micro-batch:
   parse -> validate -> quarantine rejects -> MERGE silver current state ->
   MERGE SCD2 history. Every write is idempotent, so a batch that is replayed
   after a crash (Spark re-runs the last uncommitted batch id) changes nothing:
     * silver MERGE only applies changes with a higher LSN than stored;
     * SCD2 MERGE is keyed by (key, LSN);
     * quarantine append uses Delta's idempotent-write options
       (txnAppId + txnVersion = batch id), so it is skipped if already committed.

Run:  python -m lakehouse.jobs.pipeline
"""
from __future__ import annotations

import json
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import config
from ..cdc import kafka_to_bronze, parse_changes, to_typed
from ..quality import split_valid_invalid, to_quarantine
from ..scd2 import apply_scd2
from ..silver import upsert_current_state
from ..spark_session import get_spark
from ..tables import CUSTOMERS, TABLES

QUARANTINE_APP_ID = "lakehouse-silver-quarantine"


def start_bronze(spark: SparkSession):
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribePattern", config.TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.metadata.max.age.ms", "5000")   # notice new topics quickly
        .load()
    )
    return (
        kafka_to_bronze(kafka)
        .writeStream.format("delta")
        .queryName("bronze")
        .outputMode("append")
        .partitionBy("source_table")
        .option("checkpointLocation", config.checkpoint_path("bronze"))
        .trigger(processingTime=config.BRONZE_TRIGGER)
        .start(config.BRONZE_PATH)
    )


def _append_metrics(record: dict) -> None:
    if "://" in config.METRICS_PATH:
        return  # remote lake root: the JSONL metrics file is a local-only convenience
    path = Path(config.METRICS_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def process_silver_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    started = time.time()
    batch = batch.persist()
    try:
        # Small projection collected to the driver for latency accounting only.
        events = batch.select(
            "source_table", "op", "source_ts_ms",
            (F.col("kafka_ts").cast("double") * 1000).alias("kafka_ms"),
            (F.col("ingested_at").cast("double") * 1000).alias("bronze_ms"),
        ).collect()
        if not events:
            return

        def process_table(spec):
            # Each table writes only its own Delta tables, so the three tables
            # run concurrently as independent Spark jobs from driver threads.
            changes = parse_changes(batch, spec)
            valid, invalid = split_valid_invalid(changes, spec)
            valid = to_typed(valid, spec).persist()
            invalid = invalid.persist()
            try:
                n_valid, n_invalid = valid.count(), invalid.count()
                if n_valid:
                    upsert_current_state(spark, valid, config.silver_path(spec.name), spec)
                    if spec is CUSTOMERS:
                        apply_scd2(spark, valid, config.SCD2_CUSTOMERS_PATH, spec)
                reject = to_quarantine(invalid, spec, batch_id).localCheckpoint(eager=True) if n_invalid else None
                return spec.name, {"valid": n_valid, "invalid": n_invalid}, reject
            finally:
                valid.unpersist()
                invalid.unpersist()

        with ThreadPoolExecutor(max_workers=len(TABLES)) as pool:
            results = list(pool.map(process_table, TABLES.values()))
        counts = {name: c for name, c, _ in results}
        rejects = [r for _, _, r in results if r is not None]

        if rejects:
            out = rejects[0]
            for r in rejects[1:]:
                out = out.unionByName(r)
            (
                out.write.format("delta").mode("append")
                .option("txnAppId", QUARANTINE_APP_ID)
                .option("txnVersion", batch_id)
                .save(config.QUARANTINE_PATH)
            )

        committed = time.time()
        commit_ms = committed * 1000
        # Snapshot ('r') events carry the snapshot time, not a commit time, so
        # they are excluded from latency.
        live = [e for e in events if e["op"] != "r" and e["source_ts_ms"] is not None]
        _append_metrics({
            "batch_id": batch_id,
            "started_ms": int(started * 1000),
            "committed_ms": int(commit_ms),
            "duration_s": round(committed - started, 3),
            "n_events": len(events),
            "counts": counts,
            # end to end: Postgres commit -> visible in silver/SCD2 (after all MERGEs)
            "e2e_ms": [int(commit_ms - e["source_ts_ms"]) for e in live],
            # stage breakdown: Postgres commit -> Kafka append, -> bronze micro-batch start
            "to_kafka_ms": [int(e["kafka_ms"] - e["source_ts_ms"]) for e in live],
            "to_bronze_ms": [int(e["bronze_ms"] - e["source_ts_ms"]) for e in live],
        })
        print(f"[silver] batch {batch_id}: {len(events)} events in {committed - started:.2f}s {counts}", flush=True)
    finally:
        batch.unpersist()


def start_silver(spark: SparkSession):
    bronze = spark.readStream.format("delta").load(config.BRONZE_PATH)
    return (
        bronze.writeStream.queryName("silver")
        .foreachBatch(process_silver_batch)
        .option("checkpointLocation", config.checkpoint_path("silver"))
        .trigger(processingTime=config.SILVER_TRIGGER)
        .start()
    )


def wait_for_bronze_table(spark: SparkSession, timeout_s: int = 120) -> None:
    from delta.tables import DeltaTable

    deadline = time.time() + timeout_s
    while not DeltaTable.isDeltaTable(spark, config.BRONZE_PATH):
        if time.time() > deadline:
            raise TimeoutError("bronze table did not appear; is Kafka/Debezium running?")
        time.sleep(1)


def main() -> None:
    spark = get_spark("lakehouse-pipeline", with_kafka=True)
    print(f"lake root: {config.LAKE_ROOT}", flush=True)
    bronze_q = start_bronze(spark)
    wait_for_bronze_table(spark)
    silver_q = start_silver(spark)

    def _stop(*_):
        print("stopping queries...", flush=True)
        for q in (silver_q, bronze_q):
            try:
                q.stop()
            except Exception:  # noqa: BLE001
                pass

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    if os.environ.get("PIPELINE_RUN_SECONDS"):
        spark.streams.awaitAnyTermination(int(os.environ["PIPELINE_RUN_SECONDS"]) * 1000)
        _stop()
    else:
        spark.streams.awaitAnyTermination()
    for q in (bronze_q, silver_q):
        if q.exception():
            raise q.exception()


if __name__ == "__main__":
    main()
