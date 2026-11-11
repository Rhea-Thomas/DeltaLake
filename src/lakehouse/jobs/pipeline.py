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
     * quarantine is an insert-only MERGE keyed by (table, key, LSN), so neither
       a replayed batch nor a change Debezium re-published later adds a row.

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
from ..quality import split_valid_invalid, to_quarantine, write_quarantine
from ..scd2 import apply_scd2
from ..silver import upsert_current_state
from ..spark_session import get_spark
from ..tables import CUSTOMERS, TABLES

FAULT_POINTS = ("before_scd2", "after_silver", "after_quarantine")


def _fault(point: str, batch_id: int) -> None:
    """Failure injection for tests: FAULT_INJECT="<point>@<min_batch_id>".

    Hard-exits the driver (no cleanup, like a kill -9 or a lost node) at a named
    point *inside* a silver micro-batch, after some of its writes have committed
    and before Spark has recorded the batch as done. On restart Spark re-runs
    the same batch id; idempotent writes must make that replay harmless.
    """
    spec = os.environ.get("FAULT_INJECT")
    if not spec:
        return
    name, _, at = spec.partition("@")
    if name == point and batch_id >= int(at or 0):
        print(f"[fault] crashing at {point} in batch {batch_id}", flush=True)
        os._exit(137)


def start_bronze(spark: SparkSession):
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribePattern", config.TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.metadata.max.age.ms", "5000")   # notice new topics quickly
        .options(**config.kafka_options())
        .load()
    )
    return (
        kafka_to_bronze(kafka)
        .writeStream.format("delta")
        .queryName("bronze")
        .outputMode("append")
        .partitionBy("source_table", "event_date")
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
                        _fault("before_scd2", batch_id)
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

        _fault("after_silver", batch_id)
        if rejects:
            out = rejects[0]
            for r in rejects[1:]:
                out = out.unionByName(r)
            write_quarantine(spark, out, config.QUARANTINE_PATH)

        _fault("after_quarantine", batch_id)
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
    # skipChangeCommits: ignore commits that rewrite existing bronze files (a
    # one-off re-layout such as `maintenance relayout-bronze`) instead of failing
    # or re-reading them as new data. Appends are unaffected, and OPTIMIZE
    # commits (dataChange=false) are skipped by the Delta source anyway.
    reader = spark.readStream.format("delta").option("skipChangeCommits", "true")
    # After a bronze re-layout the silver query starts on a fresh checkpoint
    # from a recorded version (see lakehouse.maintenance.restart_silver_from).
    marker = Path(config.checkpoint_path("silver.start_version"))
    if marker.exists() and not (Path(config.checkpoint_path("silver")) / "offsets").exists():
        version = int(marker.read_text().strip())
        print(f"[pipeline] silver starting from bronze version {version} on a new checkpoint", flush=True)
        reader = reader.option("startingVersion", version)
    bronze = reader.load(config.BRONZE_PATH)
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
    """Run both queries; with supervision (default) restart them after a failure.

    A failed query (for example the Kafka source giving up while the broker is
    down) is restarted from its checkpoint with exponential backoff. This is
    the job-retry behaviour a scheduler such as Databricks Jobs provides; the
    checkpoints and idempotent writes are what make a restart safe.
    """
    spark = get_spark("lakehouse-pipeline", with_kafka=True)
    supervise = os.environ.get("PIPELINE_SUPERVISE", "1") == "1"
    run_seconds = float(os.environ.get("PIPELINE_RUN_SECONDS", "0"))
    deadline = time.time() + run_seconds if run_seconds else None
    print(f"lake root: {config.LAKE_ROOT} supervise={supervise}", flush=True)

    queries: list = []
    stopping = {"flag": False}

    def _stop(*_):
        stopping["flag"] = True
        print("stopping queries...", flush=True)
        for q in queries:
            try:
                q.stop()
            except Exception:  # noqa: BLE001
                pass

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    restarts = 0
    while not stopping["flag"]:
        queries[:] = [start_bronze(spark)]
        wait_for_bronze_table(spark)
        queries.append(start_silver(spark))
        print("[pipeline] queries started", flush=True)
        failure = None
        try:
            timeout_ms = int((deadline - time.time()) * 1000) if deadline else None
            if timeout_ms is None:
                spark.streams.awaitAnyTermination()
            elif timeout_ms > 0:
                spark.streams.awaitAnyTermination(timeout_ms)
        except Exception as exc:  # StreamingQueryException
            failure = exc
        spark.streams.resetTerminated()
        for q in queries:
            try:
                q.stop()
            except Exception:  # noqa: BLE001
                pass
        if stopping["flag"] or (deadline and time.time() >= deadline):
            break
        if failure is None:
            failure = next((q.exception() for q in queries if q.exception()), None)
        if not supervise:
            if failure:
                raise failure
            break
        restarts += 1
        backoff = min(30, 2 ** min(restarts, 5))
        print(f"[supervisor] query failed ({str(failure).splitlines()[0][:200] if failure else 'terminated'}); "
              f"restart #{restarts} in {backoff}s from checkpoints", flush=True)
        time.sleep(backoff)


if __name__ == "__main__":
    main()
