"""Runtime configuration. Everything is overridable by environment variables so
the same code runs locally or on a cluster (e.g. LAKE_ROOT=s3://... or a
Databricks Volume path)."""
from __future__ import annotations

import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

LAKE_ROOT = os.environ.get("LAKE_ROOT", str(REPO_ROOT / "lake"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", r"src\.public\..*")
BRONZE_TRIGGER = os.environ.get("BRONZE_TRIGGER", "1 second")
SILVER_TRIGGER = os.environ.get("SILVER_TRIGGER", "2 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "200000"))

# How the streaming queries run:
#   processing_time (default): run forever, one micro-batch every BRONZE/SILVER_TRIGGER
#   available_now: process everything waiting, then stop. The only mode
#     serverless compute (e.g. Databricks Free Edition) allows; schedule it as a job.
TRIGGER_MODE = os.environ.get("TRIGGER_MODE", "processing_time")

# Where bronze reads raw change events from:
#   kafka (default): the Debezium topics
#   files: newline-delimited JSON Kafka records (topic, partition, offset,
#     timestamp, key, value), e.g. exported with lakehouse.export_for_databricks
#     and uploaded to a Databricks volume. Same records, same bronze rows.
BRONZE_SOURCE = os.environ.get("BRONZE_SOURCE", "kafka")


def trigger_kwargs(interval: str) -> dict:
    if TRIGGER_MODE == "available_now":
        return {"availableNow": True}
    if TRIGGER_MODE != "processing_time":
        raise ValueError(f"unknown TRIGGER_MODE {TRIGGER_MODE!r}")
    return {"processingTime": interval}


def kafka_options() -> dict[str, str]:
    """Extra Kafka source options as JSON, e.g. for a managed Kafka with SASL:
    KAFKA_OPTIONS_JSON='{"kafka.security.protocol": "SASL_SSL", ...}'."""
    raw = os.environ.get("KAFKA_OPTIONS_JSON", "").strip()
    if not raw:
        return {}
    opts = json.loads(raw)
    if not isinstance(opts, dict):
        raise ValueError("KAFKA_OPTIONS_JSON must be a JSON object")
    return {str(k): str(v) for k, v in opts.items()}


def lake_path(*parts: str) -> str:
    return "/".join([LAKE_ROOT.rstrip("/"), *parts])


BRONZE_PATH = lake_path("bronze", "cdc_events")
QUARANTINE_PATH = lake_path("quarantine", "cdc_rejects")
BRONZE_SOURCE_PATH = os.environ.get("BRONZE_SOURCE_PATH", lake_path("_landing"))
SCD2_CUSTOMERS_PATH = lake_path("silver", "dim_customers_scd2")
METRICS_PATH = lake_path("_metrics", "batch_latency.jsonl")


def silver_path(table: str) -> str:
    return lake_path("silver", table)


def checkpoint_path(name: str) -> str:
    return lake_path("_checkpoints", name)
