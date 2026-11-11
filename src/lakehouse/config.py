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
SCD2_CUSTOMERS_PATH = lake_path("silver", "dim_customers_scd2")
METRICS_PATH = lake_path("_metrics", "batch_latency.jsonl")


def silver_path(table: str) -> str:
    return lake_path("silver", table)


def checkpoint_path(name: str) -> str:
    return lake_path("_checkpoints", name)
