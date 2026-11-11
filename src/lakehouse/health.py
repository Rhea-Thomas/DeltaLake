"""Pipeline health and lag, for orchestration (Airflow) and humans.

    python -m lakehouse.health services        # Postgres, Kafka, Connect, Trino reachable
    python -m lakehouse.health lag             # how far bronze and silver trail the source
    python -m lakehouse.health lag --max-kafka-lag 50000 --max-silver-lag 50

Lag is measured without touching the running Spark app:

* Kafka -> bronze: Kafka end offsets (messages produced) minus bronze rows.
* bronze -> silver: bronze table version minus the bronze version recorded in
  the silver query's latest checkpoint offset file (Delta source offsets carry
  ``reservoirVersion``). Each bronze version is one bronze micro-batch.

Exit code 1 when a threshold is exceeded, so an orchestrator task fails loudly.
"""
from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
from pathlib import Path

from . import config
from .reconcile import COMPOSE_FILE, SOURCE_DSN, kafka_message_count


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def services() -> dict[str, bool]:
    out = {
        "postgres:5433": _port_open("localhost", 5433),
        "kafka:9092": _port_open("localhost", 9092),
        "connect:8083": _port_open("localhost", 8083),
        "trino:8080": _port_open("localhost", 8080),
    }
    try:
        import psycopg
        with psycopg.connect(SOURCE_DSN, connect_timeout=3) as pg:
            slot = pg.execute(
                "SELECT active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                "FROM pg_replication_slots WHERE slot_name = 'dbz_slot'").fetchone()
        out["replication slot active"] = bool(slot and slot[0])
        out["_slot_retained_wal_bytes"] = int(slot[1]) if slot else None
    except Exception:  # noqa: BLE001
        out["replication slot active"] = False
    return out


def _bronze_version() -> int:
    log = Path(config.BRONZE_PATH) / "_delta_log"
    return max(int(p.stem) for p in log.glob("*.json"))


def _silver_processed_bronze_version() -> int | None:
    offsets = Path(config.checkpoint_path("silver")) / "offsets"
    files = sorted((p for p in offsets.iterdir() if p.name.isdigit()), key=lambda p: int(p.name))
    if not files:
        return None
    # Line 1 = version marker, line 2 = batch metadata, line 3+ = one offset per source.
    source_offset = json.loads(files[-1].read_text().splitlines()[2])
    return int(source_offset["reservoirVersion"])


def lag() -> dict:
    from .query import connect

    bronze_rows = connect().execute("SELECT count(*) FROM bronze_cdc_events").fetchone()[0]
    kafka = kafka_message_count()
    bv, sv = _bronze_version(), _silver_processed_bronze_version()
    return {
        "kafka_messages": kafka,
        "bronze_rows": bronze_rows,
        "kafka_to_bronze_lag_messages": kafka - bronze_rows,
        "bronze_version": bv,
        "silver_processed_bronze_version": sv,
        # reservoirVersion is the *next* version to read, so caught up == bv + 1.
        "bronze_to_silver_lag_versions": None if sv is None else max(0, bv + 1 - sv),
    }


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("check", choices=["services", "lag"])
    ap.add_argument("--max-kafka-lag", type=int, default=100_000)
    ap.add_argument("--max-silver-lag", type=int, default=100)
    args = ap.parse_args(argv)

    if args.check == "services":
        res = services()
        print(json.dumps(res, indent=2))
        bad = [k for k, v in res.items() if not k.startswith("_") and not v]
        if bad:
            print(f"UNHEALTHY: {bad}", file=sys.stderr)
            sys.exit(1)
        return

    res = lag()
    print(json.dumps(res, indent=2))
    problems = []
    if res["kafka_to_bronze_lag_messages"] > args.max_kafka_lag:
        problems.append(f"bronze is {res['kafka_to_bronze_lag_messages']} messages behind Kafka")
    if res["bronze_to_silver_lag_versions"] is None:
        problems.append("silver query has no checkpoint yet")
    elif res["bronze_to_silver_lag_versions"] > args.max_silver_lag:
        problems.append(f"silver is {res['bronze_to_silver_lag_versions']} bronze commits behind")
    if problems:
        print("LAGGING: " + "; ".join(problems), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
