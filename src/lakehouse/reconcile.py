"""End-to-end reconciliation: do the Delta tables match the source Postgres?

The expected state is defined precisely. For every source row, look at the
*latest* change event captured for its key (highest LSN):

* if that event passed data quality, silver must hold the key with values
  identical to Postgres;
* if that event was quarantined, the quarantine table must hold it (silver then
  keeps the last valid version, or nothing if there never was one).

On top of that:

* no loss / no duplicates into bronze: bronze row count == Kafka message count,
  and (topic, partition, offset) is unique in bronze;
* nothing invalid in silver: no silver row carries an LSN that was quarantined;
* no live key in silver that no longer exists in Postgres;
* SCD2: exactly one current version per live customer, current versions equal
  silver, versions == distinct valid customer events, intervals chain with no
  gaps or overlaps.

Run: python -m lakehouse.reconcile [--timeout 300]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field

import psycopg

from . import config
from .query import connect
from .tables import TABLES, TableSpec

SOURCE_DSN = os.environ.get(
    "SOURCE_DSN", "host=localhost port=5433 dbname=shop user=postgres password=postgres"
)
COMPOSE_FILE = str(config.REPO_ROOT / "docker-compose.yml")


@dataclass
class Report:
    ok: bool = True
    failures: list[str] = field(default_factory=list)
    facts: dict = field(default_factory=dict)

    def check(self, cond: bool, msg: str) -> None:
        if not cond:
            self.ok = False
            self.failures.append(msg)


def _select_list(spec: TableSpec, engine: str) -> str:
    out = []
    for col, typ in spec.columns:
        if typ == "timestamp":
            out.append(f"(extract(epoch from {col}) * 1000000)::bigint" if engine == "pg" else f"epoch_us({col})")
        else:
            out.append(col)
    return ", ".join(out)


def kafka_message_count() -> int:
    out = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T", "kafka",
         "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
         "--topic", r"src\.public\..*", "--time", "-1"],
        check=True, capture_output=True, text=True,
    ).stdout
    return sum(int(line.rsplit(":", 1)[1]) for line in out.split() if line.count(":") == 2)


def reconcile_once(pg: psycopg.Connection) -> Report:
    r = Report()
    con = connect()

    kafka_n = kafka_message_count()
    bronze_n, bronze_distinct = con.execute(
        "SELECT count(*), count(DISTINCT (topic, \"partition\", \"offset\")) FROM bronze_cdc_events"
    ).fetchone()
    r.facts.update(kafka_messages=kafka_n, bronze_rows=bronze_n)
    r.check(bronze_n == bronze_distinct, f"bronze has {bronze_n - bronze_distinct} duplicate Kafka offsets")
    r.check(bronze_n == kafka_n, f"bronze rows {bronze_n} != Kafka messages {kafka_n}")

    for spec in TABLES.values():
        t, pk = spec.name, spec.pk
        source = {row[0]: row for row in pg.execute(
            f"SELECT {_select_list(spec, 'pg')} FROM {t}").fetchall()}
        silver_all = {row[0]: row for row in con.execute(
            f"SELECT {_select_list(spec, 'duck')}, _is_deleted, _lsn FROM silver_{t}_all").fetchall()}
        live = {k: v[:-2] for k, v in silver_all.items() if not v[-2]}
        latest = dict(con.execute(f"""
            SELECT CAST(json_extract_string(key, '$.{pk}') AS BIGINT) AS k, max(lsn)
            FROM bronze_cdc_events WHERE source_table = '{t}' GROUP BY k""").fetchall())
        quarantined = {(int(k), lsn) for k, lsn in con.execute(
            f"SELECT record_key, lsn FROM quarantine WHERE source_table = '{t}' AND record_key IS NOT NULL"
        ).fetchall()}

        matched = stale_ok = 0
        mismatches, missing = [], []
        for k, row in source.items():
            if k not in latest:
                missing.append(k)
            elif (k, latest[k]) in quarantined:
                stale_ok += 1
            elif live.get(k) == row:
                matched += 1
            else:
                mismatches.append((k, row, live.get(k)))
        extra = [k for k in live if k not in source]
        invalid_in_silver = [k for k, v in silver_all.items() if (k, v[-1]) in quarantined]

        r.facts[t] = dict(source_rows=len(source), silver_live=len(live), silver_soft_deleted=len(silver_all) - len(live),
                          matched_exactly=matched, latest_change_quarantined=stale_ok,
                          quarantined_events=len(quarantined))
        r.check(not missing, f"{t}: {len(missing)} source keys never captured, e.g. {missing[:5]}")
        r.check(not mismatches, f"{t}: {len(mismatches)} rows differ, e.g. {mismatches[:2]}")
        r.check(not extra, f"{t}: {len(extra)} live silver keys absent from source, e.g. {extra[:5]}")
        r.check(not invalid_in_silver, f"{t}: quarantined events reached silver for keys {invalid_in_silver[:5]}")

    # ---- SCD2 ----
    c = TABLES["customers"]
    cur = {row[0]: row for row in con.execute(
        f"SELECT {_select_list(c, 'duck')} FROM dim_customers_scd2 WHERE is_current").fetchall()}
    silver_c = {row[0]: row for row in con.execute(f"SELECT {_select_list(c, 'duck')} FROM customers").fetchall()}
    multi_current = con.execute(
        "SELECT count(*) FROM (SELECT customer_id FROM dim_customers_scd2 WHERE is_current GROUP BY 1 HAVING count(*) > 1)"
    ).fetchone()[0]
    versions = con.execute("SELECT count(*) FROM dim_customers_scd2").fetchone()[0]
    valid_events = con.execute("""
        SELECT count(*) FROM (
          SELECT DISTINCT CAST(json_extract_string(b.key, '$.customer_id') AS BIGINT) AS k, b.lsn
          FROM bronze_cdc_events b
          WHERE b.source_table = 'customers'
            AND NOT EXISTS (SELECT 1 FROM quarantine q WHERE q.source_table = 'customers' AND q.lsn = b.lsn
                              AND q.record_key = json_extract_string(b.key, '$.customer_id')))
    """).fetchone()[0]
    broken_chain = con.execute("""
        SELECT count(*) FROM (
          SELECT valid_to, lead(valid_from) OVER w AS nf, _end_lsn, lead(_lsn) OVER w AS nl
          FROM dim_customers_scd2 WINDOW w AS (PARTITION BY customer_id ORDER BY _lsn))
        WHERE valid_to IS DISTINCT FROM nf OR _end_lsn IS DISTINCT FROM nl
    """).fetchone()[0]
    r.facts["dim_customers_scd2"] = dict(versions=versions, current=len(cur), valid_customer_events=valid_events)
    r.check(multi_current == 0, f"scd2: {multi_current} customers with more than one current row")
    r.check(cur == silver_c, f"scd2: current versions != silver customers ({len(cur)} vs {len(silver_c)})")
    r.check(versions == valid_events, f"scd2: {versions} versions != {valid_events} distinct valid customer events")
    r.check(broken_chain == 0, f"scd2: {broken_chain} versions whose interval does not chain to the next")

    q_dupes = con.execute(
        "SELECT count(*) - count(DISTINCT (source_table, record_key, lsn)) FROM quarantine").fetchone()[0]
    r.check(q_dupes == 0, f"quarantine has {q_dupes} duplicate rows")
    r.facts["quarantine_rows"] = con.execute("SELECT count(*) FROM quarantine").fetchone()[0]
    return r


def reconcile(timeout_s: float = 300, poll_s: float = 5) -> tuple[Report, float]:
    """Poll until the lake has caught up and every check passes, or time out."""
    start = time.monotonic()
    with psycopg.connect(SOURCE_DSN, autocommit=True) as pg:
        while True:
            try:
                report = reconcile_once(pg)
            except Exception as exc:  # tables may not exist yet
                report = Report(ok=False, failures=[f"error: {exc}"])
            elapsed = time.monotonic() - start
            if report.ok or elapsed > timeout_s:
                return report, elapsed
            time.sleep(poll_s)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=float, default=300)
    args = ap.parse_args()
    report, elapsed = reconcile(args.timeout)
    print(json.dumps(report.facts, indent=2, default=str))
    if report.ok:
        print(f"RECONCILED: Delta tables match source Postgres (converged after {elapsed:.0f}s of polling)")
    else:
        print("RECONCILIATION FAILED:")
        for f in report.failures:
            print("  -", f)
        sys.exit(1)


if __name__ == "__main__":
    main()
