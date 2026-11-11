"""Package captured change events (and a Postgres snapshot) as files for Databricks.

Databricks Free Edition cannot reach a Kafka broker on your laptop, so the CDC
part stays local (Postgres -> Debezium -> Kafka -> bronze) and this script
exports the raw Kafka records as gzipped JSON lines. Uploaded to a Unity
Catalog volume, they feed the same pipeline with BRONZE_SOURCE=files.

    python -m lakehouse.export_for_databricks            # incremental: only new records
    python -m lakehouse.export_for_databricks --full     # everything again (new export dir)

Output (default ./export/):
    landing/events-<run>.json.gz   Kafka records: topic, partition, offset, timestamp, key, value
    source_snapshot/<table>.json.gz   Postgres rows at export time (for reconciliation)
    manifest.json                  record counts, offsets, export time
    .state.json                    highest exported offset per topic-partition (incremental)

The local bronze table is an exact copy of Kafka (reconciliation proves bronze
rows == Kafka messages with unique offsets), so records are read from bronze
with DuckDB. Run it while the pipeline is idle and caught up, so the snapshot
and the events describe the same moment.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from pathlib import Path

import duckdb
import psycopg

from . import config
from .reconcile import SOURCE_DSN
from .tables import TABLES

DEFAULT_OUT = config.REPO_ROOT / "export"


def _snapshot_select(spec) -> str:
    cols = []
    for c, t in spec.columns:
        if t == "timestamp":
            cols.append(f"(extract(epoch from {c}) * 1000000)::bigint AS {c}")   # micros, exact
        elif t.startswith("decimal"):
            cols.append(f"{c}::text AS {c}")                                     # exact decimal
        else:
            cols.append(c)
    return ", ".join(cols)


def export_snapshot(out: Path) -> dict:
    snap = out / "source_snapshot"
    snap.mkdir(parents=True, exist_ok=True)
    counts = {}
    with psycopg.connect(SOURCE_DSN) as pg:
        for t, spec in TABLES.items():
            rows = pg.execute(f"SELECT row_to_json(x)::text FROM (SELECT {_snapshot_select(spec)} FROM {t}) x").fetchall()
            with gzip.open(snap / f"{t}.json.gz", "wt") as fh:
                for (line,) in rows:
                    fh.write(line + "\n")
            counts[t] = len(rows)
    return counts


def export_events(out: Path, full: bool) -> dict:
    state_path = out / ".state.json"
    state = {} if full or not state_path.exists() else json.loads(state_path.read_text())
    landing = out / "landing"
    landing.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; SET TimeZone = 'UTC'")
    con.execute(f"CREATE VIEW bronze AS SELECT * FROM delta_scan('{config.BRONZE_PATH}')")
    conds = " OR ".join(
        f"(topic = '{tp.rsplit(':', 1)[0]}' AND \"partition\" = {tp.rsplit(':', 1)[1]} AND \"offset\" <= {off})"
        for tp, off in state.items()
    )
    where = f"WHERE NOT ({conds})" if conds else ""
    run = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    target = landing / f"events-{run}.json.gz"
    con.execute(f"""
        COPY (
            SELECT topic, "partition", "offset",
                   strftime(kafka_ts, '%Y-%m-%dT%H:%M:%S.%fZ') AS "timestamp",
                   key, value
            FROM bronze {where}
            ORDER BY topic, "partition", "offset"
        ) TO '{target}' (FORMAT JSON, COMPRESSION GZIP)
    """)
    n = con.execute(f"SELECT count(*) FROM read_json_auto('{target}')").fetchone()[0]
    if n == 0:
        target.unlink()
        return {"file": None, "records": 0}
    for tp, off in con.execute(f"""
            SELECT topic || ':' || "partition", max("offset") FROM read_json_auto('{target}') GROUP BY 1""").fetchall():
        state[tp] = max(off, state.get(tp, -1))
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True))
    return {"file": str(target.relative_to(out)), "records": n}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--full", action="store_true", help="ignore previous exports and export everything")
    args = ap.parse_args(argv)

    args.out.mkdir(parents=True, exist_ok=True)
    events = export_events(args.out, args.full)
    snapshot = export_snapshot(args.out)
    total = sum(1 for f in (args.out / "landing").glob("*.json.gz")
                for _ in gzip.open(f, "rt"))
    manifest = {
        "exported_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "this_run": events,
        "landing_files_total_records": total,
        "source_snapshot_rows": snapshot,
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    print(f"\nUpload the contents of {args.out}/ (landing/, source_snapshot/, manifest.json)"
          " to your Databricks volume, keeping the folder names.")


if __name__ == "__main__":
    main()
