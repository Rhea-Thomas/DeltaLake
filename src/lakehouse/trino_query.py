"""Trino query layer over the same Delta tables.

Trino's Delta Lake connector reads the Delta transaction log, just as DuckDB
does, but as a distributed SQL engine with a catalog. Tables are *registered*
(``system.register_table``) into a file-based metastore: no data is copied,
and Trino and Spark read the very same Parquet files and log.

Usage:
    python -m lakehouse.trino_query                  # register tables, run queries/trino/*.sql
    python -m lakehouse.trino_query --sql "SELECT count(*) FROM customers"
    python -m lakehouse.trino_query --check          # compare aggregates with DuckDB
    python -m lakehouse.trino_query --register       # register tables + gold schemas only (before dbt)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import trino

from . import config
from .tables import TABLES

HOST = os.environ.get("TRINO_HOST", "localhost")
PORT = int(os.environ.get("TRINO_PORT", "8080"))
CATALOG, SCHEMA = "lake", "lakehouse"
# Path of the lake *inside* the Trino container (see docker-compose.yml).
TRINO_LAKE_ROOT = os.environ.get("TRINO_LAKE_ROOT", "file:///lake")

TABLE_LOCATIONS = {
    "bronze_cdc_events": "bronze/cdc_events",
    "silver_customers_all": "silver/customers",
    "silver_products_all": "silver/products",
    "silver_orders_all": "silver/orders",
    "dim_customers_scd2": "silver/dim_customers_scd2",
    "quarantine": "quarantine/cdc_rejects",
}


GOLD_SCHEMAS = {"gold": "gold", "gold_staging": "gold_staging"}


def connect() -> trino.dbapi.Connection:
    return trino.dbapi.connect(host=HOST, port=PORT, user="lakehouse", catalog=CATALOG, schema=SCHEMA)


def _exec(cur, sql: str, params=None) -> list:
    cur.execute(sql, params) if params else cur.execute(sql)
    return cur.fetchall()


def register(conn: trino.dbapi.Connection) -> list[str]:
    """Register every Delta table that exists on disk, plus current-state views."""
    cur = conn.cursor()
    _exec(cur, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    # Schemas dbt writes the gold star schema into. They must exist, with their
    # lake location, before dbt runs: dbt would otherwise create them itself
    # with Trino's default location (inside the metastore directory).
    for schema, rel in GOLD_SCHEMAS.items():
        _exec(cur, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} "
                   f"WITH (location = '{TRINO_LAKE_ROOT}/{rel}')")
    existing = {r[0] for r in _exec(cur, f"SHOW TABLES FROM {CATALOG}.{SCHEMA}")}
    registered = []
    for name, rel in TABLE_LOCATIONS.items():
        if not (Path(config.LAKE_ROOT) / rel / "_delta_log").is_dir():
            continue
        if name not in existing:
            _exec(cur, f"CALL {CATALOG}.system.register_table("
                       f"schema_name => '{SCHEMA}', table_name => '{name}', "
                       f"table_location => '{TRINO_LAKE_ROOT}/{rel}')")
        registered.append(name)
    for t, spec in TABLES.items():
        if f"silver_{t}_all" in registered:
            cols = ", ".join([*spec.column_names, "_lsn", "_source_ts"])
            _exec(cur, f"CREATE OR REPLACE VIEW {t} AS SELECT {cols} "
                       f"FROM silver_{t}_all WHERE NOT _is_deleted")
    return registered


def run_file(conn, path: Path, params: list | None = None) -> None:
    cur = conn.cursor()
    for stmt in [s for s in path.read_text().split(";") if s.strip() and any(
            ln.strip() and not ln.strip().startswith("--") for ln in s.splitlines())]:
        n_params = stmt.count("?")
        rows = _exec(cur, stmt, (params or [])[:n_params] if n_params else None)
        cols = [d[0] for d in cur.description]
        print(f"\n=== {path.name} ===")
        print(" | ".join(cols))
        for r in rows[:25]:
            print(" | ".join("NULL" if v is None else str(v) for v in r))
        if len(rows) > 25:
            print(f"... {len(rows)} rows")


# Aggregates that must be identical whichever engine computes them.
CHECKS = {
    "bronze rows": "SELECT count(*) FROM bronze_cdc_events",
    "bronze max lsn": "SELECT max(lsn) FROM bronze_cdc_events",
    "live customers": "SELECT count(*) FROM customers",
    "live products": "SELECT count(*) FROM products",
    "live orders": "SELECT count(*) FROM orders",
    "revenue (non-cancelled)": "SELECT CAST(sum(quantity * unit_price) AS DECIMAL(38,2)) FROM orders WHERE status <> 'cancelled'",
    "stock units": "SELECT sum(stock) FROM products",
    "scd2 versions": "SELECT count(*) FROM dim_customers_scd2",
    "scd2 current": "SELECT count(*) FROM dim_customers_scd2 WHERE is_current",
    "scd2 deleted": "SELECT count(*) FROM dim_customers_scd2 WHERE is_deleted",
    "quarantine rows": "SELECT count(*) FROM quarantine",
}


def cross_engine_check() -> dict:
    """Run the same aggregates on Trino and DuckDB; return {check: (trino, duckdb)}."""
    from .query import connect as duck_connect

    tconn = connect()
    register(tconn)
    cur = tconn.cursor()
    duck = duck_connect()
    out = {}
    for name, sql in CHECKS.items():
        t = _exec(cur, sql)[0][0]
        d = duck.execute(sql).fetchone()[0]
        out[name] = (t, d)
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*")
    ap.add_argument("--sql")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--register", action="store_true", help="only register tables/schemas, then exit")
    ap.add_argument("--param", action="append", default=[], help="positional ? parameters, in order")
    args = ap.parse_args(argv)

    if args.check:
        results = cross_engine_check()
        ok = True
        for name, (t, d) in results.items():
            same = str(t) == str(d)
            ok &= same
            print(f"{'OK ' if same else 'DIFF'} {name:26s} trino={t}  duckdb={d}")
        sys.exit(0 if ok else 1)

    conn = connect()
    print("registered:", ", ".join(register(conn)))
    if args.register:
        return
    if args.sql:
        cur = conn.cursor()
        for r in _exec(cur, args.sql):
            print(r)
        return
    files = [Path(f) for f in args.files] or sorted((config.REPO_ROOT / "queries" / "trino").glob("*.sql"))
    for f in files:
        run_file(conn, f, args.param)


if __name__ == "__main__":
    main(sys.argv[1:])
