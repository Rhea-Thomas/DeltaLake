"""DuckDB query layer over the Delta tables.

DuckDB's ``delta`` extension (built on delta-kernel-rs) reads the Delta
transaction log directly, so no Spark is needed to query the lakehouse. This
module registers one view per table and runs SQL files from ``queries/``.

Usage:
    python -m lakehouse.query                       # run every queries/*.sql
    python -m lakehouse.query queries/07_customer_as_of.sql --var customer_id=42 \
        --var "as_of=2026-11-10 18:00:00+00"
    python -m lakehouse.query --sql "SELECT count(*) FROM customers"
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

from . import config

REPO = config.REPO_ROOT

RAW_VIEWS = {
    "bronze_cdc_events": lambda: config.BRONZE_PATH,
    "silver_customers_all": lambda: config.silver_path("customers"),
    "silver_products_all": lambda: config.silver_path("products"),
    "silver_orders_all": lambda: config.silver_path("orders"),
    "dim_customers_scd2": lambda: config.SCD2_CUSTOMERS_PATH,
    "quarantine": lambda: config.QUARANTINE_PATH,
}

# Friendly current-state views: live rows only, bookkeeping columns hidden.
CURRENT_VIEWS = {
    "customers": "silver_customers_all",
    "products": "silver_products_all",
    "orders": "silver_orders_all",
}


def _is_delta(path: str) -> bool:
    return "://" in path or (Path(path) / "_delta_log").is_dir()


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; SET TimeZone = 'UTC';")
    for view, path_fn in RAW_VIEWS.items():
        path = path_fn()
        if _is_delta(path):
            con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM delta_scan('{path}')")
    existing = {r[0] for r in con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()}
    for view, src in CURRENT_VIEWS.items():
        if src in existing:
            con.execute(
                f"CREATE OR REPLACE VIEW {view} AS SELECT * EXCLUDE (_op, _is_deleted, _processed_at) "
                f"FROM {src} WHERE NOT _is_deleted"
            )
    return con


def set_vars(con: duckdb.DuckDBPyConnection, pairs: list[str]) -> None:
    for pair in pairs:
        name, _, value = pair.partition("=")
        con.execute(f"SET VARIABLE {name} = ?", [value])


def default_vars(con: duckdb.DuckDBPyConnection) -> None:
    """Pick an interesting customer (most SCD2 versions) and a moment mid-history.

    Done entirely in SQL so no timezone-aware value has to cross into Python.
    """
    con.execute("""
        SET VARIABLE customer_id = (
            SELECT customer_id FROM dim_customers_scd2 GROUP BY customer_id
            ORDER BY count(*) DESC, customer_id LIMIT 1)
    """)
    con.execute("""
        SET VARIABLE as_of = (
            SELECT min(valid_from) + (max(valid_from) - min(valid_from)) / 2
            FROM dim_customers_scd2 WHERE customer_id = getvariable('customer_id'))
    """)


def run_sql(con: duckdb.DuckDBPyConnection, sql: str, title: str | None = None) -> None:
    statements = [s for s in sql.split(";") if s.strip() and not all(
        line.strip().startswith("--") or not line.strip() for line in s.splitlines())]
    for stmt in statements:
        if title:
            print(f"\n=== {title} ===")
        rel = con.sql(stmt)
        if rel is not None:
            rel.show(max_rows=25, max_width=160)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*", help="SQL files (default: queries/*.sql)")
    ap.add_argument("--sql", help="inline SQL to run instead of files")
    ap.add_argument("--var", action="append", default=[], help="name=value for getvariable('name')")
    args = ap.parse_args(argv)

    con = connect()
    try:
        default_vars(con)
    except duckdb.Error as exc:  # e.g. SCD2 table not created yet
        print(f"(no default variables: {exc})", file=sys.stderr)
    set_vars(con, args.var)
    if args.sql:
        run_sql(con, args.sql)
        return
    files = [Path(f) for f in args.files] or sorted((REPO / "queries").glob("*.sql"))
    for f in files:
        run_sql(con, f.read_text(), title=f.name)


if __name__ == "__main__":
    main(sys.argv[1:])
