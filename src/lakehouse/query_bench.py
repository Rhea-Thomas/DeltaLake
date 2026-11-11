"""Query-time benchmark over the lake, on DuckDB and Trino.

    python -m lakehouse.query_bench --label before
    python -m lakehouse.maintenance optimize ...   (tuning)
    python -m lakehouse.query_bench --label after
    python -m lakehouse.query_bench --compare      # writes reports/PERFORMANCE.md

Each query runs once to warm up, then ``--runs`` times; the median wall time
is reported. The same SQL text runs before and after tuning, so only the
physical layout differs. Filter constants (a customer id, a time window) are
picked once and stored with the "before" results, then reused for "after".
Run with the pipeline stopped so nothing else competes for the CPU.
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from . import config

REPORTS = config.REPO_ROOT / "reports"
RAW = REPORTS / "query_bench.json"

# name -> (description, SQL with {params}); identical text on both engines
# except where the dialect differs (then a dict per engine).
QUERIES = {
    "bronze_scan": ("Full scan of bronze: events per table and op",
                    "SELECT source_table, op, count(*) FROM {bronze} GROUP BY source_table, op"),
    "bronze_window": ("One table, 30-minute event-time window (partition + file-stats pruning)",
                      "SELECT count(*), max(lsn) FROM {bronze} WHERE source_table = 'orders' "
                      "AND source_ts_ms BETWEEN {t_lo} AND {t_hi}"),
    "silver_orders_agg": ("Revenue by status over silver orders",
                          "SELECT status, count(*), sum(quantity * unit_price) FROM {orders} "
                          "WHERE NOT _is_deleted GROUP BY status"),
    "silver_orders_customer": ("All orders of one customer (Z-order on customer_id)",
                               "SELECT count(*), sum(quantity * unit_price) FROM {orders} "
                               "WHERE customer_id = {customer_id}"),
    "scd2_point_in_time": ("SCD2 history + as-of lookup for one customer (Z-order on customer_id)",
                           "SELECT count(*), max(valid_from) FROM {scd2} WHERE customer_id = {customer_id}"),
    "quarantine_reasons": ("Quarantine failures per table (many tiny append files before tuning)",
                           "SELECT source_table, count(*) FROM {quarantine} GROUP BY source_table"),
    "gold_star_join": ("Star join: revenue by month x tier-at-order x category",
                       "SELECT d.year, d.month, c.tier, p.category, sum(f.net_revenue) "
                       "FROM {fct} f JOIN {dim_date} d ON d.date_key = f.order_date_key "
                       "JOIN {dim_customer} c ON c.customer_sk = f.customer_sk "
                       "JOIN {dim_product} p ON p.product_sk = f.product_sk "
                       "GROUP BY d.year, d.month, c.tier, p.category"),
    "gold_one_day": ("Fact rows for one order date (partition pruning once partitioned)",
                     "SELECT count(*), sum(amount) FROM {fct} WHERE order_date_key = {date_key}"),
}

DUCK_TABLES = {
    "bronze": "delta_scan('{root}/bronze/cdc_events')",
    "orders": "delta_scan('{root}/silver/orders')",
    "scd2": "delta_scan('{root}/silver/dim_customers_scd2')",
    "quarantine": "delta_scan('{root}/quarantine/cdc_rejects')",
    "fct": "delta_scan('{root}/gold/fct_orders')",
    "dim_date": "delta_scan('{root}/gold/dim_date')",
    "dim_customer": "delta_scan('{root}/gold/dim_customer')",
    "dim_product": "delta_scan('{root}/gold/dim_product')",
}
TRINO_TABLES = {
    "bronze": "lake.lakehouse.bronze_cdc_events",
    "orders": "lake.lakehouse.silver_orders_all",
    "scd2": "lake.lakehouse.dim_customers_scd2",
    "quarantine": "lake.lakehouse.quarantine",
    "fct": "lake.gold.fct_orders",
    "dim_date": "lake.gold.dim_date",
    "dim_customer": "lake.gold.dim_customer",
    "dim_product": "lake.gold.dim_product",
}


def pick_params() -> dict:
    """Choose filter constants once, from the data (busiest customer, last 30 min)."""
    from .query import connect

    con = connect()
    t_hi = con.execute("SELECT max(source_ts_ms) FROM bronze_cdc_events WHERE source_table = 'orders'").fetchone()[0]
    cid = con.execute("SELECT customer_id FROM silver_orders_all GROUP BY 1 ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
    dk = con.execute(f"SELECT order_date_key FROM {DUCK_TABLES['fct'].format(root=config.LAKE_ROOT)} "
                     "GROUP BY 1 ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
    return {"t_lo": t_hi - 30 * 60 * 1000, "t_hi": t_hi, "customer_id": cid, "date_key": dk}


def _time(run, runs: int) -> tuple[float, object]:
    result = run()                      # warm-up (also the result we keep)
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        run()
        times.append(time.perf_counter() - t0)
    return statistics.median(times) * 1000, result


def bench_duckdb(params: dict, runs: int) -> dict:
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; SET TimeZone = 'UTC'")
    tables = {k: v.format(root=config.LAKE_ROOT) for k, v in DUCK_TABLES.items()}
    out = {}
    for name, (_, sql) in QUERIES.items():
        q = sql.format(**tables, **params)
        ms, res = _time(lambda: con.execute(q).fetchall(), runs)
        out[name] = {"ms": round(ms, 1), "result": str(sorted(res))[:120]}
    return out


def bench_trino(params: dict, runs: int) -> dict:
    from .trino_query import connect, register

    conn = connect()
    register(conn)
    cur = conn.cursor()
    out = {}
    for name, (_, sql) in QUERIES.items():
        q = sql.format(**TRINO_TABLES, **params)

        def run():
            cur.execute(q)
            return cur.fetchall()
        ms, res = _time(run, runs)
        out[name] = {"ms": round(ms, 1), "result": str(sorted(res))[:120]}
    return out


def file_stats() -> dict:
    """Active files per table from the Delta logs (via DuckDB metadata, no Spark)."""
    out = {}
    for name, rel in [("bronze", "bronze/cdc_events"), ("silver.orders", "silver/orders"),
                      ("scd2", "silver/dim_customers_scd2"), ("quarantine", "quarantine/cdc_rejects"),
                      ("gold.fct_orders", "gold/fct_orders")]:
        path = Path(config.LAKE_ROOT) / rel
        if not (path / "_delta_log").is_dir():
            continue
        import duckdb
        n = duckdb.sql(f"INSTALL delta; LOAD delta; SELECT count(DISTINCT filename) FROM "
                       f"delta_scan('{path}', filename = true)").fetchone()[0]
        out[name] = n
    return out


def load_raw() -> dict:
    return json.loads(RAW.read_text()) if RAW.exists() else {}


def compare() -> None:
    raw = load_raw()
    b, a = raw["before"], raw["after"]
    lines = [
        "# Query performance: before vs after tuning", "",
        "Same SQL, same data, pipeline stopped; median of 5 runs after 1 warm-up. "
        f"Before: {b['at']}; after: {a['at']}.", "",
        "Tuning applied: " + a.get("tuning", "") , "",
        "## Active data files per table", "",
        "| table | before | after |", "|---|---|---|",
    ]
    for t in b["files"]:
        lines.append(f"| {t} | {b['files'][t]} | {a['files'].get(t, '-')} |")
    for engine in ("duckdb", "trino"):
        lines += ["", f"## {engine.capitalize()} (ms)", "",
                  "| query | what it does | before | after | speed-up |", "|---|---|---|---|---|"]
        for q, (desc, _) in QUERIES.items():
            tb, ta = b[engine][q]["ms"], a[engine][q]["ms"]
            same = b[engine][q]["result"] == a[engine][q]["result"]
            lines.append(f"| `{q}` | {desc} | {tb:.0f} | {ta:.0f} | "
                         f"**{tb / ta:.1f}x**{'' if same else ' (result differs!)'} |")
    (REPORTS / "PERFORMANCE.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", choices=["before", "after"])
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--tuning", default="", help="description of what was changed (stored with 'after')")
    ap.add_argument("--compare", action="store_true")
    args = ap.parse_args(argv)
    if args.compare:
        compare()
        return
    raw = load_raw()
    params = raw["before"]["params"] if args.label == "after" and "before" in raw else pick_params()
    res = {"at": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()), "params": params,
           "files": file_stats(), "tuning": args.tuning}
    print("files:", res["files"], flush=True)
    res["duckdb"] = bench_duckdb(params, args.runs)
    print("duckdb:", {k: v["ms"] for k, v in res["duckdb"].items()}, flush=True)
    res["trino"] = bench_trino(params, args.runs)
    print("trino:", {k: v["ms"] for k, v in res["trino"].items()}, flush=True)
    raw[args.label] = res
    REPORTS.mkdir(exist_ok=True)
    RAW.write_text(json.dumps(raw, indent=2, default=str))


if __name__ == "__main__":
    main()
