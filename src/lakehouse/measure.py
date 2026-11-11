"""Benchmark the running pipeline and write reports/RESULTS.md + results.json.

Requires: `make up` and the pipeline running (`make stream`).

Phases
  1. steady   - generator at a fixed rate; per-event latency from Postgres
                commit (Debezium source.ts_ms) to "visible in silver + SCD2"
                (wall clock after the micro-batch's last MERGE commits).
  2. burst    - generator unthrottled; sustained end-to-end throughput.
  3. bulk     - one transaction updating every order; how fast a large single
                change set flows through.
  then        - reconciliation against Postgres and row counts per layer.

Latency mixes two clocks (Postgres container vs host); the measured skew is
reported alongside so the reader can judge it.
"""
from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
import time
from pathlib import Path

import psycopg

from . import config
from .query import connect
from .reconcile import SOURCE_DSN, reconcile

REPORTS = config.REPO_ROOT / "reports"


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 1)


def summarize(values: list[float]) -> dict:
    return {"n": len(values), "p50": pct(values, 50), "p95": pct(values, 95),
            "p99": pct(values, 99), "max": max(values) if values else None,
            "mean": round(sum(values) / len(values), 1) if values else None}


def batches_since(since_ms: int) -> list[dict]:
    path = Path(config.METRICS_PATH)
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        b = json.loads(line)
        if b["committed_ms"] >= since_ms:
            out.append(b)
    return out


def clock_skew_ms() -> float:
    with psycopg.connect(SOURCE_DSN) as pg:
        t0 = time.time()
        db = pg.execute("SELECT extract(epoch FROM clock_timestamp())::float8").fetchone()[0]
        t1 = time.time()
    return round((db - (t0 + t1) / 2) * 1000, 1)


def run_generator(rate: float, duration: float, seed: int) -> str:
    cmd = [sys.executable, "-m", "generator.generate", "--rate", str(rate),
           "--duration", str(duration), "--seed", str(seed)]
    res = subprocess.run(cmd, cwd=config.REPO_ROOT, check=True, capture_output=True, text=True)
    return res.stdout.strip()


def wait_idle(since_ms: int, quiet_s: float = 8, timeout_s: float = 600) -> None:
    """Wait until no new non-empty batch has committed for ``quiet_s`` seconds."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        bs = batches_since(since_ms)
        last = max((b["committed_ms"] for b in bs), default=since_ms)
        if time.time() * 1000 - last > quiet_s * 1000:
            return
        time.sleep(1)
    raise TimeoutError("pipeline did not go idle")


def phase_steady(rate: float, duration: float) -> dict:
    wait_idle(int(time.time() * 1000) - 60_000)
    t0 = int(time.time() * 1000)
    gen = run_generator(rate, duration, seed=11)
    wait_idle(t0)
    bs = batches_since(t0)
    return {
        "rate_actions_per_s": rate, "duration_s": duration, "generator": gen,
        "batches": len(bs),
        "events": sum(len(b["e2e_ms"]) for b in bs),
        "e2e_latency_ms": summarize([x for b in bs for x in b["e2e_ms"]]),
        "pg_to_kafka_ms": summarize([x for b in bs for x in b["to_kafka_ms"]]),
        "pg_to_bronze_batch_start_ms": summarize([x for b in bs for x in b["to_bronze_ms"]]),
        "silver_batch_duration_s": summarize([b["duration_s"] for b in bs]),
    }


def phase_burst(duration: float) -> dict:
    t0 = int(time.time() * 1000)
    gen = run_generator(0, duration, seed=22)
    gen_done = int(time.time() * 1000)
    wait_idle(t0)
    bs = batches_since(t0)
    events = sum(b["n_events"] for b in bs)
    last_commit = max(b["committed_ms"] for b in bs)
    busy = sum(b["duration_s"] for b in bs)
    return {
        "duration_s": duration, "generator": gen, "events": events, "batches": len(bs),
        "source_events_per_s": round(events / ((gen_done - t0) / 1000), 1),
        "end_to_end_events_per_s": round(events / ((last_commit - t0) / 1000), 1),
        "catch_up_after_generator_stopped_s": round((last_commit - gen_done) / 1000, 1),
        "silver_processing_events_per_busy_s": round(events / busy, 1) if busy else None,
        "e2e_latency_ms": summarize([x for b in bs for x in b["e2e_ms"]]),
    }


def phase_bulk() -> dict:
    t0 = int(time.time() * 1000)
    with psycopg.connect(SOURCE_DSN, autocommit=True) as pg:
        n = pg.execute("UPDATE orders SET updated_at = now()").rowcount
    committed = int(time.time() * 1000)
    wait_idle(t0)
    bs = batches_since(t0)
    lat = [x for b in bs for x in b["e2e_ms"]]
    last = max(b["committed_ms"] for b in bs)
    return {
        "rows_updated_in_one_transaction": n, "batches": len(bs),
        "events_seen": sum(b["n_events"] for b in bs),
        "commit_to_fully_visible_s": round((last - committed) / 1000, 1),
        "events_per_s": round(n / ((last - committed) / 1000), 1),
        "e2e_latency_ms": summarize(lat),
    }


def layer_counts() -> dict:
    con = connect()
    rows = con.execute((config.REPO_ROOT / "queries" / "01_layer_row_counts.sql").read_text()).fetchall()
    counts = {name: n for name, n in rows}
    counts["bronze by table/op"] = {f"{t}/{op}": n for t, op, n in con.execute(
        "SELECT source_table, op, count(*) FROM bronze_cdc_events GROUP BY ALL ORDER BY 1, 2").fetchall()}
    return counts


def write_markdown(res: dict) -> None:
    s, b, k = res["steady"], res["burst"], res["bulk"]
    L = s["e2e_latency_ms"]
    lines = [
        "# Measured results",
        "",
        f"Run at {res['run_at']} on {res['machine']}. Clock skew Postgres container vs host: "
        f"{res['clock_skew_ms']} ms (positive = container ahead).",
        "",
        "## End-to-end latency (Postgres commit -> visible in silver and SCD2)",
        "",
        f"Steady load: {s['rate_actions_per_s']:.0f} generator actions/s for {s['duration_s']:.0f}s, "
        f"{s['events']} change events in {s['batches']} silver micro-batches.",
        "",
        "| stage | p50 | p95 | p99 | max |",
        "|---|---|---|---|---|",
    ]
    for label, key in [("Postgres -> Kafka (Debezium)", "pg_to_kafka_ms"),
                       ("Postgres -> bronze micro-batch start", "pg_to_bronze_batch_start_ms"),
                       ("**Postgres -> silver + SCD2 committed**", "e2e_latency_ms")]:
        v = s[key]
        lines.append(f"| {label} | {v['p50']} ms | {v['p95']} ms | {v['p99']} ms | {v['max']} ms |")
    lines += [
        "",
        f"Silver micro-batch duration: p50 {s['silver_batch_duration_s']['p50']} s, "
        f"p95 {s['silver_batch_duration_s']['p95']} s.",
        "",
        "## Throughput",
        "",
        f"* Unthrottled generator for {b['duration_s']:.0f}s produced {b['events']} events "
        f"({b['source_events_per_s']} events/s at the source); the pipeline caught up "
        f"{b['catch_up_after_generator_stopped_s']} s after the generator stopped. "
        f"End-to-end: **{b['end_to_end_events_per_s']} events/s**. Latency under this load: "
        f"p50 {b['e2e_latency_ms']['p50']} ms, p95 {b['e2e_latency_ms']['p95']} ms.",
        f"* One transaction updating all {k['rows_updated_in_one_transaction']} orders: fully visible in "
        f"silver {k['commit_to_fully_visible_s']} s after commit (**{k['events_per_s']} events/s**), "
        f"in {k['batches']} micro-batch(es).",
        "",
        "## Row counts per layer (after the run)",
        "",
        "| table | rows |",
        "|---|---|",
    ]
    for name, n in res["layer_counts"].items():
        if isinstance(n, dict):
            continue
        lines.append(f"| {name} | {n} |")
    lines += ["", "Bronze events by table/op: " + ", ".join(
        f"{k2}={v2}" for k2, v2 in res["layer_counts"]["bronze by table/op"].items()), ""]
    rec = res["reconciliation"]
    lines += ["## Reconciliation", "",
              ("PASSED" if rec["ok"] else "FAILED") + f" after {rec['seconds']:.0f}s of polling.", "",
              "```json", json.dumps(rec["facts"], indent=2, default=str), "```"]
    if rec["failures"]:
        lines += ["", "Failures:", *[f"* {f}" for f in rec["failures"]]]
    (REPORTS / "RESULTS.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=100)
    ap.add_argument("--steady-seconds", type=float, default=120)
    ap.add_argument("--burst-seconds", type=float, default=60)
    args = ap.parse_args()

    REPORTS.mkdir(exist_ok=True)
    res = {
        "run_at": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        "machine": f"{platform.machine()} {platform.system()} {platform.release()}",
        "clock_skew_ms": clock_skew_ms(),
    }
    print("phase 1: steady load...", flush=True)
    res["steady"] = phase_steady(args.rate, args.steady_seconds)
    print(json.dumps(res["steady"]["e2e_latency_ms"]), flush=True)
    print("phase 2: burst...", flush=True)
    res["burst"] = phase_burst(args.burst_seconds)
    print(res["burst"]["end_to_end_events_per_s"], "events/s", flush=True)
    print("phase 3: bulk transaction...", flush=True)
    res["bulk"] = phase_bulk()
    print("reconciling...", flush=True)
    report, secs = reconcile(timeout_s=300)
    res["reconciliation"] = {"ok": report.ok, "seconds": secs, "facts": report.facts, "failures": report.failures}
    res["layer_counts"] = layer_counts()
    (REPORTS / "results.json").write_text(json.dumps(res, indent=2, default=str))
    write_markdown(res)
    print((REPORTS / "RESULTS.md").read_text())


if __name__ == "__main__":
    main()
