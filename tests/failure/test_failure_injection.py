"""Failure-injection tests: crash parts of the pipeline under live load and
verify it recovers cleanly, meaning the lake reconciles EXACTLY with Postgres
afterwards (no lost change, no duplicate, nothing invalid in silver, SCD2
consistent). See lakehouse.reconcile for the precise checks.

Needs the Docker stack (`make up`); each test starts and stops its own pipeline
process. Stop any `make stream` you have running first.

    make failure            # all scenarios, ~15 minutes; writes reports/FAILURE_TESTS.md
"""
from __future__ import annotations

import json
import os
import time

import pytest

from lakehouse import config
from lakehouse.reconcile import reconcile

from .harness import (Pipeline, compose, connector_state, generator, wait_connector_running,
                      wait_healthy)

pytestmark = [
    pytest.mark.failure,
    pytest.mark.skipif(os.environ.get("FAILURE_TESTS") != "1",
                       reason="set FAILURE_TESTS=1 with the Docker stack running"),
]

RESULTS: list[dict] = []


@pytest.fixture(scope="module", autouse=True)
def write_report():
    yield
    if not RESULTS:
        return
    out = config.REPO_ROOT / "reports"
    out.mkdir(exist_ok=True)
    (out / "failure_tests.json").write_text(json.dumps(RESULTS, indent=2, default=str))
    lines = ["# Failure-injection results", "",
             f"Run {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. Each scenario runs the workload "
             "generator, injects the fault, lets the system recover, then requires an exact "
             "reconciliation with Postgres.", "",
             "| scenario | fault injected | recovery | reconciled | events in lake | duplicate changes absorbed |",
             "|---|---|---|---|---|---|"]
    for r in RESULTS:
        lines.append(f"| {r['scenario']} | {r['fault']} | {r['recovery']} | "
                     f"{'yes' if r['ok'] else 'NO'} ({r['reconcile_s']:.0f}s after load stopped) | "
                     f"{r['bronze_rows']} | {r['duplicates']} |")
    failures = [r for r in RESULTS if not r["ok"]]
    if failures:
        lines += ["", "## Failures", *[f"* {r['scenario']}: {r['failures']}" for r in failures]]
    (out / "FAILURE_TESTS.md").write_text("\n".join(lines) + "\n")


def _finish(name: str, fault: str, recovery: str, gen) -> None:
    gen_out, _ = gen.communicate(timeout=600)
    assert gen.returncode == 0, gen_out
    report, secs = reconcile(timeout_s=300)
    RESULTS.append({
        "scenario": name, "fault": fault, "recovery": recovery, "ok": report.ok,
        "reconcile_s": secs, "bronze_rows": report.facts.get("bronze_rows"),
        "duplicates": report.facts.get("duplicate_change_events_absorbed"),
        "failures": report.failures, "facts": report.facts,
    })
    assert report.ok, "\n".join(report.failures)


@pytest.fixture
def pipeline(request):
    p = Pipeline(request.node.name)
    yield p
    p.stop()


def test_spark_driver_killed_mid_stream(pipeline):
    """kill -9 the whole Spark application while it is ingesting, then restart."""
    pipeline.start()
    pipeline.wait_until_started()
    gen = generator(60, seed=101)
    time.sleep(20)
    pipeline.kill9()
    time.sleep(10)                      # writes keep landing in Postgres/Kafka meanwhile
    t = time.time()
    pipeline.start()
    pipeline.wait_until_processing()
    _finish("Spark driver killed", "SIGKILL of the Spark app mid-stream, down 10 s",
            f"restarted from checkpoints, first batch after {time.time() - t:.0f}s", gen)


@pytest.mark.parametrize("point", ["before_scd2", "after_silver", "after_quarantine"])
def test_crash_inside_a_silver_batch(pipeline, point):
    """Hard crash at a precise point inside a micro-batch, after some of its
    writes have committed but before Spark marks the batch done. Spark replays
    the same batch id on restart; idempotent writes must make that harmless."""
    pipeline.start()
    pipeline.wait_until_started()
    gen = generator(45, seed=200 + len(point))
    time.sleep(5)
    pipeline.stop()
    pipeline.start(FAULT_INJECT=f"{point}@0")
    code = pipeline.wait_exit(timeout=300)
    assert code == 137, f"expected injected crash, got exit {code}"
    assert f"[fault] crashing at {point}" in pipeline.log_text()
    pipeline.start()
    pipeline.wait_until_processing()
    _finish(f"crash {point.replace('_', ' ')}", f"driver exits at `{point}` inside a batch",
            "same batch id replayed on restart", gen)


def test_kafka_broker_killed(pipeline):
    """SIGKILL the Kafka broker for 25 s under load."""
    pipeline.start()
    pipeline.wait_until_started()
    gen = generator(70, seed=303)
    time.sleep(15)
    compose("kill", "kafka")
    time.sleep(25)
    compose("start", "kafka")
    wait_healthy("kafka")
    waited = wait_connector_running()
    log_has_restart = "[supervisor]" in pipeline.log_text()
    _finish("Kafka broker killed", "SIGKILL of the broker, down 25 s",
            f"broker restarted; Debezium RUNNING after {waited:.0f}s; "
            f"Spark {'query restarted by supervisor' if log_has_restart else 'Kafka source retried in place'}",
            gen)


def test_kafka_connect_killed(pipeline):
    """SIGKILL Kafka Connect (Debezium) under load: it resumes from its stored
    offsets and may re-publish changes, which must be absorbed as duplicates."""
    pipeline.start()
    pipeline.wait_until_started()
    gen = generator(60, seed=404)
    time.sleep(15)
    compose("kill", "connect")
    time.sleep(15)
    compose("start", "connect")
    wait_healthy("connect")
    waited = wait_connector_running()
    _finish("Kafka Connect killed", "SIGKILL of Connect/Debezium, down 15 s",
            f"connector resumed from stored offsets, RUNNING after {waited:.0f}s", gen)


def test_postgres_crash(pipeline):
    """SIGKILL Postgres (crash recovery on restart). The replication slot is
    durable, so Debezium must reconnect and continue from where it was."""
    pipeline.start()
    pipeline.wait_until_started()
    gen = generator(20, seed=505)
    gen.communicate(timeout=120)
    compose("kill", "postgres")
    time.sleep(10)
    compose("start", "postgres")
    wait_healthy("postgres")
    waited = wait_connector_running(timeout=300)
    gen = generator(30, seed=506)
    _finish("Postgres crashed", "SIGKILL of Postgres between two load phases, down 10 s",
            f"crash recovery; Debezium state {connector_state()} after {waited:.0f}s", gen)
