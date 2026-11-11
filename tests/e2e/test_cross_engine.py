"""Trino and DuckDB read the same Delta tables and must agree exactly.

Needs the stack (`make up`, which includes Trino) and the pipeline to have run.
Run while the pipeline is idle, since both engines must read the same snapshot.
"""
import os

import pytest

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.environ.get("E2E") != "1", reason="set E2E=1 with the stack running"),
]


def test_trino_and_duckdb_agree():
    from lakehouse.reconcile import reconcile
    from lakehouse.trino_query import cross_engine_check

    report, _ = reconcile(timeout_s=300)   # waits until the lake has caught up
    assert report.ok, report.failures
    results = cross_engine_check()
    diffs = {k: v for k, v in results.items() if str(v[0]) != str(v[1])}
    assert not diffs, f"engines disagree: {diffs}"
    assert results["live orders"][0] > 0
