"""End-to-end: after the generator runs, the Delta tables reconcile exactly with
the source Postgres (see lakehouse.reconcile for the precise definition).

Needs the Docker stack (`make up`) and the pipeline (`make stream`) running:
    make e2e
"""
import os
import subprocess
import sys

import pytest

from lakehouse import config
from lakehouse.reconcile import reconcile

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.environ.get("E2E") != "1", reason="set E2E=1 with the stack running"),
]


def test_delta_tables_reconcile_with_postgres():
    subprocess.run(
        [sys.executable, "-m", "generator.generate", "--rate", "80", "--duration", "45", "--seed", "7"],
        cwd=config.REPO_ROOT, check=True,
    )
    report, seconds = reconcile(timeout_s=300)
    print(f"\nreconciliation facts after {seconds:.0f}s: {report.facts}")
    assert report.ok, "\n".join(report.failures)
    for table in ("customers", "products", "orders"):
        assert report.facts[table]["matched_exactly"] > 0
