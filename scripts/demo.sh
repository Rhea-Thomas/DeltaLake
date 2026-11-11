#!/usr/bin/env bash
# Full demo: start the pipeline in the background, run the workload, wait for the
# lake to reconcile with Postgres, then print the example queries.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=src
PY=.venv/bin/python
mkdir -p lake
$PY -m lakehouse.jobs.pipeline > lake/pipeline.log 2>&1 &
PIPE=$!
trap 'kill $PIPE 2>/dev/null || true' EXIT
echo "pipeline pid $PIPE (log: lake/pipeline.log)"
$PY -m generator.generate --rate "${RATE:-50}" --duration "${DURATION:-60}"
$PY -m lakehouse.reconcile --timeout 300
$PY -m lakehouse.query
