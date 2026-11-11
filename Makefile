# One-command setup: `make up` starts Postgres/Kafka/Debezium, creates the venv,
# seeds the source database and registers the CDC connector.
SHELL := /bin/bash
PY     := .venv/bin/python
export PYTHONPATH := src
export JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)
export PATH := $(JAVA_HOME)/bin:$(PATH)

.PHONY: help up venv infra seed connector stream generate query trino-query trino-check dbt dbt-venv reconcile measure test e2e failure demo down clean

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-11s %s\n", $$1, $$2}'

up: venv infra seed connector ## start infra, seed Postgres, register Debezium (one command)
	@echo "ready: run 'make stream' (pipeline) and 'make generate' (workload)"

venv: .venv/.installed ## create .venv with pinned deps (needs python3.11)
.venv/.installed: requirements.txt
	python3.11 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements.txt
	touch $@

infra: ## docker compose up and wait for health checks
	docker info > /dev/null
	docker compose up -d --wait

seed: ## create the source schema and seed rows
	$(PY) -m generator.generate --init

connector: ## register the Debezium Postgres connector
	./scripts/register_connector.sh

stream: ## run the Spark streaming pipeline (Ctrl-C to stop; resumes from checkpoints)
	$(PY) -m lakehouse.jobs.pipeline

generate: ## run the workload generator (RATE=50 DURATION=120)
	$(PY) -m generator.generate --rate $${RATE:-50} --duration $${DURATION:-120}

query: ## run the example DuckDB queries in queries/
	$(PY) -m lakehouse.query

trino-query: ## run the Trino queries in queries/trino/ (registers the Delta tables first)
	$(PY) -m lakehouse.trino_query

trino-check: ## same aggregates on Trino and DuckDB must match exactly
	$(PY) -m lakehouse.trino_query --check

dbt-venv: .venv-dbt/.installed
.venv-dbt/.installed: requirements-dbt.txt
	python3.11 -m venv .venv-dbt
	.venv-dbt/bin/pip install -q -r requirements-dbt.txt
	touch $@

dbt: dbt-venv ## build the gold star schema with dbt on Trino and run all dbt tests
	$(PY) -m lakehouse.trino_query --register
	cd dbt && ../.venv-dbt/bin/dbt build --profiles-dir .

reconcile: ## check Delta tables against source Postgres
	$(PY) -m lakehouse.reconcile

measure: ## benchmark latency/throughput and write reports/RESULTS.md
	$(PY) -m lakehouse.measure

test: ## unit tests (no Docker needed)
	$(PY) -m pytest tests/unit -q

e2e: ## end-to-end reconciliation test (needs `make up` + `make stream` running)
	E2E=1 $(PY) -m pytest tests/e2e -q -s

failure: ## failure-injection tests (needs `make up`; stop `make stream` first) -> reports/FAILURE_TESTS.md
	FAILURE_TESTS=1 $(PY) -m pytest tests/failure -v -s

demo: up ## everything: infra, pipeline in background, workload, reconcile, queries
	./scripts/demo.sh

down: ## stop containers and delete their volumes
	docker compose down -v

clean: down ## also delete the local lake (Delta tables + checkpoints)
	rm -rf ./lake
