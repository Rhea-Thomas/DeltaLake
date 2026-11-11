"""Helpers for failure-injection tests: drive the pipeline process, the
workload generator and the Docker services, then reconcile."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from lakehouse import config

REPO = config.REPO_ROOT
LOGS = Path(config.LAKE_ROOT) / "failure_logs"
COMPOSE = ["docker", "compose", "-f", str(REPO / "docker-compose.yml")]


def _env(**extra) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO / "src")
    env.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
    env.pop("FAULT_INJECT", None)
    env.update({k: str(v) for k, v in extra.items()})
    return env


def metrics_lines() -> int:
    p = Path(config.METRICS_PATH)
    return len(p.read_text().splitlines()) if p.exists() else 0


class Pipeline:
    def __init__(self, name: str):
        LOGS.mkdir(parents=True, exist_ok=True)
        self.log_path = LOGS / f"{name}.log"
        self.proc: subprocess.Popen | None = None
        self._starts_seen = self.log_path.read_text().count("[pipeline] queries started") if self.log_path.exists() else 0

    def start(self, **env) -> None:
        log = open(self.log_path, "a")
        log.write(f"\n===== start {time.strftime('%H:%M:%S')} env={env} =====\n")
        log.flush()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "lakehouse.jobs.pipeline"], cwd=REPO,
            env=_env(**env), stdout=log, stderr=subprocess.STDOUT,
        )

    def wait_until_started(self, timeout: float = 180) -> None:
        """Wait until this process has started both streaming queries."""
        marker = "[pipeline] queries started"
        before = self._starts_seen
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.log_text().count(marker) > before:
                self._starts_seen = self.log_text().count(marker)
                return
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(f"pipeline exited with {self.proc.returncode}; see {self.log_path}")
            time.sleep(1)
        raise TimeoutError("pipeline did not start its queries")

    def wait_until_processing(self, timeout: float = 180) -> None:
        """Wait for the next committed non-empty silver batch (needs load running)."""
        start_n = metrics_lines()
        deadline = time.time() + timeout
        while time.time() < deadline:
            if metrics_lines() > start_n:
                return
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(f"pipeline exited with {self.proc.returncode}; see {self.log_path}")
            time.sleep(1)
        raise TimeoutError("pipeline produced no batch")

    def kill9(self) -> None:
        assert self.proc
        os.kill(self.proc.pid, signal.SIGKILL)
        self.proc.wait()

    def wait_exit(self, timeout: float = 300) -> int:
        assert self.proc
        return self.proc.wait(timeout=timeout)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def log_text(self) -> str:
        return self.log_path.read_text()


def generator(seconds: float, rate: float = 80, seed: int = 1) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "generator.generate", "--rate", str(rate),
         "--duration", str(seconds), "--seed", str(seed)],
        cwd=REPO, env=_env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def compose(*args: str, check: bool = True) -> str:
    return subprocess.run([*COMPOSE, *args], check=check, capture_output=True, text=True).stdout


def wait_healthy(service: str, timeout: float = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = compose("ps", "--format", "json", service, check=False)
        rows = [json.loads(l) for l in out.splitlines() if l.strip()]
        if rows and rows[0].get("Health") == "healthy":
            return
        time.sleep(2)
    raise TimeoutError(f"{service} not healthy")


def connector_state() -> str:
    out = subprocess.run(["curl", "-fsS", "http://localhost:8083/connectors/pg-source/status"],
                         capture_output=True, text=True)
    if out.returncode:
        return "UNREACHABLE"
    s = json.loads(out.stdout)
    tasks = s.get("tasks", [])
    return f"{s['connector']['state']}/{tasks[0]['state'] if tasks else 'NONE'}"


def wait_connector_running(timeout: float = 180) -> float:
    """Wait for Debezium to report RUNNING on its own; return seconds waited."""
    start = time.time()
    while time.time() - start < timeout:
        if connector_state() == "RUNNING/RUNNING":
            return time.time() - start
        time.sleep(2)
    raise TimeoutError(f"connector did not recover by itself: {connector_state()}")


def bronze_offsets_written() -> int:
    """Rows in bronze, read through DuckDB (cheap progress signal)."""
    from lakehouse.query import connect
    return connect().execute("SELECT count(*) FROM bronze_cdc_events").fetchone()[0]
