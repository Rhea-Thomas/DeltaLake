"""The silver micro-batch function as a whole: routes each table, quarantines
rejects, and is safe to replay with the same batch id (exactly-once effect)."""
import pytest

from lakehouse import config
from lakehouse.jobs import pipeline

from .events import bronze_df, customer, event


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LAKE_ROOT", str(tmp_path))
    monkeypatch.setattr(config, "QUARANTINE_PATH", str(tmp_path / "quarantine"))
    monkeypatch.setattr(config, "SCD2_CUSTOMERS_PATH", str(tmp_path / "scd2"))
    monkeypatch.setattr(config, "METRICS_PATH", str(tmp_path / "metrics.jsonl"))
    return tmp_path


def test_batch_routes_and_replay_is_idempotent(spark, lake):
    order = {"order_id": 5, "customer_id": 1, "product_id": 9, "quantity": 0, "unit_price": "2.00",
             "status": "placed", "ordered_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}
    batch = bronze_df(spark, [
        event("customers", "c", 10, customer(1)),
        event("customers", "c", 11, customer(2, email="broken")),
        event("customers", "u", 12, customer(1, tier="premium")),
        event("orders", "c", 13, order),
    ])
    pipeline.process_silver_batch(batch, 0)
    pipeline.process_silver_batch(batch, 0)   # crash-and-retry of the same batch id

    read = lambda p: spark.read.format("delta").load(str(p))
    silver = read(lake / "silver" / "customers").collect()
    assert [(r["customer_id"], r["tier"]) for r in silver] == [(1, "premium")]
    assert read(lake / "scd2").count() == 2
    q = read(lake / "quarantine").orderBy("lsn").collect()
    assert [(r["source_table"], r["record_key"]) for r in q] == [("customers", "2"), ("orders", "5")]
    assert q[1]["reason"] == "quantity_positive: quantity='0'"
