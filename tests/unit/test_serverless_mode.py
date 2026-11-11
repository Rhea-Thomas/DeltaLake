"""Options used on serverless compute (Databricks Free Edition)."""
import datetime as dt
import json

import pytest

from lakehouse import config
from lakehouse.cdc import KAFKA_RECORD_SCHEMA, files_to_kafka_shape, kafka_to_bronze


def test_trigger_modes(monkeypatch):
    monkeypatch.setattr(config, "TRIGGER_MODE", "processing_time")
    assert config.trigger_kwargs("2 seconds") == {"processingTime": "2 seconds"}
    monkeypatch.setattr(config, "TRIGGER_MODE", "available_now")
    assert config.trigger_kwargs("2 seconds") == {"availableNow": True}
    monkeypatch.setattr(config, "TRIGGER_MODE", "sometimes")
    with pytest.raises(ValueError):
        config.trigger_kwargs("1 second")


def test_exported_file_record_becomes_the_same_bronze_row(spark):
    payload = {"before": None, "after": {"order_id": 3}, "op": "c",
               "source": {"lsn": 99, "ts_ms": 1767225600000, "table": "orders"}}
    record = ("src.public.orders", 2, 41, "2026-01-01T00:00:00.123000Z", '{"order_id":3}', json.dumps(payload))
    row = kafka_to_bronze(files_to_kafka_shape(spark.createDataFrame([record], KAFKA_RECORD_SCHEMA))).collect()[0]
    assert (row.topic, row.partition, row.offset, row.source_table, row.op, row.lsn) == (
        "src.public.orders", 2, 41, "orders", "c", 99)
    assert row.kafka_ts == dt.datetime(2026, 1, 1, 0, 0, 0, 123000)
    assert row.key == '{"order_id":3}' and json.loads(row.value) == payload
    assert row.event_date == dt.date(2026, 1, 1)
