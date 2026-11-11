"""Helpers that fabricate bronze rows exactly as the bronze job would write them."""
from __future__ import annotations

import datetime as dt
import json

from pyspark.sql import types as T

from lakehouse.tables import TABLES

BRONZE_SCHEMA = T.StructType([
    T.StructField("topic", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("kafka_ts", T.TimestampType()),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
    T.StructField("source_table", T.StringType()),
    T.StructField("op", T.StringType()),
    T.StructField("lsn", T.LongType()),
    T.StructField("source_ts_ms", T.LongType()),
    T.StructField("ingested_at", T.TimestampType()),
])

BASE_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
_offset = [0]


def ts_ms(minutes: float) -> int:
    return BASE_MS + int(minutes * 60_000)


def ts(minutes: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts_ms(minutes) / 1000, tz=dt.timezone.utc).replace(tzinfo=None)


def customer(cid, email=None, full_name="Ava Chen", country="US", tier="free", **kw):
    row = {"customer_id": cid, "email": email if email is not None else f"c{cid}@example.com",
           "full_name": full_name, "country": country, "tier": tier,
           "created_at": "2026-01-01T00:00:00.000000Z", "updated_at": "2026-01-01T00:00:00.000000Z"}
    row.update(kw)
    return row


def event(table: str, op: str, lsn: int, row: dict, minutes: float | None = None, offset: int | None = None):
    """One bronze row. ``minutes`` = commit time offset from BASE (default: lsn/10)."""
    minutes = lsn / 10 if minutes is None else minutes
    if offset is None:
        _offset[0] += 1
        offset = _offset[0]
    pk = TABLES[table].pk
    payload = {
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "source": {"lsn": lsn, "ts_ms": ts_ms(minutes), "table": table, "txId": lsn},
        "op": op,
        "ts_ms": ts_ms(minutes) + 50,
    }
    return (f"src.public.{table}", 0, offset, ts(minutes), json.dumps({pk: row.get(pk)}),
            json.dumps(payload), table, op, lsn, ts_ms(minutes), ts(minutes))


def bronze_df(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)
