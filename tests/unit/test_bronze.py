import datetime as dt
import json

from pyspark.sql import types as T

from lakehouse.cdc import kafka_to_bronze


def test_bronze_projection_promotes_fields_and_event_date(spark):
    payload = {"before": None, "after": {"customer_id": 1}, "op": "c",
               "source": {"lsn": 42, "ts_ms": 1767311999000, "table": "customers"}}   # 2026-01-01T23:59:59Z
    schema = T.StructType([T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()),
                           T.StructField("offset", T.LongType()), T.StructField("timestamp", T.TimestampType()),
                           T.StructField("key", T.BinaryType()), T.StructField("value", T.BinaryType())])
    kafka = spark.createDataFrame([("src.public.customers", 0, 7, dt.datetime(2026, 1, 2),
                                    b'{"customer_id":1}', json.dumps(payload).encode())], schema)
    row = kafka_to_bronze(kafka).collect()[0]
    assert (row.source_table, row.op, row.lsn, row.offset) == ("customers", "c", 42, 7)
    assert row.event_date == dt.date(2026, 1, 1)   # event time (source commit), not arrival time
    assert json.loads(row.value) == payload         # raw payload kept verbatim
