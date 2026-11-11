"""Debezium change-event handling.

Debezium (JSON converter, schemas disabled) writes one Kafka message per row
change with this envelope::

    {"before": {...} | null, "after": {...} | null,
     "source": {"lsn": 12345, "ts_ms": 1731234567890, "txId": 789, "table": "customers", ...},
     "op": "c" | "u" | "d" | "r",   # create, update, delete, snapshot read
     "ts_ms": 1731234567999}       # when Debezium processed it

Bronze keeps the message verbatim plus a few promoted fields. Silver-side
parsing turns it into one flat row per change.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .tables import TableSpec

# Metadata columns carried on every parsed change row.
META_COLUMNS = ["_op", "_lsn", "_source_ts_ms", "_source_ts", "_topic", "_partition", "_offset", "_raw"]


# A Kafka record as exported to JSON files (see BRONZE_SOURCE=files).
KAFKA_RECORD_SCHEMA = T.StructType([
    T.StructField("topic", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("key", T.StringType()),
    T.StructField("value", T.StringType()),
])


def files_to_kafka_shape(files_df: DataFrame) -> DataFrame:
    """Exported JSON records -> the same columns the Kafka source produces."""
    return files_df.select(
        "topic", "partition", "offset",
        F.to_timestamp("timestamp").alias("timestamp"),
        "key", "value",
    )


def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> bronze rows. Raw payload is preserved untouched."""
    value = F.col("value").cast("string")
    return kafka_df.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key"),
        value.alias("value"),
        F.element_at(F.split(F.col("topic"), r"\."), -1).alias("source_table"),
        F.get_json_object(value, "$.op").alias("op"),
        F.get_json_object(value, "$.source.lsn").cast("long").alias("lsn"),
        F.get_json_object(value, "$.source.ts_ms").cast("long").alias("source_ts_ms"),
        F.current_timestamp().alias("ingested_at"),
        # Partition column: the source commit date (event time, not arrival),
        # so "changes on day X" prunes to one directory per table.
        F.to_date(F.expr("timestamp_millis(CAST(get_json_object(CAST(value AS STRING), '$.source.ts_ms') AS BIGINT))"))
        .alias("event_date"),
    )


def _image_schema(spec: TableSpec) -> T.StructType:
    # Every field is read as a string on purpose: type coercion is a data-quality
    # concern, so a value that cannot be cast is reported, not silently nulled.
    return T.StructType([T.StructField(c, T.StringType()) for c in spec.column_names])


def parse_changes(bronze_df: DataFrame, spec: TableSpec) -> DataFrame:
    """Bronze rows for one table -> one row per change, columns as raw strings.

    For deletes the row image comes from ``before`` (complete because the source
    tables use REPLICA IDENTITY FULL); for c/u/r it comes from ``after``.
    """
    image = F.when(F.col("op") == "d", F.get_json_object("value", "$.before")).otherwise(
        F.get_json_object("value", "$.after")
    )
    return (
        bronze_df.where(F.col("source_table") == spec.name)
        .select(
            F.col("op").alias("_op"),
            F.col("lsn").alias("_lsn"),
            F.col("source_ts_ms").alias("_source_ts_ms"),
            F.expr("timestamp_millis(source_ts_ms)").alias("_source_ts"),
            F.col("topic").alias("_topic"),
            F.col("partition").alias("_partition"),
            F.col("offset").alias("_offset"),
            F.col("value").alias("_raw"),
            F.from_json(image, _image_schema(spec)).alias("_img"),
        )
        .select(*META_COLUMNS, *[F.col(f"_img.{c}").alias(c) for c in spec.column_names])
    )


def typed_column(name: str, sql_type: str) -> Column:
    return F.col(name).cast(sql_type).alias(name)


def to_typed(changes: DataFrame, spec: TableSpec) -> DataFrame:
    """Cast raw string columns to their declared types (run only on validated rows)."""
    extra = [c for c in changes.columns if c not in spec.column_names]
    return changes.select(*[typed_column(c, t) for c, t in spec.columns], *extra)
