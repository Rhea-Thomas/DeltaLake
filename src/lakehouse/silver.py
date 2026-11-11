"""Silver current-state tables: one row per source primary key.

Ordering is by the source LSN (Postgres WAL position), never by arrival time:

* Within a micro-batch, only the change with the highest LSN per key is kept.
* Across micro-batches, the MERGE only overwrites a row when the incoming LSN is
  strictly greater than the stored one, so a late (out-of-order) or replayed
  (duplicate) event can never roll a row back.

Deletes are applied as soft deletes (``_is_deleted = true``) instead of removing
the row. Keeping the key and its LSN is what lets a late, older event for a
deleted key be recognised as stale; a hard delete would forget the key and let
the stale event resurrect it. Consumers read ``WHERE NOT _is_deleted``.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .delta_utils import ensure_table
from .tables import TableSpec


def latest_per_key(changes: DataFrame, pk: str) -> DataFrame:
    w = Window.partitionBy(pk).orderBy(F.col("_lsn").desc(), F.col("_offset").desc())
    return changes.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def to_current_state_rows(typed_changes: DataFrame, spec: TableSpec) -> DataFrame:
    latest = latest_per_key(typed_changes, spec.pk)
    return latest.select(
        *spec.column_names,
        F.col("_op"),
        F.col("_lsn"),
        F.col("_source_ts"),
        (F.col("_op") == "d").alias("_is_deleted"),
        F.current_timestamp().alias("_processed_at"),
    )


def upsert_current_state(spark: SparkSession, typed_changes: DataFrame, path: str, spec: TableSpec) -> None:
    rows = to_current_state_rows(typed_changes, spec)
    target = ensure_table(spark, path, rows.schema)
    (
        target.alias("t")
        .merge(rows.alias("s"), f"t.{spec.pk} = s.{spec.pk}")
        .whenMatchedUpdateAll(condition="s._lsn > t._lsn")
        .whenNotMatchedInsertAll()
        .execute()
    )
