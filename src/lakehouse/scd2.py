"""SCD Type 2 history for a dimension (used for customers).

Every validated change becomes a *version* row identified by ``(key, _lsn)``.
Each version is valid from its source commit time until the next version's
commit time::

    customer_id | tier     | valid_from | valid_to   | is_current | is_deleted | _lsn
    42          | free     | 2026-01-01 | 2026-03-05 | false      | false      | 100
    42          | premium  | 2026-03-05 | 2026-06-10 | false      | false      | 250
    42          | premium  | 2026-06-10 | NULL       | false      | true       | 410   <- delete

How the hard cases are handled:

* **Duplicates** (Kafka/Debezium redelivery): a version is keyed by
  ``(key, _lsn)``, so a second copy of the same change is a no-op.
* **Out-of-order events**: rather than "close the current row and append",
  the batch rebuilds the full version chain for every key it touches: existing
  versions UNION new versions, sorted by LSN, with ``valid_to`` / ``is_current``
  recomputed using ``lead()``. A late event therefore slots into the middle of
  the history and its neighbours' intervals are corrected.
* **Deletes**: a delete adds a tombstone version (``is_deleted = true``,
  attributes from the before-image) that closes the previous version. A deleted
  key has no ``is_current`` row.

The MERGE inserts new versions and updates only the bookkeeping columns of
existing versions whose interval changed, so re-running a batch is idempotent.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .delta_utils import ensure_table
from .tables import TableSpec

VERSION_META = ["valid_from", "is_deleted", "_lsn"]
HISTORY_META = ["valid_to", "is_current", "_end_lsn"]


def changes_to_versions(typed_changes: DataFrame, spec: TableSpec) -> DataFrame:
    return typed_changes.dropDuplicates([spec.pk, "_lsn"]).select(
        *spec.column_names,
        F.col("_source_ts").alias("valid_from"),
        (F.col("_op") == "d").alias("is_deleted"),
        F.col("_lsn"),
    )


def recompute_history(versions: DataFrame, spec: TableSpec) -> DataFrame:
    w = Window.partitionBy(spec.pk).orderBy("_lsn")
    next_lsn = F.lead("_lsn").over(w)
    return versions.select(
        *versions.columns,
        F.lead("valid_from").over(w).alias("valid_to"),
        (next_lsn.isNull() & ~F.col("is_deleted")).alias("is_current"),
        next_lsn.alias("_end_lsn"),
    )


def apply_scd2(spark: SparkSession, typed_changes: DataFrame, path: str, spec: TableSpec) -> None:
    new_versions = changes_to_versions(typed_changes, spec)
    target = ensure_table(spark, path, recompute_history(new_versions, spec).schema)

    version_cols = [*spec.column_names, *VERSION_META]
    touched_keys = new_versions.select(spec.pk).distinct()
    existing = target.toDF().join(touched_keys, spec.pk, "left_semi").select(*version_cols)

    history = recompute_history(
        existing.unionByName(new_versions.select(*version_cols)).dropDuplicates([spec.pk, "_lsn"]),
        spec,
    )
    # Materialise so the MERGE source is not re-derived from the table it is writing.
    # Re-aliasing gives the columns fresh identities; otherwise Delta cannot tell
    # target columns from source columns that were derived from the target.
    history = history.localCheckpoint(eager=True)
    history = history.select(*[F.col(c).alias(c) for c in history.columns])

    (
        target.alias("t")
        .merge(history.alias("s"), f"t.{spec.pk} = s.{spec.pk} AND t._lsn = s._lsn")
        .whenMatchedUpdate(
            condition="NOT (t.valid_to <=> s.valid_to) OR NOT (t.is_current <=> s.is_current)",
            set={"valid_to": "s.valid_to", "is_current": "s.is_current", "_end_lsn": "s._end_lsn"},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
