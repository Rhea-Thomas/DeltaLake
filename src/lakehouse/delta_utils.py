from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import types as T


def materialize(df):
    """Compute a DataFrame once and cut it loose from its inputs.

    localCheckpoint() does that best, but serverless/Spark Connect compute (e.g.
    Databricks Free Edition) may not support it, so there persist() + count()
    is used instead. LAKEHOUSE_MATERIALIZE=local_checkpoint|persist overrides
    the automatic choice (the unit tests run both).
    """
    mode = os.environ.get("LAKEHOUSE_MATERIALIZE", "auto")
    if mode == "auto":
        mode = "persist" if os.environ.get("DATABRICKS_RUNTIME_VERSION") else "local_checkpoint"
    if mode == "persist":
        df = df.persist()
        df.count()
        return df
    return df.localCheckpoint(eager=True)


def _nullable(schema: T.StructType) -> T.StructType:
    return T.StructType([T.StructField(f.name, f.dataType, True) for f in schema.fields])


_known_tables: set[str] = set()


def ensure_table(spark: SparkSession, path: str, schema: T.StructType,
                 partition_by: list[str] | None = None) -> DeltaTable:
    """Create an empty Delta table at ``path`` if none exists, then return it.

    Existence is remembered per process so steady-state batches skip the check.
    """
    if path in _known_tables:
        return DeltaTable.forPath(spark, path)
    builder = DeltaTable.createIfNotExists(spark).location(path).addColumns(_nullable(schema))
    if partition_by:
        builder = builder.partitionedBy(*partition_by)
    builder.execute()
    _known_tables.add(path)
    return DeltaTable.forPath(spark, path)
