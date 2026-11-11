"""SparkSession factory.

On Databricks (or any managed runtime) a session already exists with Delta
configured, so we just return it. Locally we build one with the Delta Lake and
Kafka connector packages pulled from Maven.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

SPARK_VERSION = "3.5.3"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def get_spark(app: str = "lakehouse", with_kafka: bool = False, master: str | None = None,
              shuffle_partitions: int = 4) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from delta import configure_spark_with_delta_pip

    # Python workers must run the same interpreter as the driver (the venv),
    # not whatever `python3` is first on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app)
        .master(master or os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.ui.showConsoleProgress", "false")
        # Keep tables readable by non-Spark engines (DuckDB / delta-kernel).
        .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
    )
    packages = [KAFKA_PACKAGE] if with_kafka else []
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
