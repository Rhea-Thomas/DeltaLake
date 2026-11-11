"""Data-quality rules.

Each rule is a named boolean predicate over the *raw string* columns of a
parsed change row. A row is valid when every applicable rule evaluates to
TRUE. A rule that evaluates to NULL (for example a regex on a NULL column)
counts as a failure: "unknown" is never good enough to reach silver.

Key rules apply to every change, including deletes. Content rules apply only to
c/u/r changes, because a delete's job is to remove the row whatever it held.

Failed rows keep their raw Debezium payload and get an ``_dq_errors`` array
such as ``["email_format: email='bob_at_example.com'"]``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .tables import TableSpec

EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"
ALLOWED_TIERS = ["free", "standard", "premium"]
ALLOWED_STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
VALID_OPS = ["c", "u", "d", "r"]


@dataclass(frozen=True)
class Rule:
    name: str
    column: str | None          # column whose value is reported on failure
    predicate: Callable[[], Column]
    key_rule: bool = False      # key rules also apply to deletes


def _castable(col: str, sql_type: str) -> Column:
    return F.col(col).isNotNull() & F.col(col).cast(sql_type).isNotNull()


def _non_blank(col: str) -> Column:
    return F.length(F.trim(F.col(col))) > 0


COMMON_RULES = [
    Rule("op_known", "_op", lambda: F.col("_op").isin(VALID_OPS), key_rule=True),
    Rule("lsn_present", "_lsn", lambda: F.col("_lsn").isNotNull(), key_rule=True),
]

RULES: dict[str, list[Rule]] = {
    "customers": [
        Rule("customer_id_present", "customer_id", lambda: _castable("customer_id", "bigint"), key_rule=True),
        Rule("email_format", "email", lambda: F.col("email").rlike(EMAIL_RE)),
        Rule("full_name_present", "full_name", lambda: _non_blank("full_name")),
        Rule("country_iso2", "country", lambda: F.col("country").rlike(r"^[A-Z]{2}$")),
        Rule("tier_allowed", "tier", lambda: F.col("tier").isin(ALLOWED_TIERS)),
        Rule("created_at_timestamp", "created_at", lambda: _castable("created_at", "timestamp")),
    ],
    "products": [
        Rule("product_id_present", "product_id", lambda: _castable("product_id", "bigint"), key_rule=True),
        Rule("sku_format", "sku", lambda: F.col("sku").rlike(r"^SKU-[0-9]{6}$")),
        Rule("name_present", "name", lambda: _non_blank("name")),
        Rule("price_decimal", "price", lambda: _castable("price", "decimal(10,2)")),
        Rule("price_non_negative", "price", lambda: F.col("price").cast("decimal(10,2)") >= 0),
        Rule("stock_non_negative", "stock", lambda: F.col("stock").cast("int") >= 0),
    ],
    "orders": [
        Rule("order_id_present", "order_id", lambda: _castable("order_id", "bigint"), key_rule=True),
        Rule("customer_id_present", "customer_id", lambda: _castable("customer_id", "bigint")),
        Rule("product_id_present", "product_id", lambda: _castable("product_id", "bigint")),
        Rule("quantity_positive", "quantity", lambda: F.col("quantity").cast("int") > 0),
        Rule("unit_price_non_negative", "unit_price", lambda: F.col("unit_price").cast("decimal(10,2)") >= 0),
        Rule("status_allowed", "status", lambda: F.col("status").isin(ALLOWED_STATUSES)),
        Rule("ordered_at_timestamp", "ordered_at", lambda: _castable("ordered_at", "timestamp")),
    ],
}


def rules_for(spec: TableSpec) -> list[Rule]:
    return COMMON_RULES + RULES[spec.name]


def _failure_message(rule: Rule) -> Column:
    if rule.column is None:
        return F.lit(rule.name)
    shown = F.coalesce(F.concat(F.lit("'"), F.col(rule.column).cast("string"), F.lit("'")), F.lit("NULL"))
    return F.concat(F.lit(f"{rule.name}: {rule.column}="), shown)


def evaluate(changes: DataFrame, spec: TableSpec) -> DataFrame:
    """Add ``_dq_errors`` (array<string>, empty when the row is valid)."""
    is_delete = F.col("_op") == "d"
    checks = []
    for rule in rules_for(spec):
        passed = F.coalesce(rule.predicate(), F.lit(False))   # NULL -> failure
        applies = F.lit(True) if rule.key_rule else ~F.coalesce(is_delete, F.lit(False))
        checks.append(F.when(applies & ~passed, _failure_message(rule)))
    errors = F.filter(F.array(*checks), lambda e: e.isNotNull())
    return changes.withColumn("_dq_errors", errors)


def split_valid_invalid(changes: DataFrame, spec: TableSpec) -> tuple[DataFrame, DataFrame]:
    checked = evaluate(changes, spec)
    ok = F.size("_dq_errors") == 0
    return checked.where(ok).drop("_dq_errors"), checked.where(~ok)


def to_quarantine(invalid: DataFrame, spec: TableSpec, batch_id: int) -> DataFrame:
    """Shape rejected changes for the quarantine table (one schema for all tables)."""
    return invalid.select(
        F.lit(spec.name).alias("source_table"),
        F.col(spec.pk).cast("string").alias("record_key"),
        F.col("_op").alias("op"),
        F.col("_lsn").alias("lsn"),
        F.col("_source_ts").alias("source_ts"),
        F.col("_dq_errors").alias("errors"),
        F.array_join("_dq_errors", "; ").alias("reason"),
        F.col("_raw").alias("raw_event"),
        F.col("_topic").alias("kafka_topic"),
        F.col("_partition").alias("kafka_partition"),
        F.col("_offset").alias("kafka_offset"),
        F.lit(batch_id).cast("long").alias("batch_id"),
        F.current_timestamp().alias("quarantined_at"),
    )


QUARANTINE_KEY = ["source_table", "record_key", "lsn"]


def write_quarantine(spark: SparkSession, rejects: DataFrame, path: str) -> None:
    """Insert rejected changes that are not already quarantined.

    Keyed by (source_table, record_key, lsn): one source change, one row.
    A plain append would be safe against Spark replaying a batch, but not
    against Debezium re-publishing the same change into a *later* batch (it is
    at-least-once, e.g. after a Kafka Connect crash). An insert-only MERGE
    handles both.
    """
    from .delta_utils import ensure_table

    rows = rejects.dropDuplicates(QUARANTINE_KEY)
    target = ensure_table(spark, path, rows.schema)
    cond = " AND ".join(f"t.{c} <=> s.{c}" for c in QUARANTINE_KEY)
    target.alias("t").merge(rows.alias("s"), cond).whenNotMatchedInsertAll().execute()
