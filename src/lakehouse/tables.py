"""Source table specifications: primary key and typed columns.

The same spec drives parsing of Debezium payloads, data-quality rules, the
silver current-state schema and the reconciliation check.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk: str
    columns: tuple[tuple[str, str], ...]  # (column, Spark SQL type)

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]


CUSTOMERS = TableSpec(
    "customers",
    "customer_id",
    (
        ("customer_id", "bigint"),
        ("email", "string"),
        ("full_name", "string"),
        ("country", "string"),
        ("tier", "string"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
)

PRODUCTS = TableSpec(
    "products",
    "product_id",
    (
        ("product_id", "bigint"),
        ("sku", "string"),
        ("name", "string"),
        ("category", "string"),
        ("price", "decimal(10,2)"),
        ("stock", "int"),
        ("updated_at", "timestamp"),
    ),
)

ORDERS = TableSpec(
    "orders",
    "order_id",
    (
        ("order_id", "bigint"),
        ("customer_id", "bigint"),
        ("product_id", "bigint"),
        ("quantity", "int"),
        ("unit_price", "decimal(10,2)"),
        ("status", "string"),
        ("ordered_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
)

TABLES: dict[str, TableSpec] = {t.name: t for t in (CUSTOMERS, PRODUCTS, ORDERS)}
