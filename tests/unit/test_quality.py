"""Data-quality rules: valid rows pass, every rule catches its violation
(including NULLs), deletes are judged on the key only, and quarantined rows
never reach silver."""
import json

import pytest

from lakehouse.cdc import parse_changes, to_typed
from lakehouse.quality import split_valid_invalid, to_quarantine
from lakehouse.silver import upsert_current_state
from lakehouse.tables import CUSTOMERS, ORDERS, PRODUCTS

from .events import bronze_df, customer, event


def _check(spark, spec, rows):
    changes = parse_changes(bronze_df(spark, rows), spec)
    valid, invalid = split_valid_invalid(changes, spec)
    return valid.collect(), invalid.collect()


def product(pid, **kw):
    row = {"product_id": pid, "sku": "SKU-000123", "name": "Pro Lamp", "category": "home",
           "price": "19.99", "stock": 10, "updated_at": "2026-01-01T00:00:00Z"}
    row.update(kw)
    return row


def order(oid, **kw):
    row = {"order_id": oid, "customer_id": 1, "product_id": 2, "quantity": 2, "unit_price": "5.00",
           "status": "placed", "ordered_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}
    row.update(kw)
    return row


def test_valid_customer_passes_and_numbers_parse_as_strings(spark):
    valid, invalid = _check(spark, CUSTOMERS, [event("customers", "c", 10, customer(1))])
    assert invalid == []
    assert len(valid) == 1
    assert valid[0]["customer_id"] == "1"  # JSON number read as raw text, typed later


@pytest.mark.parametrize("override, rule", [
    ({"email": "ava_at_example.com"}, "email_format"),
    ({"email": "ava@example"}, "email_format"),
    ({"country": "usa"}, "country_iso2"),
    ({"country": "us"}, "country_iso2"),
    ({"tier": "gold-ish"}, "tier_allowed"),
    ({"full_name": "   "}, "full_name_present"),
    ({"created_at": "not-a-time"}, "created_at_timestamp"),
])
def test_customer_rule_violations(spark, override, rule):
    _, invalid = _check(spark, CUSTOMERS, [event("customers", "c", 10, customer(1, **override))])
    assert len(invalid) == 1
    assert [e.split(":")[0] for e in invalid[0]["_dq_errors"]] == [rule]


@pytest.mark.parametrize("column, rule", [
    ("email", "email_format"), ("country", "country_iso2"),
    ("tier", "tier_allowed"), ("full_name", "full_name_present"),
])
def test_null_values_fail_instead_of_slipping_through(spark, column, rule):
    row = customer(1)
    row[column] = None  # a regex/IN on NULL is NULL, which must count as a failure
    _, invalid = _check(spark, CUSTOMERS, [event("customers", "u", 10, row)])
    assert len(invalid) == 1
    assert invalid[0]["_dq_errors"] == [f"{rule}: {column}=NULL"]


def test_multiple_failures_are_all_reported(spark):
    _, invalid = _check(spark, CUSTOMERS, [event("customers", "c", 10, customer(1, email="x", country="zz"))])
    assert sorted(e.split(":")[0] for e in invalid[0]["_dq_errors"]) == ["country_iso2", "email_format"]


def test_delete_is_judged_on_key_only(spark):
    bad_content = customer(1, email="broken", tier="??")
    no_key = customer(None)
    valid, invalid = _check(spark, CUSTOMERS, [
        event("customers", "d", 10, bad_content),
        event("customers", "d", 11, no_key),
    ])
    assert [r["_lsn"] for r in valid] == [10]
    assert [r["_lsn"] for r in invalid] == [11]
    assert invalid[0]["_dq_errors"] == ["customer_id_present: customer_id=NULL"]


def test_unknown_op_is_rejected(spark):
    _, invalid = _check(spark, CUSTOMERS, [event("customers", "x", 10, customer(1))])
    assert invalid[0]["_dq_errors"][0].startswith("op_known")


@pytest.mark.parametrize("override, rule", [
    ({"price": "-3.50"}, "price_non_negative"),
    ({"price": "abc"}, "price_decimal"),
    ({"sku": "sku-000123"}, "sku_format"),
    ({"stock": -1}, "stock_non_negative"),
    ({"name": ""}, "name_present"),
])
def test_product_rules(spark, override, rule):
    _, invalid = _check(spark, PRODUCTS, [event("products", "c", 10, product(1, **override))])
    assert rule in [e.split(":")[0] for e in invalid[0]["_dq_errors"]]


@pytest.mark.parametrize("override, rule", [
    ({"quantity": 0}, "quantity_positive"),
    ({"quantity": None}, "quantity_positive"),
    ({"status": "teleported"}, "status_allowed"),
    ({"unit_price": "-1.00"}, "unit_price_non_negative"),
    ({"customer_id": None}, "customer_id_present"),
])
def test_order_rules(spark, override, rule):
    valid, invalid = _check(spark, ORDERS, [event("orders", "c", 10, order(1, **override))])
    assert valid == []
    assert rule in [e.split(":")[0] for e in invalid[0]["_dq_errors"]]


def test_valid_product_and_order_pass(spark):
    assert _check(spark, PRODUCTS, [event("products", "r", 1, product(1))])[1] == []
    assert _check(spark, ORDERS, [event("orders", "r", 1, order(1))])[1] == []


def test_quarantine_record_keeps_reason_and_raw_payload(spark):
    rows = [event("customers", "c", 10, customer(7, email="nope"))]
    changes = parse_changes(bronze_df(spark, rows), CUSTOMERS)
    _, invalid = split_valid_invalid(changes, CUSTOMERS)
    q = to_quarantine(invalid, CUSTOMERS, batch_id=3).collect()[0]
    assert q["source_table"] == "customers"
    assert q["record_key"] == "7"
    assert q["lsn"] == 10
    assert q["reason"] == "email_format: email='nope'"
    assert json.loads(q["raw_event"])["after"]["email"] == "nope"
    assert q["batch_id"] == 3


def test_invalid_rows_never_reach_silver(spark, tmp_path):
    rows = [
        event("customers", "c", 10, customer(1)),
        event("customers", "c", 11, customer(2, email="bad")),
        # valid row 3 later receives an invalid update: silver keeps the last good version
        event("customers", "c", 12, customer(3, tier="free")),
        event("customers", "u", 13, customer(3, tier="platinum")),
    ]
    changes = parse_changes(bronze_df(spark, rows), CUSTOMERS)
    valid, invalid = split_valid_invalid(changes, CUSTOMERS)
    upsert_current_state(spark, to_typed(valid, CUSTOMERS), str(tmp_path / "c"), CUSTOMERS)
    silver = {r["customer_id"]: r for r in spark.read.format("delta").load(str(tmp_path / "c")).collect()}
    assert set(silver) == {1, 3}
    assert silver[3]["tier"] == "free" and silver[3]["_lsn"] == 12
    assert sorted(r["_lsn"] for r in invalid.collect()) == [11, 13]
