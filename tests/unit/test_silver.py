"""Silver current-state MERGE: latest-by-LSN wins, out-of-order and duplicate
events cannot roll a row back, deletes are soft so stale events stay stale."""
from lakehouse.cdc import parse_changes, to_typed
from lakehouse.silver import upsert_current_state
from lakehouse.tables import CUSTOMERS

from .events import bronze_df, customer, event


def apply(spark, path, rows):
    typed = to_typed(parse_changes(bronze_df(spark, rows), CUSTOMERS), CUSTOMERS)
    upsert_current_state(spark, typed, path, CUSTOMERS)
    return {r["customer_id"]: r for r in spark.read.format("delta").load(path).collect()}


def test_latest_lsn_in_batch_wins_regardless_of_arrival_order(spark, tmp_path):
    p = str(tmp_path / "s")
    got = apply(spark, p, [
        event("customers", "u", 30, customer(1, tier="premium")),   # arrives first
        event("customers", "c", 10, customer(1, tier="free")),
        event("customers", "u", 20, customer(1, tier="standard")),
    ])
    assert got[1]["tier"] == "premium" and got[1]["_lsn"] == 30


def test_late_event_in_later_batch_is_ignored(spark, tmp_path):
    p = str(tmp_path / "s")
    apply(spark, p, [event("customers", "u", 30, customer(1, tier="premium"))])
    got = apply(spark, p, [event("customers", "u", 20, customer(1, tier="standard"))])
    assert got[1]["tier"] == "premium"


def test_replaying_a_batch_is_a_no_op(spark, tmp_path):
    p = str(tmp_path / "s")
    rows = [event("customers", "c", 10, customer(1)), event("customers", "c", 11, customer(2))]
    first = apply(spark, p, rows)
    second = apply(spark, p, rows + rows)  # replay + in-batch duplicates
    strip = lambda d: {k: {c: v for c, v in r.asDict().items() if c != "_processed_at"} for k, r in d.items()}
    assert strip(first) == strip(second)


def test_delete_is_soft_and_blocks_stale_resurrection(spark, tmp_path):
    p = str(tmp_path / "s")
    apply(spark, p, [event("customers", "c", 10, customer(1))])
    apply(spark, p, [event("customers", "d", 30, customer(1))])
    got = apply(spark, p, [event("customers", "u", 20, customer(1, tier="premium"))])  # stale
    assert got[1]["_is_deleted"] is True and got[1]["_lsn"] == 30
