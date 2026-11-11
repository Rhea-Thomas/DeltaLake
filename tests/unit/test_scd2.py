"""SCD Type 2 MERGE: version chains, out-of-order, duplicates, deletes,
idempotent replay and point-in-time lookups."""
import random

import pytest

from lakehouse.cdc import parse_changes, to_typed
from lakehouse.scd2 import apply_scd2
from lakehouse.tables import CUSTOMERS

from .events import bronze_df, customer, event, ts


def apply(spark, path, rows):
    typed = to_typed(parse_changes(bronze_df(spark, rows), CUSTOMERS), CUSTOMERS)
    apply_scd2(spark, typed, path, CUSTOMERS)


def history(spark, path, cid=None):
    df = spark.read.format("delta").load(path)
    if cid is not None:
        df = df.where(f"customer_id = {cid}")
    cols = ["customer_id", "tier", "valid_from", "valid_to", "is_current", "is_deleted", "_lsn", "_end_lsn"]
    return [tuple(r) for r in df.select(*cols).orderBy("customer_id", "_lsn").collect()]


def as_of(spark, path, cid, when):
    rows = (spark.read.format("delta").load(path)
            .where(f"customer_id = {cid} AND NOT is_deleted")
            .where(f"valid_from <= '{when}' AND (valid_to IS NULL OR valid_to > '{when}')")
            .collect())
    assert len(rows) <= 1, "point-in-time must resolve to at most one version"
    return rows[0]["tier"] if rows else None


def test_inserts_and_updates_build_a_chain(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "c", 10, customer(1, tier="free"))])
    apply(spark, p, [event("customers", "u", 20, customer(1, tier="standard")),
                     event("customers", "u", 30, customer(1, tier="premium"))])
    assert history(spark, p) == [
        (1, "free", ts(1), ts(2), False, False, 10, 20),
        (1, "standard", ts(2), ts(3), False, False, 20, 30),
        (1, "premium", ts(3), None, True, False, 30, None),
    ]


def test_out_of_order_event_is_slotted_into_the_middle(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "c", 10, customer(1, tier="free")),
                     event("customers", "u", 30, customer(1, tier="premium"))])
    apply(spark, p, [event("customers", "u", 20, customer(1, tier="standard"))])  # late
    assert history(spark, p) == [
        (1, "free", ts(1), ts(2), False, False, 10, 20),
        (1, "standard", ts(2), ts(3), False, False, 20, 30),
        (1, "premium", ts(3), None, True, False, 30, None),
    ]


def test_late_event_older_than_everything(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "u", 20, customer(1, tier="standard"))])
    apply(spark, p, [event("customers", "c", 10, customer(1, tier="free"))])
    assert [h[1] for h in history(spark, p)] == ["free", "standard"]
    assert history(spark, p)[1][4] is True


def test_duplicates_within_and_across_batches(spark, tmp_path):
    p = str(tmp_path / "d")
    e1 = event("customers", "c", 10, customer(1, tier="free"))
    e2 = event("customers", "u", 20, customer(1, tier="premium"))
    apply(spark, p, [e1, e1, e2])
    before = history(spark, p)
    apply(spark, p, [e2, e1])  # full replay
    assert history(spark, p) == before
    assert len(before) == 2


def test_delete_adds_tombstone_and_leaves_no_current_row(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "c", 10, customer(1, tier="free"))])
    apply(spark, p, [event("customers", "d", 40, customer(1, tier="free"))])
    assert history(spark, p) == [
        (1, "free", ts(1), ts(4), False, False, 10, 40),
        (1, "free", ts(4), None, False, True, 40, None),
    ]


def test_late_update_arriving_after_delete(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "c", 10, customer(1, tier="free")),
                     event("customers", "d", 40, customer(1, tier="premium"))])
    apply(spark, p, [event("customers", "u", 20, customer(1, tier="premium"))])
    h = history(spark, p)
    assert [(r[1], r[5]) for r in h] == [("free", False), ("premium", False), ("premium", True)]
    assert not any(r[4] for r in h)
    assert h[1][3] == ts(4)


def test_point_in_time_lookup(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "c", 10, customer(1, tier="free")),
                     event("customers", "u", 20, customer(1, tier="standard")),
                     event("customers", "u", 30, customer(1, tier="premium")),
                     event("customers", "d", 50, customer(1, tier="premium"))])
    assert as_of(spark, p, 1, ts(0.5)) is None          # before it existed
    assert as_of(spark, p, 1, ts(1)) == "free"          # valid_from is inclusive
    assert as_of(spark, p, 1, ts(1.99)) == "free"
    assert as_of(spark, p, 1, ts(2)) == "standard"      # valid_to is exclusive
    assert as_of(spark, p, 1, ts(4)) == "premium"
    assert as_of(spark, p, 1, ts(6)) is None            # deleted


def test_keys_are_independent(spark, tmp_path):
    p = str(tmp_path / "d")
    apply(spark, p, [event("customers", "c", 10, customer(1)), event("customers", "c", 11, customer(2))])
    apply(spark, p, [event("customers", "u", 20, customer(2, tier="premium"))])
    h = history(spark, p)
    assert [(r[0], r[1], r[4]) for r in h] == [(1, "free", True), (2, "free", False), (2, "premium", True)]


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_shuffled_duplicated_batches_equal_in_order_result(spark, tmp_path, seed):
    """Property check: any arrival order, batching and duplication -> same history."""
    rng = random.Random(seed)
    tiers = ["free", "standard", "premium"]
    events, lsn = [], 0
    for cid in range(1, 6):
        lsn += 1
        events.append(event("customers", "c", lsn, customer(cid, tier="free")))
        for _ in range(rng.randint(0, 4)):
            lsn += 1
            events.append(event("customers", "u", lsn, customer(cid, tier=rng.choice(tiers))))
        if rng.random() < 0.4:
            lsn += 1
            events.append(event("customers", "d", lsn, customer(cid)))

    in_order = str(tmp_path / "ordered")
    apply(spark, in_order, events)

    messy = events + rng.sample(events, k=len(events) // 3)   # duplicates
    rng.shuffle(messy)
    chaotic = str(tmp_path / "chaotic")
    cut = sorted(rng.sample(range(1, len(messy)), k=3))
    for chunk in (messy[:cut[0]], messy[cut[0]:cut[1]], messy[cut[1]:cut[2]], messy[cut[2]:]):
        apply(spark, chaotic, chunk)

    assert history(spark, chaotic) == history(spark, in_order)
    current = [r for r in history(spark, in_order) if r[4]]
    assert len({r[0] for r in current}) == len(current)  # at most one current per key
