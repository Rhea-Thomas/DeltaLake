"""Workload generator for the source Postgres database.

Continuously inserts, updates and deletes rows in customers / products / orders
so that Debezium has a realistic stream of row-level changes to capture.

A small fraction of writes (``--bad-ratio``) deliberately violate the business
rules (malformed email, lowercase country, negative price, zero quantity,
unknown status, ...) so the data-quality layer has something to quarantine.

Examples:
    python -m generator.generate --init            # create schema + seed data
    python -m generator.generate --rate 100 --duration 120
    python -m generator.generate --rate 0 --duration 60   # unthrottled
"""
from __future__ import annotations

import argparse
import os
import random
import string
import time
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

DSN = os.environ.get(
    "SOURCE_DSN", "host=localhost port=5433 dbname=shop user=postgres password=postgres"
)
SCHEMA_SQL = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"

FIRST = ["Ava", "Liam", "Maya", "Noah", "Zoe", "Ethan", "Priya", "Lucas", "Mei", "Omar",
         "Sofia", "Kenji", "Amara", "Diego", "Hana", "Ivan", "Leila", "Mateo", "Nia", "Ravi"]
LAST = ["Smith", "Garcia", "Chen", "Patel", "Kim", "Nguyen", "Okafor", "Silva", "Müller",
        "Rossi", "Kowalski", "Haddad", "Tanaka", "Johnson", "Dubois", "Novak"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "JP", "BR", "AU", "NG", "MX", "ES"]
TIERS = ["free", "standard", "premium"]
CATEGORIES = ["books", "electronics", "kitchen", "garden", "toys", "sports", "beauty"]
NOUNS = ["Lamp", "Kettle", "Novel", "Headphones", "Trowel", "Puzzle", "Racket", "Serum",
         "Backpack", "Blender", "Notebook", "Drone", "Mug", "Yoga Mat"]
STATUS_NEXT = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}

# Each action and its relative weight in the workload mix.
ACTIONS = [
    ("insert_customer", 10),
    ("update_customer", 18),
    ("delete_customer", 2),
    ("insert_product", 2),
    ("update_product", 8),
    ("place_order", 35),
    ("advance_order", 20),
    ("cancel_order", 3),
    ("delete_order", 2),
]


@dataclass
class Stats:
    counts: dict = field(default_factory=dict)
    bad: int = 0

    def add(self, action: str) -> None:
        self.counts[action] = self.counts.get(action, 0) + 1


class Generator:
    def __init__(self, conn: psycopg.Connection, bad_ratio: float, rng: random.Random):
        self.conn = conn
        self.bad_ratio = bad_ratio
        self.rng = rng
        self.stats = Stats()

    # ---------- helpers ----------
    def _bad(self) -> bool:
        hit = self.rng.random() < self.bad_ratio
        if hit:
            self.stats.bad += 1
        return hit

    def _random_id(self, table: str, pk: str) -> int | None:
        # TABLESAMPLE would be faster on huge tables; this is fine at demo scale.
        row = self.conn.execute(
            f"SELECT {pk} FROM {table} WHERE {pk} >= "
            f"(SELECT floor(random() * (max({pk}) - min({pk}) + 1) + min({pk})) FROM {table}) "
            f"ORDER BY {pk} LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    def _customer_values(self):
        first, last = self.rng.choice(FIRST), self.rng.choice(LAST)
        tag = "".join(self.rng.choices(string.ascii_lowercase + string.digits, k=5))
        email = f"{first.lower()}.{last.lower()}.{tag}@example.com"
        country = self.rng.choice(COUNTRIES)
        tier = self.rng.choice(TIERS)
        if self._bad():
            kind = self.rng.choice(["email_no_at", "email_null", "country_lower", "tier_bad", "name_blank"])
            if kind == "email_no_at":
                email = email.replace("@", "_at_")
            elif kind == "email_null":
                email = None
            elif kind == "country_lower":
                country = country.lower() + "a"
            elif kind == "tier_bad":
                tier = "gold-ish"
            else:
                first, last = " ", ""
        return email, f"{first} {last}".strip() or " ", country, tier

    # ---------- actions ----------
    def insert_customer(self):
        self.conn.execute(
            "INSERT INTO customers (email, full_name, country, tier) VALUES (%s, %s, %s, %s)",
            self._customer_values(),
        )

    def update_customer(self):
        cid = self._random_id("customers", "customer_id")
        if cid is None:
            return self.insert_customer()
        email, name, country, tier = self._customer_values()
        col, val = self.rng.choice([("tier", tier), ("country", country), ("email", email), ("full_name", name)])
        self.conn.execute(
            f"UPDATE customers SET {col} = %s, updated_at = now() WHERE customer_id = %s", (val, cid)
        )

    def delete_customer(self):
        cid = self._random_id("customers", "customer_id")
        if cid is not None:
            # ON DELETE CASCADE also deletes the customer's orders -> extra CDC deletes.
            self.conn.execute("DELETE FROM customers WHERE customer_id = %s", (cid,))

    def insert_product(self):
        sku = f"SKU-{self.rng.randint(0, 999999):06d}"
        price = round(self.rng.uniform(3, 400), 2)
        if self._bad():
            if self.rng.random() < 0.5:
                price = -price
            else:
                sku = "sku" + sku[4:]
        self.conn.execute(
            "INSERT INTO products (sku, name, category, price, stock) VALUES (%s, %s, %s, %s, %s)",
            (sku, f"{self.rng.choice(['Classic', 'Pro', 'Mini', 'Eco'])} {self.rng.choice(NOUNS)}",
             self.rng.choice(CATEGORIES), price, self.rng.randint(0, 500)),
        )

    def update_product(self):
        pid = self._random_id("products", "product_id")
        if pid is None:
            return self.insert_product()
        if self._bad():
            self.conn.execute(
                "UPDATE products SET price = -abs(price), updated_at = now() WHERE product_id = %s", (pid,)
            )
        elif self.rng.random() < 0.5:
            self.conn.execute(
                "UPDATE products SET price = round((price * %s)::numeric, 2), updated_at = now() "
                "WHERE product_id = %s", (self.rng.uniform(0.85, 1.15), pid),
            )
        else:
            self.conn.execute(
                "UPDATE products SET stock = %s, updated_at = now() WHERE product_id = %s",
                (self.rng.randint(0, 500), pid),
            )

    def place_order(self):
        cid = self._random_id("customers", "customer_id")
        pid = self._random_id("products", "product_id")
        if cid is None or pid is None:
            return self.insert_customer()
        qty = self.rng.randint(1, 5)
        status = "placed"
        if self._bad():
            if self.rng.random() < 0.5:
                qty = 0
            else:
                status = "teleported"
        # A multi-statement transaction: the order and the stock change commit atomically.
        with self.conn.transaction():
            self.conn.execute(
                "INSERT INTO orders (customer_id, product_id, quantity, unit_price, status) "
                "SELECT %s, product_id, %s, abs(price), %s FROM products WHERE product_id = %s",
                (cid, qty, status, pid),
            )
            self.conn.execute(
                "UPDATE products SET stock = greatest(stock - %s, 0), updated_at = now() WHERE product_id = %s",
                (max(qty, 0), pid),
            )

    def advance_order(self):
        oid = self._random_id("orders", "order_id")
        if oid is None:
            return self.place_order()
        row = self.conn.execute("SELECT status FROM orders WHERE order_id = %s", (oid,)).fetchone()
        nxt = STATUS_NEXT.get(row[0]) if row else None
        if nxt:
            self.conn.execute(
                "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s", (nxt, oid)
            )

    def cancel_order(self):
        oid = self._random_id("orders", "order_id")
        if oid is not None:
            self.conn.execute(
                "UPDATE orders SET status = 'cancelled', updated_at = now() "
                "WHERE order_id = %s AND status IN ('placed', 'paid')", (oid,),
            )

    def delete_order(self):
        oid = self._random_id("orders", "order_id")
        if oid is not None:
            self.conn.execute("DELETE FROM orders WHERE order_id = %s", (oid,))

    def step(self) -> None:
        names, weights = zip(*ACTIONS)
        action = self.rng.choices(names, weights=weights)[0]
        getattr(self, action)()
        self.stats.add(action)


def init_schema(conn: psycopg.Connection, customers: int, products: int, orders: int, rng: random.Random) -> None:
    conn.execute(SCHEMA_SQL.read_text())
    existing = conn.execute("SELECT count(*) FROM customers").fetchone()[0]
    if existing:
        print(f"schema exists with {existing} customers; skipping seed")
        return
    gen = Generator(conn, bad_ratio=0.0, rng=rng)
    for _ in range(customers):
        gen.insert_customer()
    for _ in range(products):
        gen.insert_product()
    for _ in range(orders):
        gen.place_order()
    print(f"seeded {customers} customers, {products} products, {orders} orders")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=DSN)
    ap.add_argument("--init", action="store_true", help="create schema and seed data, then exit")
    ap.add_argument("--rate", type=float, default=50, help="actions per second (0 = unthrottled)")
    ap.add_argument("--duration", type=float, default=60, help="seconds to run")
    ap.add_argument("--bad-ratio", type=float, default=0.03, help="fraction of writes that break a rule")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        if args.init:
            init_schema(conn, 300, 60, 600, rng)
            return
        gen = Generator(conn, args.bad_ratio, rng)
        start = time.monotonic()
        n = 0
        interval = 1.0 / args.rate if args.rate > 0 else 0.0
        next_at = start
        while time.monotonic() - start < args.duration:
            gen.step()
            n += 1
            if interval:
                next_at += interval
                sleep = next_at - time.monotonic()
                if sleep > 0:
                    time.sleep(sleep)
        elapsed = time.monotonic() - start
        print(f"ran {n} actions in {elapsed:.1f}s ({n / elapsed:.0f}/s), ~{gen.stats.bad} rule-breaking values generated")
        for k, v in sorted(gen.stats.counts.items()):
            print(f"  {k:16s} {v}")


if __name__ == "__main__":
    main()
