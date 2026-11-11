-- Source OLTP schema for a small online shop.
--
-- Constraints are deliberately loose on some business columns (email format,
-- country code, price sign, order quantity, status). That mimics a real
-- upstream application that sometimes writes bad data; catching it is the job
-- of the lakehouse data-quality layer, not the source database.

CREATE TABLE IF NOT EXISTS customers (
    customer_id  BIGSERIAL PRIMARY KEY,
    email        TEXT,
    full_name    TEXT,
    country      TEXT,
    tier         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   BIGSERIAL PRIMARY KEY,
    sku          TEXT,
    name         TEXT,
    category     TEXT,
    price        NUMERIC(10, 2),
    stock        INTEGER,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT NOT NULL REFERENCES customers (customer_id) ON DELETE CASCADE,
    product_id   BIGINT NOT NULL REFERENCES products (product_id),
    quantity     INTEGER,
    unit_price   NUMERIC(10, 2),
    status       TEXT,
    ordered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id);

-- REPLICA IDENTITY FULL makes Postgres log the complete old row for UPDATE and
-- DELETE, so Debezium's "before" image carries every column, not just the key.
-- Deletes therefore still tell us what the row looked like when it was removed.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

-- The publication defines which tables the pgoutput plugin streams.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication FOR TABLE customers, products, orders;
    END IF;
END $$;
