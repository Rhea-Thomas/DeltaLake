# Measured results

Run at 2026-11-11 03:03 UTC on arm64 Darwin 25.6.0. Clock skew Postgres container vs host: -14.3 ms (positive = container ahead).

## End-to-end latency (Postgres commit -> visible in silver and SCD2)

Steady load: 100 generator actions/s for 120s, 16676 change events in 18 silver micro-batches.

| stage | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Postgres -> Kafka (Debezium) | 489.0 ms | 853.0 ms | 945.0 ms | 1001 ms |
| Postgres -> bronze micro-batch start | 1381.0 ms | 2693.2 ms | 3297.5 ms | 3662 ms |
| **Postgres -> silver + SCD2 committed** | 12973.0 ms | 16844.2 ms | 18217.5 ms | 18705 ms |

Silver micro-batch duration: p50 6.8 s, p95 8.2 s.

## Throughput

* Unthrottled generator for 60s produced 30566 events (508.0 events/s at the source); the pipeline caught up 8.8 s after the generator stopped. End-to-end: **443.0 events/s**. Latency under this load: p50 14049.0 ms, p95 18336.8 ms.
* One transaction updating all 58510 orders: fully visible in silver 6.7 s after commit (**8718.5 events/s**), in 1 micro-batch(es).

## Row counts per layer (after the run)

| table | rows |
|---|---|
| bronze.cdc_events | 490432 |
| silver.customers (live) | 19466 |
| silver.products (live) | 4691 |
| silver.orders (live) | 56706 |
| silver.dim_customers_scd2 (versions) | 71343 |
| silver.dim_customers_scd2 (current) | 19466 |
| quarantine.cdc_rejects | 23796 |

Bronze events by table/op: customers/c=24337, customers/d=4753, customers/r=300, customers/u=44010, orders/c=84071, orders/d=26120, orders/r=600, orders/u=198003, products/c=4806, products/r=60, products/u=103372

## Reconciliation

PASSED after 2s of polling.

```json
{
  "kafka_messages": 490432,
  "bronze_rows": 490432,
  "duplicate_change_events_absorbed": 250,
  "customers": {
    "source_rows": 19877,
    "silver_live": 19466,
    "silver_soft_deleted": 4749,
    "matched_exactly": 19277,
    "latest_change_quarantined": 600,
    "quarantined_events": 2018
  },
  "products": {
    "source_rows": 4858,
    "silver_live": 4691,
    "silver_soft_deleted": 0,
    "matched_exactly": 4234,
    "latest_change_quarantined": 624,
    "quarantined_events": 13793
  },
  "orders": {
    "source_rows": 58510,
    "silver_live": 56706,
    "silver_soft_deleted": 26093,
    "matched_exactly": 56706,
    "latest_change_quarantined": 1804,
    "quarantined_events": 7985
  },
  "dim_customers_scd2": {
    "versions": 71343,
    "current": 19466,
    "valid_customer_events": 71343
  },
  "quarantine_rows": 23796
}
```
