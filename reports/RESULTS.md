# Measured results

Run at 2026-11-11 01:22 UTC on arm64 Darwin 25.6.0. Clock skew Postgres container vs host: 1.3 ms (positive = container ahead).

## End-to-end latency (Postgres commit -> visible in silver and SCD2)

Steady load: 100 generator actions/s for 120s, 16539 change events in 18 silver micro-batches.

| stage | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Postgres -> Kafka (Debezium) | 478.0 ms | 850.0 ms | 942.6 ms | 1003 ms |
| Postgres -> bronze micro-batch start | 1363.0 ms | 2585.0 ms | 3146.2 ms | 3655 ms |
| **Postgres -> silver + SCD2 committed** | 12846.0 ms | 17222.1 ms | 19155.0 ms | 20140 ms |

Silver micro-batch duration: p50 6.6 s, p95 9.0 s.

## Throughput

* Unthrottled generator for 60s produced 40229 events (668.9 events/s at the source); the pipeline caught up 6.6 s after the generator stopped. End-to-end: **602.9 events/s**. Latency under this load: p50 15629.0 ms, p95 21319.0 ms.
* One transaction updating all 27825 orders: fully visible in silver 5.5 s after commit (**5056.3 events/s**), in 1 micro-batch(es).

## Row counts per layer (after the run)

| table | rows |
|---|---|
| bronze.cdc_events | 196159 |
| silver.customers (live) | 9033 |
| silver.products (live) | 2159 |
| silver.orders (live) | 26942 |
| silver.dim_customers_scd2 (versions) | 32499 |
| silver.dim_customers_scd2 (current) | 9033 |
| quarantine.cdc_rejects | 9574 |

Bronze events by table/op: customers/c=11060, customers/d=2149, customers/r=300, customers/u=19853, orders/c=37844, orders/d=10619, orders/r=600, orders/u=65033, products/c=2180, products/r=60, products/u=46461

## Reconciliation

PASSED after 2s of polling.

```json
{
  "kafka_messages": 196159,
  "bronze_rows": 196159,
  "customers": {
    "source_rows": 9211,
    "silver_live": 9033,
    "silver_soft_deleted": 2149,
    "matched_exactly": 8938,
    "latest_change_quarantined": 273,
    "quarantined_events": 863
  },
  "products": {
    "source_rows": 2240,
    "silver_live": 2159,
    "silver_soft_deleted": 0,
    "matched_exactly": 1960,
    "latest_change_quarantined": 280,
    "quarantined_events": 5849
  },
  "orders": {
    "source_rows": 27825,
    "silver_live": 26942,
    "silver_soft_deleted": 10619,
    "matched_exactly": 26942,
    "latest_change_quarantined": 883,
    "quarantined_events": 2862
  },
  "dim_customers_scd2": {
    "versions": 32499,
    "current": 9033,
    "valid_customer_events": 32499
  },
  "quarantine_rows": 9574
}
```
