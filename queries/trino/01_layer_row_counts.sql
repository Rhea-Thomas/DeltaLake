-- Row counts per medallion layer (Trino dialect).
SELECT 'bronze.cdc_events' AS table_name, count(*) AS row_count FROM bronze_cdc_events
UNION ALL SELECT 'silver.customers (live)', count(*) FROM customers
UNION ALL SELECT 'silver.products (live)', count(*) FROM products
UNION ALL SELECT 'silver.orders (live)', count(*) FROM orders
UNION ALL SELECT 'silver.dim_customers_scd2 (versions)', count(*) FROM dim_customers_scd2
UNION ALL SELECT 'quarantine.cdc_rejects', count(*) FROM quarantine;
