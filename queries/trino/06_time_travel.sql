-- Delta time travel from Trino. Each Delta commit is a version (one per silver
-- micro-batch). Note: within ONE query Trino resolves a single snapshot per
-- table, so "then" and "now" are separate statements rather than a UNION.
SELECT 'version 1' AS snapshot, count(*) AS live_customers
FROM silver_customers_all FOR VERSION AS OF 1
WHERE NOT _is_deleted;

SELECT 'latest' AS snapshot, count(*) AS live_customers
FROM silver_customers_all
WHERE NOT _is_deleted;
