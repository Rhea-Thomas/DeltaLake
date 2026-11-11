-- Customers with the most recorded versions, and how many times their tier changed.
SELECT customer_id,
       count(*)                                    AS versions,
       count(DISTINCT tier)                        AS distinct_tiers,
       bool_or(is_deleted)                         AS was_deleted,
       min(valid_from)                             AS first_seen,
       max(valid_from)                             AS last_change
FROM dim_customers_scd2
GROUP BY customer_id
ORDER BY versions DESC
LIMIT 10;
