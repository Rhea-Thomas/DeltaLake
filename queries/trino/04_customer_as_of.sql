-- Point in time: customer X as of timestamp T.
-- Defaults: the customer with the most versions, at the middle of its history.
-- Override with: --param <customer_id> --param '<timestamp with time zone>'
WITH target AS (
    SELECT customer_id,
           min(valid_from) + (max(valid_from) - min(valid_from)) / 2 AS as_of
    FROM dim_customers_scd2
    GROUP BY customer_id
    ORDER BY count(*) DESC, customer_id
    LIMIT 1
)
SELECT h.customer_id, t.as_of, h.email, h.full_name, h.country, h.tier, h.valid_from, h.valid_to
FROM dim_customers_scd2 h
JOIN target t ON h.customer_id = t.customer_id
WHERE NOT h.is_deleted
  AND h.valid_from <= t.as_of
  AND (h.valid_to IS NULL OR h.valid_to > t.as_of);
