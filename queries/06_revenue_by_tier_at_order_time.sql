-- Why SCD2 matters: attribute each order to the tier the customer had WHEN THEY
-- ORDERED (point-in-time join), versus the tier they have today.
WITH attributed AS (
    SELECT o.order_id, o.quantity * o.unit_price AS amount,
           h.tier AS tier_at_order,
           cur.tier AS tier_now
    FROM orders o
    JOIN dim_customers_scd2 h
      ON h.customer_id = o.customer_id
     AND NOT h.is_deleted
     AND h.valid_from <= o.ordered_at
     AND (h.valid_to IS NULL OR h.valid_to > o.ordered_at)
    LEFT JOIN customers cur ON cur.customer_id = o.customer_id
    WHERE o.status <> 'cancelled'
)
SELECT tier_at_order,
       count(*)                                   AS orders,
       sum(amount)                                AS revenue_at_order_time,
       count(*) FILTER (WHERE tier_now IS DISTINCT FROM tier_at_order) AS orders_whose_tier_changed_since
FROM attributed
GROUP BY tier_at_order
ORDER BY revenue_at_order_time DESC;
