-- Point-in-time join: tier the customer had when they ordered vs. now.
SELECT h.tier AS tier_at_order,
       count(*) AS orders,
       sum(o.quantity * o.unit_price) AS revenue_at_order_time,
       count_if(cur.tier IS DISTINCT FROM h.tier) AS orders_whose_tier_changed_since
FROM orders o
JOIN dim_customers_scd2 h
  ON h.customer_id = o.customer_id
 AND NOT h.is_deleted
 AND h.valid_from <= o.ordered_at
 AND (h.valid_to IS NULL OR h.valid_to > o.ordered_at)
LEFT JOIN customers cur ON cur.customer_id = o.customer_id
WHERE o.status <> 'cancelled'
GROUP BY h.tier
ORDER BY revenue_at_order_time DESC;
