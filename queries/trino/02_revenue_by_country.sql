-- Revenue from non-cancelled orders by the customer's current country.
SELECT c.country, count(DISTINCT c.customer_id) AS customers, count(*) AS orders,
       sum(o.quantity * o.unit_price) AS revenue
FROM orders o JOIN customers c ON c.customer_id = o.customer_id
WHERE o.status <> 'cancelled'
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 10;
