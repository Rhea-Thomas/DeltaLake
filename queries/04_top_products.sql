-- Best-selling products and remaining stock.
SELECT p.product_id, p.name, p.category, p.price, p.stock,
       sum(o.quantity) AS units_sold,
       sum(o.quantity * o.unit_price) AS revenue
FROM orders o
JOIN products p USING (product_id)
WHERE o.status <> 'cancelled'
GROUP BY ALL
ORDER BY revenue DESC
LIMIT 10;
