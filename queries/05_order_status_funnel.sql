-- Order pipeline: how many orders sit in each status.
SELECT status, count(*) AS orders,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM orders
GROUP BY status
ORDER BY CASE status WHEN 'placed' THEN 1 WHEN 'paid' THEN 2 WHEN 'shipped' THEN 3
                     WHEN 'delivered' THEN 4 ELSE 5 END;
