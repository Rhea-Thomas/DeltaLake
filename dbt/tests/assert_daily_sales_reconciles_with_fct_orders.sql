-- The aggregate fact must add up exactly to the transaction fact.
select *
from (select count(*) as orders, sum(amount) as gross, sum(net_revenue) as net from {{ ref('fct_orders') }}) a
cross join (select sum(orders) as orders, sum(gross_amount) as gross, sum(net_revenue) as net
            from {{ ref('fct_daily_sales') }}) b
where a.orders <> b.orders or a.gross <> b.gross or a.net <> b.net
