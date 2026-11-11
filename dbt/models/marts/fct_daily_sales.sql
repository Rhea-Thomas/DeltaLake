-- Daily sales fact, grain = date x product x customer tier at order time.
-- Aggregated from fct_orders so the two facts always agree.
select
    f.order_date_key,
    f.product_sk,
    c.tier                       as customer_tier_at_order,
    count(*)                     as orders,
    sum(f.quantity)              as units,
    sum(f.amount)                as gross_amount,
    sum(f.net_revenue)           as net_revenue,
    count_if(f.status = 'cancelled') as cancelled_orders
from {{ ref('fct_orders') }} f
join {{ ref('dim_customer') }} c on c.customer_sk = f.customer_sk
group by 1, 2, 3
