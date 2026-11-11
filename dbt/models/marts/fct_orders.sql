-- Order fact, grain = one row per order (current state).
-- The customer key points at the customer *version valid when the order was
-- placed* (point-in-time join on the SCD2 dimension), not today's version.
--   customer_match = 'as_of'    : a version was valid at ordered_at
--                    'earliest' : order predates the first captured version
--                                 (seed orders vs. the CDC snapshot time) -> first version
--                    'unknown'  : no valid version at all (every customer change quarantined)
with orders as (
    select * from {{ ref('stg_orders') }}
),
cust as (
    select * from {{ ref('dim_customer') }} where customer_sk <> -1
),
as_of as (
    select o.order_id, c.customer_sk
    from orders o
    join cust c
      on c.customer_id = o.customer_id
     and c.valid_from <= o.ordered_at
     and (c.valid_to is null or c.valid_to > o.ordered_at)
),
earliest as (
    select customer_id, customer_sk from cust where version_number = 1
)
select
    o.order_id,
    coalesce(a.customer_sk, e.customer_sk, -1)                         as customer_sk,
    case when a.customer_sk is not null then 'as_of'
         when e.customer_sk is not null then 'earliest'
         else 'unknown' end                                            as customer_match,
    coalesce(p.product_sk, -1)                                         as product_sk,
    cast(date_format(o.ordered_at, '%Y%m%d') as integer)               as order_date_key,
    o.status,
    o.quantity,
    o.unit_price,
    o.amount,
    case when s.counts_as_revenue then o.amount else 0 end             as net_revenue,
    o.ordered_at,
    cast(o.ordered_at as date)                                         as order_date,
    o.updated_at,
    o._lsn                                                             as source_lsn
from orders o
left join as_of a on a.order_id = o.order_id
left join earliest e on a.customer_sk is null and e.customer_id = o.customer_id
left join {{ ref('dim_product') }} p on p.product_id = o.product_id
left join {{ ref('dim_order_status') }} s on s.status = o.status
