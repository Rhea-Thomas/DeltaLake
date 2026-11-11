-- Live orders with the money measure computed once.
select
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    cast(quantity * unit_price as decimal(18, 2)) as amount,
    status,
    ordered_at,
    updated_at,
    _lsn
from {{ source('lakehouse', 'silver_orders_all') }}
where not _is_deleted
