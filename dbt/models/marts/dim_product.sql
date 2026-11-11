-- Product dimension (SCD Type 1: current attributes only). Row -1 = unknown
-- member, e.g. orders for a product whose every change was quarantined.
select
    {{ surrogate_key(['product_id']) }} as product_sk,
    product_id, sku, name, category,
    price as current_price,
    stock as current_stock
from {{ ref('stg_products') }}
union all
select cast(-1 as bigint), cast(null as bigint), 'unknown', 'Unknown product', 'unknown',
       cast(null as decimal(10, 2)), cast(null as integer)
