select product_id, sku, name, category, price, stock, updated_at, _lsn
from {{ source('lakehouse', 'silver_products_all') }}
where not _is_deleted
