-- One row per valid customer version; tombstones (deletes) dropped: a deleted
-- customer simply has no version valid after the delete.
select customer_id, email, full_name, country, tier,
       valid_from, valid_to, is_current, _lsn
from {{ source('lakehouse', 'dim_customers_scd2') }}
where not is_deleted
