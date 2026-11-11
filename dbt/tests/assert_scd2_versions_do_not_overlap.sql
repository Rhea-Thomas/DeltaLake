-- No two versions of the same customer may be valid at the same instant.
select a.customer_id, a.customer_sk, b.customer_sk as overlapping_sk
from {{ ref('dim_customer') }} a
join {{ ref('dim_customer') }} b
  on a.customer_id = b.customer_id
 and a.customer_sk < b.customer_sk
 and a.valid_from < coalesce(b.valid_to, timestamp '9999-12-31 00:00:00 UTC')
 and b.valid_from < coalesce(a.valid_to, timestamp '9999-12-31 00:00:00 UTC')
 and a.valid_from < coalesce(a.valid_to, timestamp '9999-12-31 00:00:00 UTC')  -- ignore zero-length versions
 and b.valid_from < coalesce(b.valid_to, timestamp '9999-12-31 00:00:00 UTC')  -- (two changes in one txn)
where a.customer_sk <> -1
