-- Every silver order that has not changed since fct_orders was built must be
-- in the fact with the same amount, and no deleted order may be in the fact.
--
-- "Not changed since" uses an LSN watermark: the highest source LSN the fact
-- saw. A silver row whose _lsn is <= that watermark is in the same state now as
-- when the fact was built (any later change would carry a higher LSN), so the
-- check is exact even while the streaming pipeline keeps writing.
with watermark as (
    select max(source_lsn) as lsn from {{ ref('fct_orders') }}
),
stable_silver as (
    select s.order_id, s._is_deleted, cast(s.quantity * s.unit_price as decimal(18, 2)) as amount
    from {{ source('lakehouse', 'silver_orders_all') }} s, watermark w
    where s._lsn <= w.lsn
)
select coalesce(s.order_id, f.order_id) as order_id,
       s._is_deleted, s.amount as silver_amount, f.amount as fact_amount
from stable_silver s
full outer join {{ ref('fct_orders') }} f on f.order_id = s.order_id
where (s.order_id is not null and not s._is_deleted and (f.order_id is null or f.amount <> s.amount))
   or (s.order_id is not null and s._is_deleted and f.order_id is not null)
