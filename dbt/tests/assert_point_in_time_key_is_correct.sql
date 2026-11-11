-- Every 'as_of' fact row must point at a version whose interval contains the
-- order time; every 'earliest' row must predate that first version.
select f.order_id, f.customer_match, f.ordered_at, c.valid_from, c.valid_to
from {{ ref('fct_orders') }} f
join {{ ref('dim_customer') }} c on c.customer_sk = f.customer_sk
where (f.customer_match = 'as_of'
       and not (c.valid_from <= f.ordered_at and (c.valid_to is null or c.valid_to > f.ordered_at)))
   or (f.customer_match = 'earliest' and not (f.ordered_at < c.valid_from))
