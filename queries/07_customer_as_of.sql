-- Point in time: what did customer X look like at time T?
-- Variables: customer_id, as_of (defaults: the customer with the most versions, mid-history).
SELECT getvariable('customer_id') AS customer_id, getvariable('as_of')::TIMESTAMPTZ AS as_of,
       email, full_name, country, tier, valid_from, valid_to
FROM dim_customers_scd2
WHERE customer_id = getvariable('customer_id')::BIGINT
  AND NOT is_deleted
  AND valid_from <= getvariable('as_of')::TIMESTAMPTZ
  AND (valid_to IS NULL OR valid_to > getvariable('as_of')::TIMESTAMPTZ);

-- ...and that customer's full history, oldest first.
SELECT customer_id, email, country, tier, valid_from, valid_to, is_current, is_deleted, _lsn
FROM dim_customers_scd2
WHERE customer_id = getvariable('customer_id')::BIGINT
ORDER BY _lsn;
