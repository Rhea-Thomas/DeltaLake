-- Customer dimension, SCD Type 2: one row per version, keyed by a surrogate
-- key so facts can point at the version that was valid when they happened.
-- Row -1 is the standard "unknown member" for facts with no matching version.
with versions as (
    select
        {{ surrogate_key(['customer_id', '_lsn']) }} as customer_sk,
        customer_id, email, full_name, country, tier,
        valid_from, valid_to, is_current,
        row_number() over (partition by customer_id order by _lsn) as version_number,
        _lsn as source_lsn
    from {{ ref('stg_customer_versions') }}
)
select * from versions
union all
select cast(-1 as bigint), cast(null as bigint), 'unknown', 'Unknown customer', 'XX', 'unknown',
       cast(null as timestamp(3) with time zone), cast(null as timestamp(3) with time zone),
       false, 0, cast(null as bigint)
