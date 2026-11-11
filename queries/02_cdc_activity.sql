-- Change events captured per table and operation (c=insert, u=update, d=delete, r=snapshot).
SELECT source_table, op, count(*) AS events, min(lsn) AS first_lsn, max(lsn) AS last_lsn
FROM bronze_cdc_events
GROUP BY ALL
ORDER BY source_table, op;
