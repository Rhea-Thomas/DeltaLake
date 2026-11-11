-- Why records were quarantined, per table and rule.
SELECT source_table,
       split_part(err, ':', 1) AS rule,
       count(*) AS failures,
       any_value(err) AS example
FROM quarantine, unnest(errors) AS t(err)
GROUP BY ALL
ORDER BY failures DESC;
