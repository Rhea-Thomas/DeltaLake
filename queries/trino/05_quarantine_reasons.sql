-- Why records were quarantined, per table and rule.
SELECT source_table, split_part(err, ':', 1) AS rule, count(*) AS failures, arbitrary(err) AS example
FROM quarantine CROSS JOIN UNNEST(errors) AS t(err)
GROUP BY source_table, split_part(err, ':', 1)
ORDER BY failures DESC;
