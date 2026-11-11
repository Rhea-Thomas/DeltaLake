-- Small, fixed lookup. Kept as a model (CREATE TABLE AS) rather than a dbt
-- seed: seeds need INSERT into an existing table, which Trino's Delta
-- connector does not allow on a local file system (see trino/catalog).
select * from (
    values
        ('placed',    1, false, true,  'Order received'),
        ('paid',      2, false, true,  'Payment captured'),
        ('shipped',   3, false, true,  'Handed to carrier'),
        ('delivered', 4, true,  true,  'Received by customer'),
        ('cancelled', 5, true,  false, 'Cancelled before shipping')
) as t (status, stage_order, is_terminal, counts_as_revenue, description)
