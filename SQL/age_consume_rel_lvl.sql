SELECT
    c.oid::regclass,
    age(c.relfrozenxid),
    pg_size_pretty(pg_total_relation_size(c.oid))
FROM
    pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE
    relkind IN ('r', 't', 'm')
    AND n.nspname NOT IN ('pg_toast')
ORDER BY
    2 DESC
LIMIT 50;
