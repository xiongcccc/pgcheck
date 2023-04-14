SELECT
    sub.table,
    pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size,
    (array_agg(idx))[1] AS idx1,
    (array_agg(idx))[2] AS idx2,
    (array_agg(idx))[3] AS idx3,
    (array_agg(idx))[4] AS idx4
FROM (
    SELECT
        s.schemaname || '.' || s.relname AS table,
        i.indexrelid::regclass AS idx,
        (i.indrelid::text || E'\n' || i.indclass::text || E'\n' || i.indkey::text || E'\n' || coalesce(i.indexprs::text, '') || E'\n' || coalesce(i.indpred::text, '')) AS key
    FROM
        pg_index i
        JOIN pg_stat_all_indexes s ON i.indexrelid = s.indexrelid) sub
GROUP BY
    sub.table,
    key
HAVING
    count(*) > 1
ORDER BY
    sum(pg_relation_size(idx)) DESC;
