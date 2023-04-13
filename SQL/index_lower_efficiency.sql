SELECT s.relname AS table_name,
       indexrelname AS index_name,
       i.indisunique,
       idx_scan AS idx_scans
FROM   pg_catalog.pg_stat_user_indexes s,
       pg_index i
WHERE  i.indexrelid = s.indexrelid;
