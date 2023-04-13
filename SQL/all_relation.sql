SELECT
   *,
   pg_size_pretty(table_bytes) AS table,
   pg_size_pretty(index_bytes) AS index,
   pg_size_pretty(total_bytes) AS total
 FROM (
   SELECT
     *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
   FROM (
     SELECT
       c.oid,
       c.relfilenode,
       case when c.oid=c.relfilenode then 'NO' else 'YES' end as has_been_rewrite,
       nspname AS table_schema,
       relname AS table_name,
       c.reltuples AS row_estimate,
       pg_total_relation_size(c.oid) AS total_bytes,
       pg_indexes_size(c.oid) AS index_bytes,
       pg_total_relation_size(reltoastrelid) AS toast_bytes
     FROM
       pg_class c
       LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE relkind = 'r'
   ) a
 ) a
 WHERE table_schema = 'public'
 ORDER BY total_bytes DESC;
