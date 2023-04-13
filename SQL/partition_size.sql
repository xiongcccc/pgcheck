SELECT
    pi.inhparent::regclass AS parent_table_name,
    pg_size_pretty(sum(pg_total_relation_size(psu.relid))) AS total,
    pg_size_pretty(sum(pg_relation_size(psu.relid))) AS internal,
    pg_size_pretty(sum(pg_table_size(psu.relid) - pg_relation_size(psu.relid))) AS external, -- toast
    pg_size_pretty(sum(pg_indexes_size(psu.relid))) AS indexes
 FROM pg_catalog.pg_statio_user_tables psu
    JOIN pg_class pc ON psu.relname = pc.relname
    JOIN pg_database pd ON pc.relowner = pd.datdba
    JOIN pg_inherits pi ON pi.inhrelid = pc.oid
 WHERE pd.datname = 'postgres'
 GROUP BY pi.inhparent
 ORDER BY sum(pg_total_relation_size(psu.relid)) DESC;
