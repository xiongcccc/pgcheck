SELECT
    pg_stat_user_tables.relname,
    pg_stat_user_tables.n_dead_tup,
    50 + 0.1 * pg_class.reltuples as vacuum_threshold,
    pg_stat_user_tables.n_live_tup,
    pg_stat_user_tables.n_tup_del,
    pg_stat_user_tables.n_tup_upd,
    pg_stat_user_tables.autovacuum_count,
    pg_stat_user_tables.last_vacuum,
    pg_stat_user_tables.last_autovacuum,
    now() as now,
    pg_class.reltuples,
    pg_stat_user_tables.n_dead_tup > (50 + 0.1 * pg_class.reltuples) as is_vacuum
 FROM
    pg_stat_user_tables INNER JOIN pg_class ON pg_stat_user_tables.relname = pg_class.relname
 ORDER BY
    pg_stat_user_tables.n_dead_tup > (50 + 0.1 * pg_class.reltuples) DESC;
