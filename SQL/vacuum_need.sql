{{vacuum_settings}}
SELECT
    s.schemaname,
    s.relname,
    s.n_dead_tup,
    v.vacuum_threshold,
    (to_jsonb(s) ->> 'n_ins_since_vacuum')::bigint AS inserts_since_vacuum,
    v.insert_threshold,
    v.autovacuum_enabled,
    s.n_live_tup,
    s.n_tup_del,
    s.n_tup_upd,
    s.autovacuum_count,
    s.last_vacuum,
    s.last_autovacuum,
    now() AS now,
    v.reltuples,
    coalesce(s.n_dead_tup > v.vacuum_threshold, false)
        OR coalesce((to_jsonb(s) ->> 'n_ins_since_vacuum')::bigint > v.insert_threshold, false) AS is_vacuum
FROM pg_stat_user_tables s
JOIN vacuum_settings v ON v.oid = s.relid
ORDER BY is_vacuum DESC, s.n_dead_tup DESC, s.relid;
