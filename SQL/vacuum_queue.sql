{{vacuum_settings}}
SELECT
    CASE WHEN s.relid IS NOT NULL THEN format('%I.%I', s.schemaname, s.relname)
        ELSE format('[database %I, relation oid %s]', p.datname, p.relid) END AS relation,
    round(100 * s.n_dead_tup::numeric / nullif(v.tuples, 0), 2) AS dead_tup_pct,
    v.reltuples,
    s.n_dead_tup,
    v.vacuum_threshold,
    (to_jsonb(s) ->> 'n_ins_since_vacuum')::bigint AS inserts_since_vacuum,
    v.insert_threshold,
    v.autovacuum_enabled,
    format('vt: %s, vsf: %s, %s', v.base_threshold, v.scale_factor,
        CASE WHEN v.autovacuum_enabled THEN 'enabled' ELSE 'DISABLED' END) AS effective_settings,
    CASE WHEN s.last_autovacuum > coalesce(s.last_vacuum, '-infinity'::timestamptz)
        THEN left(s.last_autovacuum::text, 19) || ' (auto)'
        WHEN s.last_vacuum IS NOT NULL THEN left(s.last_vacuum::text, 19) || ' (manual)'
    END AS last_vacuumed,
    coalesce(p.phase, CASE WHEN NOT v.autovacuum_enabled THEN 'autovacuum disabled'
        ELSE '~~~ in queue ~~~' END) AS status,
    p.pid,
    CASE WHEN a.query ~* '^autovacuum.*to prevent wraparound' THEN 'wraparound'
        WHEN a.backend_type = 'autovacuum worker' THEN 'regular'
        WHEN a.pid IS NOT NULL THEN 'user' END AS mode,
    a.wait_event_type || '.' || a.wait_event AS waiting,
    round(100.0 * p.heap_blks_scanned / nullif(p.heap_blks_total, 0), 1) AS scanned_pct,
    round(100.0 * p.heap_blks_vacuumed / nullif(p.heap_blks_total, 0), 1) AS vacuumed_pct,
    p.index_vacuum_count,
    CASE WHEN s.relid IS NOT NULL AND p.pid IS NOT NULL THEN
        (SELECT count(*) FROM pg_index WHERE indrelid = s.relid)
    END AS index_count
FROM pg_stat_all_tables s
JOIN vacuum_settings v ON v.oid = s.relid
FULL JOIN pg_stat_progress_vacuum p ON p.relid = s.relid AND p.datname = current_database()
LEFT JOIN pg_stat_activity a ON a.pid = p.pid
WHERE p.pid IS NOT NULL
    OR s.n_dead_tup > v.vacuum_threshold
    OR (to_jsonb(s) ->> 'n_ins_since_vacuum')::bigint > v.insert_threshold
ORDER BY status, relation;
