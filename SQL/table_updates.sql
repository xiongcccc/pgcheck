-- Adapted from fabiotr/pg_scripts, tables_changes_91up.sql and fillfactor_93up.sql.
-- Source: https://github.com/fabiotr/pg_scripts/tree/c35755bb7a464ffdbbb36f0339ec8d0a9d0865ad/sql
-- Copyright (c) 2023, Fabio Telles Rodriguez. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
-- Changes: cumulative updates, numeric HOT ratio and optional PG16+ cross-page count.
WITH ranked AS (
    SELECT s.*, to_jsonb(s) AS stat
    FROM pg_stat_user_tables AS s
    WHERE n_tup_upd > 0
    ORDER BY n_tup_upd DESC, relid
    LIMIT 10
)
SELECT
    format('%I.%I', s.schemaname, s.relname) AS relation,
    pg_size_pretty(pg_total_relation_size(s.relid)) AS total_size,
    s.n_tup_ins, s.n_tup_upd, s.n_tup_del,
    round(100 * s.n_tup_hot_upd::numeric / nullif(s.n_tup_upd, 0), 2) AS hot_update_pct,
    (s.stat ->> 'n_tup_newpage_upd')::bigint AS newpage_updates,
    coalesce((SELECT option_value::int FROM pg_options_to_table(c.reloptions)
        WHERE option_name = 'fillfactor'), 100) AS fillfactor
FROM ranked AS s
JOIN pg_class AS c ON c.oid = s.relid
ORDER BY s.n_tup_upd DESC, s.relid;
