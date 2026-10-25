-- Shared effective settings for vacuum_needed and vacuum_queue.
-- INSERT triggers require PG13+; the maximum threshold and relallfrozen require PG18+.
WITH options AS (
    SELECT c.oid, c.relname, c.reltuples, c.relpages, n.nspname,
        to_jsonb(c) AS class_info,
        (SELECT json_object_agg(option_name, option_value)
         FROM pg_options_to_table(coalesce(c.reloptions, parent.reloptions))) AS opts
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_class parent ON c.relkind = 't' AND parent.reltoastrelid = c.oid
    WHERE c.relkind IN ('r', 'm', 't') AND c.relpersistence <> 't'
), settings AS (
    SELECT oid, relname, nspname, reltuples,
        greatest(reltuples, 0)::numeric AS tuples,
        current_setting('autovacuum')::boolean
            AND coalesce((opts ->> 'autovacuum_enabled')::boolean, true) AS autovacuum_enabled,
        coalesce((opts ->> 'autovacuum_vacuum_threshold')::numeric,
            current_setting('autovacuum_vacuum_threshold')::numeric) AS base_threshold,
        coalesce((opts ->> 'autovacuum_vacuum_scale_factor')::numeric,
            current_setting('autovacuum_vacuum_scale_factor')::numeric) AS scale_factor,
        coalesce((opts ->> 'autovacuum_vacuum_max_threshold')::numeric,
            current_setting('autovacuum_vacuum_max_threshold', true)::numeric) AS max_threshold,
        coalesce((opts ->> 'autovacuum_vacuum_insert_threshold')::numeric,
            current_setting('autovacuum_vacuum_insert_threshold', true)::numeric) AS insert_base,
        coalesce((opts ->> 'autovacuum_vacuum_insert_scale_factor')::numeric,
            current_setting('autovacuum_vacuum_insert_scale_factor', true)::numeric) AS insert_scale,
        1 - coalesce((class_info ->> 'relallfrozen')::numeric
            / nullif(relpages, 0), 0) AS unfrozen_fraction
    FROM options
), vacuum_settings AS (
    SELECT *,
        CASE WHEN max_threshold >= 0
            THEN least(max_threshold, base_threshold + scale_factor * tuples)
            ELSE base_threshold + scale_factor * tuples END AS vacuum_threshold,
        CASE WHEN insert_base >= 0
            THEN insert_base + insert_scale * tuples * greatest(0, least(1, unfrozen_fraction))
        END AS insert_threshold
    FROM settings
)
