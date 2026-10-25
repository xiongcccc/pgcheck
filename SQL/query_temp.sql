-- Adapted from fabiotr/pg_scripts, statements_temp_17up.sql.
-- Source: https://github.com/fabiotr/pg_scripts/blob/c35755bb7a464ffdbbb36f0339ec8d0a9d0865ad/sql/statements_temp_17up.sql
-- Copyright (c) 2023, Fabio Telles Rodriguez. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
-- Changes: separate cumulative reads/writes, guarded averages and optional timing.
WITH statements AS (
    SELECT s.*, to_jsonb(s) AS stat
    FROM {{pg_stat_statements}} AS s
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
), sizes AS (
    SELECT *,
        temp_blks_read::numeric * current_setting('block_size')::numeric AS read_bytes,
        temp_blks_written::numeric * current_setting('block_size')::numeric AS write_bytes
    FROM statements
    WHERE calls > 0 AND coalesce((stat ->> 'toplevel')::boolean, true)
)
SELECT
    userid::regrole AS username,
    queryid,
    calls,
    pg_size_pretty(read_bytes) AS temp_read,
    pg_size_pretty(write_bytes) AS temp_written,
    pg_size_pretty((read_bytes + write_bytes) / nullif(calls, 0)) AS temp_io_per_call,
    CASE WHEN current_setting('track_io_timing')::boolean THEN
        round((stat ->> 'temp_blk_read_time')::numeric + (stat ->> 'temp_blk_write_time')::numeric, 2)
    END AS temp_io_ms,
    (stat ->> 'stats_since')::timestamptz AS stats_since,
    left(regexp_replace(coalesce(query, '<unavailable>'), E'[\\n\\r\\t ]+', ' ', 'g'), 160) AS query
FROM sizes
WHERE read_bytes + write_bytes > 0
ORDER BY read_bytes + write_bytes DESC, calls DESC, userid, queryid
LIMIT 10;
