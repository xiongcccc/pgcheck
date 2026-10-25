-- Adapted from fabiotr/pg_scripts, statements_exec_17up.sql.
-- Source: https://github.com/fabiotr/pg_scripts/blob/c35755bb7a464ffdbbb36f0339ec8d0a9d0865ad/sql/statements_exec_17up.sql
-- Copyright (c) 2023, Fabio Telles Rodriguez. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
-- Changes: cumulative totals, capability-based fields, top-level scope and safe arithmetic.
WITH statements AS (
    SELECT s.*, to_jsonb(s) AS stat
    FROM {{pg_stat_statements}} AS s
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
), normalized AS (
    SELECT *,
        coalesce((stat ->> 'total_exec_time')::numeric, (stat ->> 'total_time')::numeric) AS exec_ms,
        coalesce((stat ->> 'mean_exec_time')::numeric, (stat ->> 'mean_time')::numeric) AS mean_ms
    FROM statements
    WHERE calls > 0 AND coalesce((stat ->> 'toplevel')::boolean, true)
)
SELECT
    userid::regrole AS username,
    queryid,
    calls,
    round(exec_ms, 2) AS total_exec_ms,
    round(100 * exec_ms / nullif(sum(exec_ms) OVER (), 0), 2) AS exec_time_share_pct,
    round(mean_ms, 3) AS mean_exec_ms,
    round(rows::numeric / nullif(calls, 0), 2) AS rows_per_call,
    (stat ->> 'stats_since')::timestamptz AS stats_since,
    left(regexp_replace(coalesce(query, '<unavailable>'), E'[\\n\\r\\t ]+', ' ', 'g'), 160) AS query
FROM normalized
ORDER BY exec_ms DESC, calls DESC, userid, queryid
LIMIT 10;
