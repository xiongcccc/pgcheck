-- Adapted from fabiotr/pg_scripts, io_table_heap_91up.sql and io_table_index_91up.sql.
-- Source: https://github.com/fabiotr/pg_scripts/tree/c35755bb7a464ffdbbb36f0339ec8d0a9d0865ad/sql
-- Copyright (c) 2023, Fabio Telles Rodriguez. BSD-3-Clause; see THIRD_PARTY_NOTICES.md.
-- Changes: combined heap/index/TOAST reads, cumulative counters and Top-N size lookup.
WITH io AS (
    SELECT relid, schemaname, relname,
        coalesce(heap_blks_read, 0)::numeric + coalesce(idx_blks_read, 0)::numeric
            + coalesce(toast_blks_read, 0)::numeric + coalesce(tidx_blks_read, 0)::numeric AS reads,
        coalesce(heap_blks_hit, 0)::numeric + coalesce(idx_blks_hit, 0)::numeric
            + coalesce(toast_blks_hit, 0)::numeric + coalesce(tidx_blks_hit, 0)::numeric AS hits,
        heap_blks_read, idx_blks_read, toast_blks_read, tidx_blks_read
    FROM pg_statio_user_tables
), ranked AS (
    SELECT *, round(100 * reads / nullif(sum(reads) OVER (), 0), 2) AS read_share_pct
    FROM io
    WHERE reads + hits > 0
    ORDER BY reads DESC, hits DESC, relid
    LIMIT 10
)
SELECT
    format('%I.%I', schemaname, relname) AS relation,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(reads * current_setting('block_size')::numeric) AS bytes_read,
    read_share_pct,
    round(100 * hits / nullif(hits + reads, 0), 2) AS cache_hit_pct,
    heap_blks_read, idx_blks_read, toast_blks_read, tidx_blks_read
FROM ranked
ORDER BY reads DESC, hits DESC, relid;
