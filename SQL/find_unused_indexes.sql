-- Credit: http://www.databasesoup.com/2014/04/new-new-index-bloat-query.html
-- Original: https://gist.github.com/jberkus/6b1bcaf7724dfc2a54f3

-- NOTES:

WITH table_scans AS (
    SELECT
        relid,
        idx_scan + seq_scan AS all_scans,
        (n_tup_ins + n_tup_upd + n_tup_del) AS writes,
        pg_relation_size(relid) AS table_size
    FROM
        pg_stat_user_tables
),
all_writes AS (
    SELECT
        sum(writes) AS total_writes
    FROM
        table_scans
),
indexes AS (
    SELECT
        idx_stat.relid,
        idx_stat.indexrelid,
        idx_stat.schemaname,
        idx_stat.relname AS tablename,
        idx_stat.indexrelname AS indexname,
        idx_stat.idx_scan,
        pg_relation_size(idx_stat.indexrelid) AS index_bytes,
        indexdef ~* 'USING btree' AS idx_is_btree
    FROM
        pg_stat_user_indexes AS idx_stat
        JOIN pg_index
            USING (indexrelid)
        JOIN pg_indexes AS indexes
            ON idx_stat.schemaname = indexes.schemaname
            AND idx_stat.relname = indexes.tablename
            AND idx_stat.indexrelname = indexes.indexname
    WHERE
        NOT pg_index.indisprimary
        AND NOT pg_index.indisunique
        AND NOT pg_index.indisexclusion
        AND NOT EXISTS (
            SELECT
                1
            FROM
                pg_constraint c
            WHERE
                c.conindid = idx_stat.indexrelid
        )
        AND pg_relation_size(idx_stat.indexrelid) >= 32768
), index_ratios AS (
    SELECT
        schemaname,
        tablename,
        indexname,
        idx_scan,
        all_scans,
        round((
            CASE
                WHEN all_scans = 0 THEN 0.0::numeric
                ELSE idx_scan::numeric / all_scans * 100
            END), 2) AS index_scan_pct,
        writes,
        round((
            CASE
                WHEN writes = 0 THEN idx_scan::numeric
                ELSE idx_scan::numeric / writes
            END), 2) AS scans_per_write,
        pg_size_pretty(index_bytes) AS index_size,
        pg_size_pretty(table_size) AS table_size,
        idx_is_btree,
        index_bytes
    FROM indexes
    JOIN table_scans
    USING (relid)
), index_groups AS (
    SELECT 'Never Used Indexes' AS reason, *, 1 AS grp
    FROM index_ratios
    WHERE
        idx_scan = 0
        AND idx_is_btree
    UNION ALL
    SELECT 'Low Scans, High Writes' AS reason, *, 2 AS grp
    FROM index_ratios
    WHERE
        scans_per_write <= 1
        AND index_scan_pct < 10
        AND idx_scan > 0
        AND writes > 100
        AND idx_is_btree
    UNION ALL
    SELECT 'Seldom Used Large Indexes' AS reason, *, 3 AS grp
    FROM index_ratios
    WHERE
        index_scan_pct < 5
        AND scans_per_write > 1
        AND idx_scan > 0
        AND idx_is_btree
        AND index_bytes > 100000000
    UNION ALL
    SELECT 'High-Write Large Non-Btree' AS reason, index_ratios.*, 4 AS grp
    FROM index_ratios, all_writes
    WHERE
        (writes::numeric / (coalesce(total_writes, 0) + 1)) > 0.02
        AND NOT idx_is_btree
        AND index_bytes > 100000000
)
SELECT
    reason,
    schemaname,
    tablename,
    indexname,
    idx_scan,
    writes,
    index_scan_pct,
    scans_per_write,
    index_size,
    table_size
FROM index_groups
ORDER BY
    grp,
    index_bytes DESC;
