SELECT
    datname AS database_name,
    pg_size_pretty(pg_database_size(datname)) AS database_size,
    100 * blks_hit / (blks_hit + blks_read) || ' %' AS cache_hit_ratio,
    100 * xact_commit / (xact_commit + xact_rollback) || ' %' AS commit_ratio,
    conflicts,
    temp_files,
    pg_size_pretty(temp_bytes) AS temp_bytes,
    deadlocks,
    checksum_failures,
    blk_read_time,
    blk_write_time,
    session_time,
    active_time,
    idle_in_transaction_time,
    sessions,
    sessions_abandoned,
    sessions_fatal,
    sessions_killed
FROM
    pg_stat_database
WHERE (blks_hit + blks_read) > 0
    AND datname NOT LIKE '%template%';
