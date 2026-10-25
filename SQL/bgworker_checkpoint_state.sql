SELECT
    checkpoints_timed + checkpoints_req AS total_checkpoints,
    pg_size_pretty(block_size * buffers_clean ) AS bgworker_total_writen,
    pg_size_pretty(block_size * buffers_checkpoint ) AS chkpointer_total_writen,
    pg_size_pretty(block_size * buffers_backend ) AS backend_total_writen,
    pg_size_pretty(block_size * buffers_checkpoint / NULLIF(checkpoints_timed + checkpoints_req, 0)) AS checkpoint_write_avg,
    EXTRACT(EPOCH FROM (now() - stats_reset)) / NULLIF(checkpoints_timed + checkpoints_req, 0) / 60 AS minutes_between_checkpoints,
    buffers_backend_fsync,
    stats_reset
FROM
    pg_stat_bgwriter,
    (
        SELECT current_setting('block_size')::int AS block_size) AS bs
