SELECT
    checkpoints_timed + checkpoints_req AS total_checkpoints,
    pg_size_pretty(block_size * buffers_clean) AS bgworker_total_writen,
    pg_size_pretty(block_size * buffers_checkpoint) AS chkpointer_total_writen,
    pg_size_pretty(block_size * buffers_backend) AS backend_total_writen,
    pg_size_pretty(block_size * buffers_checkpoint / (checkpoints_timed + checkpoints_req)) AS checkpoint_write_avg,
    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) / (checkpoints_timed + checkpoints_req) / 60 AS minutes_between_checkpoints,
    buffers_backend_fsync
FROM
    pg_stat_bgwriter,
    (
        SELECT
            current_setting('block_size')::int AS block_size) AS bs
