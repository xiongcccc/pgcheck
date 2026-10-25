WITH raw AS (
    SELECT
        s.*,
        to_jsonb(s) AS stat
    FROM
        pg_stat_io AS s
), io AS (
    SELECT
        backend_type,
        object,
        context,
        reads,
        coalesce((stat ->> 'read_bytes')::numeric, reads::numeric * coalesce((stat ->> 'op_bytes')::numeric, current_setting('block_size')::numeric)) AS read_bytes,
        read_time,
        writes,
        coalesce((stat ->> 'write_bytes')::numeric, writes::numeric * coalesce((stat ->> 'op_bytes')::numeric, current_setting('block_size')::numeric)) AS write_bytes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        coalesce((stat ->> 'extend_bytes')::numeric, extends::numeric * coalesce((stat ->> 'op_bytes')::numeric, current_setting('block_size')::numeric)) AS extend_bytes,
        extend_time,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
    FROM
        raw
)
SELECT
    backend_type,
    object,
    context,
    reads,
    pg_size_pretty(read_bytes) AS read_bytes,
    read_time,
    writes,
    pg_size_pretty(write_bytes) AS write_bytes,
    write_time,
    writebacks,
    writeback_time,
    extends,
    pg_size_pretty(extend_bytes) AS extend_bytes,
    extend_time,
    pg_size_pretty(coalesce(read_bytes, 0) + coalesce(write_bytes, 0) + coalesce(extend_bytes, 0)) AS io_bytes,
    hits,
    evictions,
    reuses,
    fsyncs,
    fsync_time,
    stats_reset
FROM
    io
ORDER BY
    backend_type,
    object,
    context;
