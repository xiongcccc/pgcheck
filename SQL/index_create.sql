SELECT
    now(),
    query_start AS started_at,
    now() - query_start AS query_duration,
    format('[%s] %s', a.pid, a.query) AS pid_and_query,
    index_relid::regclass AS index_name,
    relid::regclass AS table_name,
    (pg_size_pretty(pg_relation_size(relid))) AS table_size,
    phase,
    nullif (wait_event_type, '') || ': ' || wait_event AS wait_type_and_event,
    current_locker_pid,
    (
        SELECT
            nullif (
            LEFT (query, 150), '') || '...'
        FROM
            pg_stat_activity a
        WHERE
            a.pid = current_locker_pid) AS current_locker_query,
    format('%s (%s of %s)', coalesce((round(100 * lockers_done::numeric / nullif (lockers_total, 0), 2))::text || '%', 'N/A'), coalesce(lockers_done::text, '?'), coalesce(lockers_total::text, '?')) AS lockers_progress,
    format('%s (%s of %s)', coalesce((round(100 * blocks_done::numeric / nullif (blocks_total, 0), 2))::text || '%', 'N/A'), coalesce(blocks_done::text, '?'), coalesce(blocks_total::text, '?')) AS blocks_progress,
    format('%s (%s of %s)', coalesce((round(100 * tuples_done::numeric / nullif (tuples_total, 0), 2))::text || '%', 'N/A'), coalesce(tuples_done::text, '?'), coalesce(tuples_total::text, '?')) AS tuples_progress,
    format('%s (%s of %s)', coalesce((round(100 * partitions_done::numeric / nullif (partitions_total, 0), 2))::text || '%', 'N/A'), coalesce(partitions_done::text, '?'), coalesce(partitions_total::text, '?')) AS partitions_progress
FROM
    pg_stat_progress_create_index p
    LEFT JOIN pg_stat_activity a ON a.pid = p.pid;
