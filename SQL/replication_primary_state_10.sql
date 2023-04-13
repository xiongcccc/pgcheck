SELECT
    client_addr AS client,
    usename AS user,
    application_name AS name,
    state,
    sync_state AS mode,
    (pg_xlog_location_diff(pg_current_xlog_location(), sent_location) / 1024)::bigint AS pending,
    (pg_xlog_location_diff(sent_location, write_location) / 1024)::bigint AS write,
    (pg_xlog_location_diff(write_location, flush_location) / 1024)::bigint AS flush,
    (pg_xlog_location_diff(flush_location, replay_location) / 1024)::bigint AS replay,
    (pg_xlog_location_diff(pg_current_xlog_location(), replay_location))::bigint / 1024 AS total_lag
FROM
    pg_stat_replication;
