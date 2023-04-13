SELECT
    client_addr AS client,
    usename AS user,
    application_name AS name,
    state,
    sync_state AS mode,
    write_lag,
    flush_lag,
    replay_lag
FROM
    pg_stat_replication;
