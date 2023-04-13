SELECT
    state,
    wait_event,
    wait_event_type,
    client_addr,
    client_port,
    application_name,
    now() - xact_start as duration,
    query
FROM
    pg_stat_activity
WHERE
    state <> 'idle'
    AND (backend_xid IS NOT NULL
        OR backend_xmin IS NOT NULL)
    AND now() - xact_start > interval '5 min'
ORDER BY
    xact_start;
