SELECT
    wait_event,
    wait_event_type,
    count(*)
FROM
    pg_stat_activity
GROUP BY
    1,
    2;
