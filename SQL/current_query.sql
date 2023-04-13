SELECT
  pid,
  age(clock_timestamp(), query_start),
  usename,
  application_name,
  wait_event,
  wait_event_type,
  query
FROM
  pg_stat_activity
WHERE
  state != 'idle'
    AND
  query NOT ILIKE '%pg_stat_activity%'
ORDER BY
  query_start DESC;
