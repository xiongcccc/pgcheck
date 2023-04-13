SELECT
    datname,
    count(*) AS open,
    count(*) FILTER (WHERE state = 'active') AS active,
    count(*) FILTER (WHERE state = 'idle') AS idle,
    count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_trans
FROM
    pg_stat_activity
GROUP BY
    ROLLUP (1);
