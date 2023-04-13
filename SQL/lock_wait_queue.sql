WITH RECURSIVE t_wait AS (
    SELECT
        a.locktype,
        a.database,
        a.relation,
        a.page,
        a.tuple,
        a.classid,
        a.objid,
        a.objsubid,
        a.pid,
        a.virtualtransaction,
        a.virtualxid,
        a.transactionid
    FROM
        pg_locks a
    WHERE
        NOT a.granted
),
t_run AS (
    SELECT
        a.mode,
        a.locktype,
        a.database,
        a.relation,
        a.page,
        a.tuple,
        a.classid,
        a.objid,
        a.objsubid,
        a.pid,
        a.virtualtransaction,
        a.virtualxid,
        a.transactionid,
        b.query,
        b.xact_start,
        b.query_start,
        b.usename,
        b.datname
    FROM
        pg_locks a,
        pg_stat_activity b
    WHERE
        a.pid = b.pid
        AND a.granted
),
w AS (
    SELECT
        r.pid r_pid,
        w.pid w_pid
    FROM
        t_wait w,
        t_run r
    WHERE
        r.locktype IS NOT DISTINCT FROM w.locktype
        AND r.database IS NOT DISTINCT FROM w.database
        AND r.relation IS NOT DISTINCT FROM w.relation
        AND r.page IS NOT DISTINCT FROM w.page
        AND r.tuple IS NOT DISTINCT FROM w.tuple
        AND r.classid IS NOT DISTINCT FROM w.classid
        AND r.objid IS NOT DISTINCT FROM w.objid
        AND r.objsubid IS NOT DISTINCT FROM w.objsubid
        AND r.transactionid IS NOT DISTINCT FROM w.transactionid
        AND r.virtualxid IS NOT DISTINCT FROM w.virtualxid
),
c (
    waiter, holder, root_holder, path, deep
) AS (
    SELECT
        w_pid,
        r_pid,
        r_pid,
        w_pid || '->' || r_pid,
        1
    FROM
        w
    UNION
    SELECT
        w_pid,
        r_pid,
        c.holder,
        w_pid || '->' || c.path,
        c.deep + 1
    FROM
        w t,
        c
    WHERE
        t.r_pid = c.waiter
)
SELECT
    t1.waiter,
    t1.holder,
    t1.root_holder,
    path,
    t1.deep
FROM
    c t1
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            c t2
        WHERE
            t2.path ~ t1.path
            AND t1.path <> t2.path)
ORDER BY
    root_holder;
