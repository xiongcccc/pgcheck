#!/bin/bash
# Author: xiongcc
# Date: 2024/04/03

# environment check
which psql > /dev/null 2>/dev/null
if [ $? -ne 0  ]; then
    echo "Fatal! please check your environment variables!"
    exit 1
fi

# current directory
current_dir=$(cd $(dirname $0); pwd)

# PostgreSQL current major version
current_version=$(psql --version | awk -F " " '{print $3}' | cut -d '.' -f 1 | head -n 1)

# utility usage info
function print_help_info() {
    echo "Description: The utility is used to collect specified information"
    echo "Current Version: 1.0.4"
    echo "Usage:"
    echo " ./pgcheck relation database schema         : list information about tables and indexes in the specified schema"
    echo " ./pgcheck relconstraint database relname   : list all constraint corresponding to the specified table"
    echo " ./pgcheck alltoast database schema         : list all toasts and their corresponding tables"
    echo " ./pgcheck reltoast database relname        : list the toast information of the specified table"
    echo " ./pgcheck dbstatus                         : list all database status and statistics"
    echo " ./pgcheck index_bloat database             : index bloat information (estimated value)"
    echo " ./pgcheck index_duplicate database         : index duplicate information"
    echo " ./pgcheck index_low database               : index low efficiency information"
    echo " ./pgcheck index_state database             : index detail information"
    echo " ./pgcheck lock database                    : lock wait queue and lock wait state"
    echo " ./pgcheck checkpoint                       : background and checkpointer state"
    echo " ./pgcheck freeze database                  : database transaction id consuming state and detail"
    echo " ./pgcheck replication                      : streaming replication (physical) state"
    echo " ./pgcheck connections database             : database connections and current query"
    echo " ./pgcheck long_transaction database        : long transaction detail"
    echo " ./pgcheck relation_bloat database          : relation bloat information (estimated value)"
    echo " ./pgcheck vacuum_state database            : current vacuum progress information"
    echo " ./pgcheck vacuum_need database             : show tables that need vacuum"
    echo " ./pgcheck index_create database            : index create progress information"
    echo " ./pgcheck wal_archive                      : wal archive progress information"
    echo " ./pgcheck wal_generate wal_path            : wal generate speed (you should provide extra wal directory)"
    echo " ./pgcheck wait_event database              : wait event and wait event type"
    echo " ./pgcheck partition database               : native and inherit partition info (estimated value)"
    echo " ./pgcheck object database user             : get the objects owned by the user in the specified database"
    echo " ./pgcheck --help or -h                     : print this help information"

    echo ""
    echo " Author: xiongcc@PostgreSQL学徒, github: https://github.com/xiongcccc."
    echo " If you have any feedback or suggestions, feel free to contact with me."
    echo " Email: xiongcc_1994@126.com/xiongcc_1994@outlook.com. Wechat: _xiongcc"
    echo ""
}

# get database name
if [ "$1" = '--help' -o "$1" = '-h' -o "$1" = 'help' -o "$1" = 'h' -o $# -lt 1 ];then
    print_help_info
    exit 1
elif [ "$1" = 'checkpoint' -o "$1" = 'dbstatus' -o "$1" = 'replication' -o "$1" = 'wal_archive' -o "$1" = 'wal_generate' ];then
    :
else
    if [ $# -lt 2 ];then
        echo "please provide an extra database name."
        echo -e "Usage: ./pgcheck $1 database\n"
        echo -e "\U1F625 take it slow, try again ~ \U1F625"
        exit 1
    fi
fi
dbname=$2

# ------------------------------------------------------- query here -------------------------------------------------------
# all relation info
relation_info="SELECT
  *,
  pg_size_pretty(table_bytes) AS table,
  pg_size_pretty(index_bytes) AS index,
  pg_size_pretty(total_bytes) AS total
FROM (
  SELECT
    *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
  FROM (
    SELECT
      c.oid,
      c.relfilenode,
      case when c.oid=c.relfilenode then 'NO' else 'YES' end as has_been_rewrite,
      nspname AS table_schema,
      relname AS table_name,
      c.reltuples AS row_estimate,
      pg_total_relation_size(c.oid) AS total_bytes,
      pg_indexes_size(c.oid) AS index_bytes,
      pg_total_relation_size(reltoastrelid) AS toast_bytes
    FROM
      pg_class c
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE relkind = 'r'
  ) a
) a
WHERE table_schema = '$3'
ORDER BY total_bytes DESC;"

# relation all constraint
relation_constraint="SELECT
    pgc.conname AS constraint_name,
    ccu.table_schema AS table_schema,
    ccu.table_name,
    ccu.column_name,
    pg_get_constraintdef(pgc.oid) AS cons_def
FROM
    pg_constraint pgc
    JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
    JOIN pg_class cls ON pgc.conrelid = cls.oid
    LEFT JOIN information_schema.constraint_column_usage ccu ON pgc.conname = ccu.constraint_name
        AND nsp.nspname = ccu.constraint_schema
WHERE
    nsp.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND ccu.table_name = '$3'
ORDER BY
    pgc.conname;"

# relation multi index
relation_multi_idx="select
t.relnamespace::regnamespace as table_schema,
    t.relname as table_name,
    i.relname as index_name,
    array_to_string(array_agg(a.attname), ', ') as column_names
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and t.relname ='$3'
group by
   table_schema,
    t.relname,
    i.relname
order by
    t.relname,
    i.relname;"

# all toast info
toast_info="SELECT
 n.nspname as schema,
     s.oid::regclass as relname,
     s.reltoastrelid::regclass as toast_name,
     pg_relation_size(s.reltoastrelid) AS toast_size
 FROM
     pg_class s join pg_namespace n
     on s.relnamespace = n.oid
 WHERE
     relkind = 'r'
     AND reltoastrelid <> 0
     AND n.nspname = '$3'
 ORDER BY
     3 DESC;"

# specified toast info
spe_toast_info="SELECT attr.attname,
              t.typname,
              CASE
                WHEN attstorage = 'p' THEN 'plain'
                WHEN attstorage = 'x' THEN 'extended'
                WHEN attstorage = 'e' THEN 'external'
                WHEN attstorage = 'm' THEN 'main'
              END AS attstorage
FROM pg_attribute attr INNER JOIN
            pg_type t ON t.OID = attr.atttypid
WHERE attrelid = '$3'::regclass
ORDER BY attr.attnum;"

# specified toast info
spe_toast_info_fur="SELECT
  n.nspname as schema,
      s.oid::regclass as relname,
      s.reltoastrelid::regclass as toast_name,
      pg_relation_size(s.reltoastrelid) AS toast_size
  FROM
      pg_class s join pg_namespace n
      on s.relnamespace = n.oid
  WHERE
      relname = '$3'
      AND reltoastrelid <> 0
  ORDER BY
      3 DESC;"

# database status
if [ $current_version -eq 15 ] ;then
    database_status="SELECT
        datname AS database_name,
        pg_size_pretty(pg_database_size(datname)) AS database_size,
        100 * blks_hit / (blks_hit + blks_read) || ' %' AS cache_hit_ratio,
        100 * xact_commit / (xact_commit + xact_rollback) || ' %' AS commit_ratio,
        conflicts,
        temp_files,
        pg_size_pretty(temp_bytes) as temp_bytes,
        deadlocks,
        checksum_failures,
        blk_read_time,
        blk_write_time,
        session_time,
        active_time,
        idle_in_transaction_time,
        sessions,
        sessions_abandoned,
        sessions_fatal,
        sessions_killed
    FROM
        pg_stat_database
        WHERE (blks_hit + blks_read) > 0
        AND datname NOT LIKE '%template%';"
elif [ $current_version -eq 13 -o $current_version -eq 12 ];then
    database_status="SELECT
         datname AS database_name,
         pg_size_pretty(pg_database_size(datname)) AS database_size,
         100 * blks_hit / (blks_hit + blks_read) || '%' AS cache_hit_ratio,
         100 * xact_commit / (xact_commit + xact_rollback) || '%' AS commit_ratio,
         conflicts,
         temp_files,
         pg_size_pretty(temp_bytes) as temp_bytes,
         deadlocks,
         checksum_failures,
         blk_read_time,
         blk_write_time
     FROM
         pg_stat_database
         WHERE (blks_hit + blks_read) > 0
         AND datname NOT LIKE '%template%';"
else
    database_status="SELECT
         datname AS database_name,
         pg_size_pretty(pg_database_size(datname)) AS database_size,
         100 * blks_hit / (blks_hit + blks_read) || '%' AS cache_hit_ratio,
         100 * xact_commit / (xact_commit + xact_rollback) || '%' AS commit_ratio,
         conflicts,
         temp_files,
         pg_size_pretty(temp_bytes) as temp_bytes,
         deadlocks,
         blk_read_time,
         blk_write_time
     FROM
         pg_stat_database
         WHERE (blks_hit + blks_read) > 0
         AND datname NOT LIKE '%template%';"
fi


# lock wait queue
lock_wait_queue="WITH RECURSIVE t_wait AS (
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
    root_holder;"

# lock wait state
lock_wait_state_further="SELECT
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    now() - blocked_activity.query_start AS blocked_duration,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    now() - blocking_activity.query_start AS blocking_duration,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement
FROM
    pg_catalog.pg_locks AS blocked_locks
    JOIN pg_catalog.pg_stat_activity AS blocked_activity ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks AS blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity AS blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE
    NOT blocked_locks.granted;"

# lock wait state
lock_wait_state="WITH RECURSIVE
  lock_composite(requested, current) AS (VALUES
    ('AccessShareLock'::text, 'AccessExclusiveLock'::text),
    ('RowShareLock'::text, 'ExclusiveLock'::text),
    ('RowShareLock'::text, 'AccessExclusiveLock'::text),
    ('RowExclusiveLock'::text, 'ShareLock'::text),
    ('RowExclusiveLock'::text, 'ShareRowExclusiveLock'::text),
    ('RowExclusiveLock'::text, 'ExclusiveLock'::text),
    ('RowExclusiveLock'::text, 'AccessExclusiveLock'::text),
    ('ShareUpdateExclusiveLock'::text, 'ShareUpdateExclusiveLock'::text),
    ('ShareUpdateExclusiveLock'::text, 'ShareLock'::text),
    ('ShareUpdateExclusiveLock'::text, 'ShareRowExclusiveLock'::text),
    ('ShareUpdateExclusiveLock'::text, 'ExclusiveLock'::text),
    ('ShareUpdateExclusiveLock'::text, 'AccessExclusiveLock'::text),
    ('ShareLock'::text, 'RowExclusiveLock'::text),
    ('ShareLock'::text, 'ShareUpdateExclusiveLock'::text),
    ('ShareLock'::text, 'ShareRowExclusiveLock'::text),
    ('ShareLock'::text, 'ExclusiveLock'::text),
    ('ShareLock'::text, 'AccessExclusiveLock'::text),
    ('ShareRowExclusiveLock'::text, 'RowExclusiveLock'::text),
    ('ShareRowExclusiveLock'::text, 'ShareUpdateExclusiveLock'::text),
    ('ShareRowExclusiveLock'::text, 'ShareLock'::text),
    ('ShareRowExclusiveLock'::text, 'ShareRowExclusiveLock'::text),
    ('ShareRowExclusiveLock'::text, 'ExclusiveLock'::text),
    ('ShareRowExclusiveLock'::text, 'AccessExclusiveLock'::text),
    ('ExclusiveLock'::text, 'RowShareLock'::text),
    ('ExclusiveLock'::text, 'RowExclusiveLock'::text),
    ('ExclusiveLock'::text, 'ShareUpdateExclusiveLock'::text),
    ('ExclusiveLock'::text, 'ShareLock'::text),
    ('ExclusiveLock'::text, 'ShareRowExclusiveLock'::text),
    ('ExclusiveLock'::text, 'ExclusiveLock'::text),
    ('ExclusiveLock'::text, 'AccessExclusiveLock'::text),
    ('AccessExclusiveLock'::text, 'AccessShareLock'::text),
    ('AccessExclusiveLock'::text, 'RowShareLock'::text),
    ('AccessExclusiveLock'::text, 'RowExclusiveLock'::text),
    ('AccessExclusiveLock'::text, 'ShareUpdateExclusiveLock'::text),
    ('AccessExclusiveLock'::text, 'ShareLock'::text),
    ('AccessExclusiveLock'::text, 'ShareRowExclusiveLock'::text),
    ('AccessExclusiveLock'::text, 'ExclusiveLock'::text),
    ('AccessExclusiveLock'::text, 'AccessExclusiveLock'::text)
  )
, lock AS (
  SELECT pid,
     virtualtransaction,
     granted,
     mode,
    (locktype,
     CASE locktype
       WHEN 'relation'      THEN concat_ws(';', 'db:'||datname, 'rel:'||relation::regclass::text)
       WHEN 'extend'        THEN concat_ws(';', 'db:'||datname, 'rel:'||relation::regclass::text)
       WHEN 'page'          THEN concat_ws(';', 'db:'||datname, 'rel:'||relation::regclass::text, 'page#'||page::text)
       WHEN 'tuple'         THEN concat_ws(';', 'db:'||datname, 'rel:'||relation::regclass::text, 'page#'||page::text, 'tuple#'||tuple::text)
       WHEN 'transactionid' THEN transactionid::text
       WHEN 'virtualxid'    THEN virtualxid::text
       WHEN 'object'        THEN concat_ws(';', 'class:'||classid::regclass::text, 'objid:'||objid, 'col#'||objsubid)
       ELSE concat('db:'||datname)
     END::text) AS target
  FROM pg_catalog.pg_locks
  LEFT JOIN pg_catalog.pg_database ON (pg_database.oid = pg_locks.database)
  )
, waiting_lock AS (
  SELECT
    blocker.pid                         AS blocker_pid,
    blocked.pid                         AS pid,
    concat(blocked.mode,blocked.target) AS lock_target
  FROM lock blocker
  JOIN lock blocked
    ON ( NOT blocked.granted
     AND blocker.granted
     AND blocked.pid != blocker.pid
     AND blocked.target IS NOT DISTINCT FROM blocker.target)
  JOIN lock_composite c ON (c.requested = blocked.mode AND c.current = blocker.mode)
  )
, acquired_lock AS (
  WITH waiting AS (
    SELECT lock_target, count(lock_target) AS wait_count FROM waiting_lock GROUP BY lock_target
  )
  SELECT
    pid,
    array_agg(concat(mode,target,' + '||wait_count) ORDER BY wait_count DESC NULLS LAST) AS locks_acquired
  FROM lock
    LEFT JOIN waiting ON waiting.lock_target = concat(mode,target)
  WHERE granted
  GROUP BY pid
  )
, blocking_lock AS (
  SELECT
    ARRAY[date_part('epoch', query_start)::int, pid] AS seq,
     0::int AS depth,
    -1::int AS blocker_pid,
    pid,
    concat('Connect: ',usename,' ',datname,' ',coalesce(host(client_addr)||':'||client_port, 'local')
      , E'\nSQL: ',replace(substr(coalesce(query,'N/A'), 1, 60), E'\n', ' ')
      , E'\nAcquired:\n  '
      , array_to_string(locks_acquired[1:5] ||
                        CASE WHEN array_upper(locks_acquired,1) > 5
                             THEN '... '||(array_upper(locks_acquired,1) - 5)::text||' more ...'
                        END,
                        E'\n  ')
    ) AS lock_info,
    concat(to_char(query_start, CASE WHEN age(query_start) > '24h' THEN 'Day DD Mon' ELSE 'HH24:MI:SS' END),E' started\n'
          ,CASE WHEN wait_event IS NOT NULL THEN 'waiting' ELSE state END,E'\n'
          ,date_trunc('second',age(now(),query_start)),' ago'
    ) AS lock_state
  FROM acquired_lock blocker
  LEFT JOIN pg_stat_activity act USING (pid)
  WHERE EXISTS
         (SELECT 'x' FROM waiting_lock blocked WHERE blocked.blocker_pid = blocker.pid)
    AND NOT EXISTS
         (SELECT 'x' FROM waiting_lock blocked WHERE blocked.pid = blocker.pid)
UNION ALL
  SELECT
    blocker.seq || blocked.pid,
    blocker.depth + 1,
    blocker.pid,
    blocked.pid,
    concat('Connect: ',usename,' ',datname,' ',coalesce(host(client_addr)||':'||client_port, 'local')
      , E'\nSQL: ',replace(substr(coalesce(query,'N/A'), 1, 60), E'\n', ' ')
      , E'\nWaiting: ',blocked.lock_target
      , CASE WHEN locks_acquired IS NOT NULL
             THEN E'\nAcquired:\n  ' ||
                  array_to_string(locks_acquired[1:5] ||
                                  CASE WHEN array_upper(locks_acquired,1) > 5
                                       THEN '... '||(array_upper(locks_acquired,1) - 5)::text||' more ...'
                                  END,
                                  E'\n  ')
        END
    ) AS lock_info,
    concat(to_char(query_start, CASE WHEN age(query_start) > '24h' THEN 'Day DD Mon' ELSE 'HH24:MI:SS' END),E' started\n'
          ,CASE WHEN wait_event IS NOT NULL THEN 'waiting' ELSE state END,E'\n'
          ,date_trunc('second',age(now(),query_start)),' ago'
    ) AS lock_state
  FROM blocking_lock blocker
  JOIN waiting_lock blocked
    ON (blocked.blocker_pid = blocker.pid)
  LEFT JOIN pg_stat_activity act ON (act.pid = blocked.pid)
  LEFT JOIN acquired_lock acq ON (acq.pid = blocked.pid)
  WHERE blocker.depth < 5
  )
SELECT concat(lpad('=> ', 4*depth, ' '),pid::text) AS PID
, lock_info AS Lock_Info
, lock_state AS State
FROM blocking_lock
ORDER BY seq;"

# checkpoint and background writer
bgworker_checkpoint_state="SELECT
    checkpoints_timed + checkpoints_req AS total_checkpoints,
    pg_size_pretty(block_size * buffers_clean ) AS bgworker_total_writen,
    pg_size_pretty(block_size * buffers_checkpoint ) AS chkpointer_total_writen,
    pg_size_pretty(block_size * buffers_backend ) AS backend_total_writen,
    pg_size_pretty(block_size * buffers_checkpoint / (checkpoints_timed + checkpoints_req)) AS checkpoint_write_avg,
    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) / (checkpoints_timed + checkpoints_req) / 60 AS minutes_between_checkpoints,
    buffers_backend_fsync
FROM
    pg_stat_bgwriter,
    (
        SELECT current_setting('block_size')::int AS block_size) AS bs"

# transaction id consume
age_consume="SELECT
    datname,
    age(datfrozenxid) AS frozen_xid_age,
    ROUND(100 * (age(datfrozenxid) / 2000000000::float)) consumed_txid_pct,
    2 * 1024 ^ 3 - 1 - age(datfrozenxid) AS remaining_txid,
    current_setting('autovacuum_freeze_max_age')::int - age(datfrozenxid) AS remaining_aggressive_vacuum
FROM
    pg_database;"


# transaction id at database level
age_consume_dblvl="WITH max_age AS (
    SELECT
        2000000000 AS max_old_txid,
        setting AS autovacuum_freeze_max_age
    FROM
        pg_catalog.pg_settings
    WHERE
        name = 'autovacuum_freeze_max_age'
),
per_database_stats AS (
    SELECT
        datname,
        m.max_old_txid::int,
        m.autovacuum_freeze_max_age::int,
        age(d.datfrozenxid) AS oldest_current_txid
FROM
    pg_catalog.pg_database d
    JOIN max_age m ON (TRUE)
    WHERE
        d.datallowconn
)
SELECT
    max(oldest_current_txid) AS oldest_current_txid,
    max(ROUND(100 * (oldest_current_txid / max_old_txid::float))) AS consumed_txid_pct,
    max(ROUND(100 * (oldest_current_txid / autovacuum_freeze_max_age::float))) AS consumed_autovac_max_age
FROM
    per_database_stats;
"

# transaction id as relation level
age_consume_rel_lvl="SELECT
    c.oid::regclass,
    age(c.relfrozenxid),
    pg_size_pretty(pg_total_relation_size(c.oid))
FROM
    pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE
    relkind IN ('r', 't', 'm')
    AND n.nspname NOT IN ('pg_toast')
ORDER BY
    2 DESC
LIMIT 50;"

# replication query
if [ $current_version -ge 10 ] ;then
    replication_qry="
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
        pg_stat_replication;"
else
    replication_qry="
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
        pg_stat_replication;"
fi

# current connection
current_connection="SELECT
    datname,
    count(*) AS open,
    count(*) FILTER (WHERE state = 'active') AS active,
    count(*) FILTER (WHERE state = 'idle') AS idle,
    count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_trans
FROM
    pg_stat_activity
GROUP BY
    ROLLUP (1);"

# current query
current_query="SELECT
  datname,
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
  query_start DESC;"

# index bloat
index_bloat="WITH btree_index_atts AS (
         SELECT pg_namespace.nspname,
            indexclass.relname AS index_name,
            indexclass.reltuples,
            indexclass.relpages,
            pg_index.indrelid,
            pg_index.indexrelid,
            indexclass.relam,
            tableclass.relname AS tablename,
            (regexp_split_to_table((pg_index.indkey)::text, ' '::text))::smallint AS attnum,
            pg_index.indexrelid AS index_oid
           FROM ((((pg_index
             JOIN pg_class indexclass ON ((pg_index.indexrelid = indexclass.oid)))
             JOIN pg_class tableclass ON ((pg_index.indrelid = tableclass.oid)))
             JOIN pg_namespace ON ((pg_namespace.oid = indexclass.relnamespace)))
             JOIN pg_am ON ((indexclass.relam = pg_am.oid)))
          WHERE ((pg_am.amname = 'btree'::name) AND (indexclass.relpages > 0))
        ), index_item_sizes AS (
         SELECT ind_atts.nspname,
            ind_atts.index_name,
            ind_atts.reltuples,
            ind_atts.relpages,
            ind_atts.relam,
            ind_atts.indrelid AS table_oid,
            ind_atts.index_oid,
            (current_setting('block_size'::text))::numeric AS bs,
            8 AS maxalign,
            24 AS pagehdr,
                CASE
                    WHEN (max(COALESCE(pg_stats.null_frac, (0)::real)) = (0)::double precision) THEN 2
                    ELSE 6
                END AS index_tuple_hdr,
            sum((((1)::double precision - COALESCE(pg_stats.null_frac, (0)::real)) * (COALESCE(pg_stats.avg_width, 1024))::double precision)) AS nulldatawidth
           FROM ((pg_attribute
             JOIN btree_index_atts ind_atts ON (((pg_attribute.attrelid = ind_atts.indexrelid) AND (pg_attribute.attnum = ind_atts.attnum))))
             JOIN pg_stats ON (((pg_stats.schemaname = ind_atts.nspname) AND (((pg_stats.tablename = ind_atts.tablename) AND ((pg_stats.attname)::text = pg_get_indexdef(pg_attribute.attrelid, (pg_attribute.attnum)::integer, true))) OR ((pg_stats.tablename = ind_atts.index_name) AND (pg_stats.attname = pg_attribute.attname))))))
          WHERE (pg_attribute.attnum > 0)
          GROUP BY ind_atts.nspname, ind_atts.index_name, ind_atts.reltuples, ind_atts.relpages, ind_atts.relam, ind_atts.indrelid, ind_atts.index_oid, (current_setting('block_size'::text))::numeric, 8::integer
        ), index_aligned_est AS (
         SELECT index_item_sizes.maxalign,
            index_item_sizes.bs,
            index_item_sizes.nspname,
            index_item_sizes.index_name,
            index_item_sizes.reltuples,
            index_item_sizes.relpages,
            index_item_sizes.relam,
            index_item_sizes.table_oid,
            index_item_sizes.index_oid,
            COALESCE(ceil((((index_item_sizes.reltuples * ((((((((6 + index_item_sizes.maxalign) -
                CASE
                    WHEN ((index_item_sizes.index_tuple_hdr % index_item_sizes.maxalign) = 0) THEN index_item_sizes.maxalign
                    ELSE (index_item_sizes.index_tuple_hdr % index_item_sizes.maxalign)
                END))::double precision + index_item_sizes.nulldatawidth) + (index_item_sizes.maxalign)::double precision) - (
                CASE
                    WHEN (((index_item_sizes.nulldatawidth)::integer % index_item_sizes.maxalign) = 0) THEN index_item_sizes.maxalign
                    ELSE ((index_item_sizes.nulldatawidth)::integer % index_item_sizes.maxalign)
                END)::double precision))::numeric)::double precision) / ((index_item_sizes.bs - (index_item_sizes.pagehdr)::numeric))::double precision) + (1)::double
 precision)), (0)::double precision) AS expected
           FROM index_item_sizes
        ), raw_bloat AS (
         SELECT current_database() AS dbname,
            index_aligned_est.nspname,
            pg_class.relname AS table_name,
            index_aligned_est.index_name,
            (index_aligned_est.bs * ((index_aligned_est.relpages)::bigint)::numeric) AS totalbytes,
            index_aligned_est.expected,
                CASE
                    WHEN ((index_aligned_est.relpages)::double precision <= index_aligned_est.expected) THEN (0)::numeric
                    ELSE (index_aligned_est.bs * ((((index_aligned_est.relpages)::double precision - index_aligned_est.expected))::bigint)::numeric)
                END AS wastedbytes,
                CASE
                    WHEN ((index_aligned_est.relpages)::double precision <= index_aligned_est.expected) THEN (0)::numeric
                    ELSE (((index_aligned_est.bs * ((((index_aligned_est.relpages)::double precision - index_aligned_est.expected))::bigint)::numeric) * (100)::numeric) / (index_aligned_est.bs * ((index_aligned_est.relpages)::bigint)::numeric))
                END AS realbloat,
            pg_relation_size((index_aligned_est.table_oid)::regclass) AS table_bytes,
            stat.idx_scan AS index_scans
           FROM ((index_aligned_est
             JOIN pg_class ON ((pg_class.oid = index_aligned_est.table_oid)))
             JOIN pg_stat_user_indexes stat ON ((index_aligned_est.index_oid = stat.indexrelid)))
        ), format_bloat AS (
         SELECT raw_bloat.dbname AS database_name,
            raw_bloat.nspname AS schema_name,
            raw_bloat.table_name,
            raw_bloat.index_name,
            round(raw_bloat.realbloat) AS bloat_pct,
            round((raw_bloat.wastedbytes / (((1024)::double precision ^ (2)::double precision))::numeric)) AS bloat_mb,
            round((raw_bloat.totalbytes / (((1024)::double precision ^ (2)::double precision))::numeric), 3) AS index_mb,
            round(((raw_bloat.table_bytes)::numeric / (((1024)::double precision ^ (2)::double precision))::numeric), 3) AS table_mb,
            raw_bloat.index_scans
           FROM raw_bloat
        )
 SELECT format_bloat.database_name AS datname,
    format_bloat.schema_name AS nspname,
    format_bloat.table_name AS relname,
    format_bloat.index_name AS idxname,
    format_bloat.index_scans AS idx_scans,
    format_bloat.bloat_pct,
    format_bloat.table_mb,
    (format_bloat.index_mb - format_bloat.bloat_mb) AS actual_mb,
    format_bloat.bloat_mb,
    format_bloat.index_mb AS total_mb
   FROM format_bloat
  ORDER BY format_bloat.bloat_mb DESC;"

# index duplicate
index_duplicate="SELECT sub.table,
       pg_size_pretty(sum(pg_relation_size(idx))::bigint) as size,
       (array_agg(idx))[1] as idx1, (array_agg(idx))[2] as idx2,
       (array_agg(idx))[3] as idx3, (array_agg(idx))[4] as idx4
FROM (
    SELECT s.schemaname||'.'||s.relname as table,i.indexrelid::regclass as idx, (i.indrelid::text ||E'\n'|| i.indclass::text ||E'\n'|| i.indkey::text ||E'\n'||
                                         coalesce(i.indexprs::text,'')||E'\n' || coalesce(i.indpred::text,'')) as key
    FROM pg_index i join pg_stat_all_indexes s on i.indexrelid = s.indexrelid) sub
GROUP BY sub.table,key HAVING count(*)>1
ORDER BY sum(pg_relation_size(idx)) DESC;"

# index low efficiency
index_low_efficiency="SELECT
       s.schemaname AS schema,
       s.relname AS table,
       indexrelname AS index,
       i.indisunique,
       idx_scan AS idx_scans
FROM   pg_catalog.pg_stat_user_indexes s,
       pg_index i
WHERE  i.indexrelid = s.indexrelid
ORDER BY idx_scan"

# index state
index_state="SELECT
    CONCAT(n.nspname, '.', c.relname) AS table,
    i.relname AS index_name,
    pg_size_pretty(pg_relation_size(x.indrelid)) AS table_size,
    pg_size_pretty(pg_relation_size(x.indexrelid)) AS index_size,
    pg_size_pretty(pg_total_relation_size(x.indrelid)) AS total_size,
    c.reltuples AS num_rows,
    c.relpages AS num_pages,
    c.reltuples / nullif (c.relpages, 0) AS num_per_page,
    CASE WHEN indisunique THEN
        'Y'
    ELSE
        'N'
    END AS UNIQUE,
    idx_scan AS number_of_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM
    pg_class c
    JOIN pg_index x ON c.oid = x.indrelid
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_stat_all_indexes s ON s.indexrelid = x.indexrelid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = ANY (ARRAY['r', 't', 'i'])
    AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
ORDER BY
    1,
    2;"

# index state
index_state_further="SELECT
    pg_namespace.nspname as schema,
    pg_class.relname as table,
    pg_size_pretty(pg_class.reltuples::bigint)            AS rows_in_bytes,
    pg_class.reltuples                                    AS num_rows,
    COUNT(*)                                              AS total_indexes,
    COUNT(*) FILTER ( WHERE indisunique)                  AS unique_indexes,
    COUNT(*) FILTER ( WHERE indnatts = 1 )                AS single_column_indexes,
    COUNT(*) FILTER ( WHERE indnatts IS DISTINCT FROM 1 ) AS multi_column_indexes
FROM
    pg_namespace
    LEFT JOIN pg_class ON pg_namespace.oid = pg_class.relnamespace
    LEFT JOIN pg_index ON pg_class.oid = pg_index.indrelid
WHERE
    pg_class.relkind = 'r'
    AND pg_namespace.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
GROUP BY pg_class.relname, pg_class.reltuples,pg_namespace.nspname
ORDER BY 1,2 DESC;"

# long transaction
long_transaction="SELECT
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
    xact_start;"

# relation bloat
relation_bloat="-- new table bloat query
-- still needs work; is often off by +/- 20%
WITH constants AS (
    -- define some constants for sizes of things
    -- for reference down the query and easy maintenance
    SELECT
        current_setting('block_size')::numeric AS bs,
        23 AS hdr,
        8 AS ma
),
no_stats AS (
    -- screen out table who have attributes
    -- which dont have stats, such as JSON
    SELECT
        table_schema,
        table_name,
        n_live_tup::numeric AS est_rows,
        pg_table_size(relid)::numeric AS table_size
    FROM
        information_schema.columns
        JOIN pg_stat_user_tables AS psut ON table_schema = psut.schemaname
            AND table_name = psut.relname
        LEFT OUTER JOIN pg_stats ON table_schema = pg_stats.schemaname
        AND table_name = pg_stats.tablename
        AND column_name = attname
    WHERE
        attname IS NULL
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY
        table_schema,
        table_name,
        relid,
        n_live_tup
),
null_headers AS (
    -- calculate null header sizes
    -- omitting tables which dont have complete stats
    -- and attributes which aren't visible
    SELECT
        hdr + 1 + (sum(
                CASE WHEN null_frac <> 0 THEN
                    1
                ELSE
                    0
                END) / 8) AS nullhdr,
        SUM((1 - null_frac) * avg_width) AS datawidth,
        MAX(null_frac) AS maxfracsum,
        schemaname,
        tablename,
        hdr,
        ma,
        bs
    FROM
        pg_stats
        CROSS JOIN constants
        LEFT OUTER JOIN no_stats ON schemaname = no_stats.table_schema
            AND tablename = no_stats.table_name
    WHERE
        schemaname NOT IN ('pg_catalog', 'information_schema')
        AND no_stats.table_name IS NULL
        AND EXISTS (
            SELECT
                1
            FROM
                information_schema.columns
            WHERE
                schemaname = columns.table_schema
                AND tablename = columns.table_name)
        GROUP BY
            schemaname,
            tablename,
            hdr,
            ma,
            bs
),
data_headers AS (
    -- estimate header and row size
    SELECT
        ma,
        bs,
        hdr,
        schemaname,
        tablename,
        (datawidth + (hdr + ma - (
                    CASE WHEN hdr % ma = 0 THEN
                        ma
                    ELSE
                        hdr % ma
                    END)))::numeric AS datahdr,
        (maxfracsum * (nullhdr + ma - (
                    CASE WHEN nullhdr % ma = 0 THEN
                        ma
                    ELSE
                        nullhdr % ma
                    END))) AS nullhdr2
    FROM
        null_headers
),
table_estimates AS (
    -- make estimates of how large the table should be
    -- based on row and page size
    SELECT
        schemaname,
        tablename,
        bs,
        reltuples::numeric AS est_rows,
        relpages * bs AS table_bytes,
        CEIL((reltuples * (datahdr + nullhdr2 + 4 + ma - (
                    CASE WHEN datahdr % ma = 0 THEN
                        ma
                    ELSE
                        datahdr % ma
                    END)) / (bs - 20))) * bs AS expected_bytes,
        reltoastrelid
    FROM
        data_headers
        JOIN pg_class ON tablename = relname
        JOIN pg_namespace ON relnamespace = pg_namespace.oid
            AND schemaname = nspname
    WHERE
        pg_class.relkind = 'r'
),
estimates_with_toast AS (
    -- add in estimated TOAST table sizes
    -- estimate based on 4 toast tuples per page because we dont have
    -- anything better.  also append the no_data tables
    SELECT
        schemaname,
        tablename,
        TRUE AS can_estimate,
        est_rows,
        table_bytes + (coalesce(toast.relpages, 0) * bs) AS table_bytes,
        expected_bytes + (ceil(coalesce(toast.reltuples, 0) / 4) * bs) AS expected_bytes
    FROM
        table_estimates
        LEFT OUTER JOIN pg_class AS toast ON table_estimates.reltoastrelid = toast.oid
        AND toast.relkind = 't'
),
table_estimates_plus AS (
    -- add some extra metadata to the table data
    -- and calculations to be reused
    -- including whether we cant estimate it
    -- or whether we think it might be compressed
    SELECT
        current_database() AS databasename,
        schemaname,
        tablename,
        can_estimate,
        est_rows,
        CASE WHEN table_bytes > 0 THEN
            table_bytes::numeric
        ELSE
            NULL::numeric
        END AS table_bytes,
        CASE WHEN expected_bytes > 0 THEN
            expected_bytes::numeric
        ELSE
            NULL::numeric
        END AS expected_bytes,
        CASE WHEN expected_bytes > 0
            AND table_bytes > 0
            AND expected_bytes <= table_bytes THEN
            (table_bytes - expected_bytes)::numeric
        ELSE
            0::numeric
        END AS bloat_bytes
    FROM
        estimates_with_toast
    UNION ALL
    SELECT
        current_database() AS databasename,
        table_schema,
        table_name,
        FALSE,
        est_rows,
        table_size,
        NULL::numeric,
        NULL::numeric
    FROM
        no_stats
),
bloat_data AS (
    -- do final math calculations and formatting
    SELECT
        current_database() AS databasename,
        schemaname,
        tablename,
        can_estimate,
        table_bytes,
        round(table_bytes / (1024 ^ 2)::numeric, 3) AS table_mb,
        expected_bytes,
        round(expected_bytes / (1024 ^ 2)::numeric, 3) AS expected_mb,
        round(bloat_bytes * 100 / table_bytes) AS pct_bloat,
        round(bloat_bytes / (1024::numeric ^ 2), 2) AS mb_bloat,
        table_bytes,
        expected_bytes,
        est_rows
    FROM
        table_estimates_plus)
        -- filter output for bloated tables
        SELECT
            databasename,
            schemaname,
            tablename,
            can_estimate,
            est_rows,
            pct_bloat,
            mb_bloat,
            table_mb
        FROM
            bloat_data
            -- this where clause defines which tables actually appear
            -- in the bloat chart
            -- example below filters for tables which are either 50%
            -- bloated and more than 20mb in size, or more than 25%
            -- bloated and more than 4GB in size
    WHERE (pct_bloat >= 50
        AND mb_bloat >= 10)
    OR (pct_bloat >= 25
        AND mb_bloat >= 1000)
ORDER BY
    pct_bloat DESC;"

# oldest xmin
<<comment
oldest_xmin="with a as (
(select 'pg_stat_activity' as src, xact_start, usename,datname, query, backend_xid, backend_xmin
from pg_stat_activity
  where backend_xid = xid(pg_snapshot_xmin(pg_current_snapshot()))
  or backend_xmin = xid(pg_snapshot_xmin(pg_current_snapshot()))
  order by xact_start limit 1 )
union all
(select '2pc' as src, prepared as xact_start, owner as usename, database as datname, gid as query, transaction as backend_xid, transaction as backend_xmin
from pg_prepared_xacts
  where transaction = xid(pg_snapshot_xmin(pg_current_snapshot()))
  order by prepared limit 1 )
)
select * from a order by xact_start limit 1;"
comment

# oldest xmin more compatible
oldest_xmin=" with a as (
 (select 'pg_stat_activity' as src, xact_start, usename,datname, query, backend_xid, backend_xmin
 from pg_stat_activity
   where backend_xid = xid(txid_snapshot_xmin(txid_current_snapshot())::text::xid)
   or backend_xmin = xid(txid_snapshot_xmin(txid_current_snapshot())::text::xid)
   order by xact_start limit 1 )
 union all
 (select '2pc' as src, prepared as xact_start, owner as usename, database as datname, gid as query, transaction as backend_xid, transaction as backend_xmin
 from pg_prepared_xacts
   where transaction = xid(txid_snapshot_xmin(txid_current_snapshot())::text::xid)
   order by prepared limit 1 )
 )
 select * from a order by xact_start limit 1;"

# oldest running xact
oldest_xact="SELECT
    (
        SELECT
            max(age(backend_xmin))
        FROM
            pg_stat_activity) AS oldest_running_xact,
    (
        SELECT
            max(age(TRANSACTION))
        FROM
            pg_prepared_xacts) AS oldest_prepared_xact,
    (
        SELECT
            max(age(xmin))
        FROM
            pg_replication_slots) AS oldest_replication_slot,
    (
        SELECT
            max(age(backend_xmin))
        FROM
            pg_stat_replication) AS oldest_replica_xact;"

# vacuum_state
vacuum_state_qry="SELECT
    p.pid,
    now() - a.xact_start AS duration,
    coalesce(wait_event_type || '.' || wait_event, 'f') AS waiting,
    CASE WHEN a.query ~* '^autovacuum.*to prevent wraparound' THEN
        'wraparound'
    WHEN a.query ~* '^vacuum' THEN
        'user'
    ELSE
        'regular'
    END AS mode,
    p.datname AS database,
    p.relid::regclass AS table,
    p.phase,
    pg_size_pretty(p.heap_blks_total * current_setting('block_size')::int) AS table_size,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(p.heap_blks_scanned * current_setting('block_size')::int) AS scanned,
    pg_size_pretty(p.heap_blks_vacuumed * current_setting('block_size')::int) AS vacuumed,
    round(100.0 * p.heap_blks_scanned / p.heap_blks_total, 1) AS scanned_pct,
    round(100.0 * p.heap_blks_vacuumed / p.heap_blks_total, 1) AS vacuumed_pct,
    p.index_vacuum_count,
    round(100.0 * p.num_dead_tuples / p.max_dead_tuples, 1) AS dead_pct
FROM
    pg_stat_progress_vacuum p
    JOIN pg_stat_activity a USING (pid)
ORDER BY
    now() - a.xact_start DESC;"

# table need vacuum
tbl_need_vacuum="SELECT
   pg_stat_user_tables.relname,
   pg_stat_user_tables.n_dead_tup,
   50 + 0.1 * pg_class.reltuples as vacuum_threshold,
   pg_stat_user_tables.n_live_tup,
   pg_stat_user_tables.n_tup_del,
   pg_stat_user_tables.n_tup_upd,
   pg_stat_user_tables.autovacuum_count,
   pg_stat_user_tables.last_vacuum,
   pg_stat_user_tables.last_autovacuum,
   now() as now,
   pg_class.reltuples,
   pg_stat_user_tables.n_dead_tup > (50 + 0.1 * pg_class.reltuples) as is_vacuum
FROM
   pg_stat_user_tables INNER JOIN pg_class ON pg_stat_user_tables.relname = pg_class.relname
ORDER BY
   pg_stat_user_tables.n_dead_tup > (50 + 0.1 * pg_class.reltuples) DESC;"

# wait event
wait_event="SELECT
    wait_event,
    wait_event_type,
    count(*)
FROM
    pg_stat_activity
GROUP BY
    1,
    2;"

# wal archive state
wal_archive="SELECT *,
    current_setting('archive_mode')::BOOLEAN
        AND (last_failed_wal IS NULL
            OR last_failed_wal <= last_archived_wal)
        AS is_archiving,
    CAST (archived_count AS NUMERIC)
        / EXTRACT (EPOCH FROM age(now(), stats_reset))
        AS current_archived_wals_per_second
FROM pg_stat_archiver;"

# wal generate speed
wal_generate_speed="with tmp_file as (
  select t1.file,
      t1.file_ls,
      (pg_stat_file(t1.file)).size as size,
      (pg_stat_file(t1.file)).access as access,
      (pg_stat_file(t1.file)).modification as last_update_time,
      (pg_stat_file(t1.file)).change as change,
      (pg_stat_file(t1.file)).creation as creation,
      (pg_stat_file(t1.file)).isdir as isdir
   from (select dir||'/'||pg_ls_dir(t0.dir) as file,
          pg_ls_dir(t0.dir) as file_ls
       from ( select '$2'::text as dir
           --需要修改这个物理路径
          ) t0
      ) t1
   where 1=1
   order by (pg_stat_file(file)).modification desc
)
select to_char(date_trunc('day',tf0.last_update_time),'yyyymmdd') as day_id,
    sum(case when date_part('hour',tf0.last_update_time) >=0 and date_part('hour',tf0.last_update_time) <24 then 1 else 0 end) as wal_num_all,
    sum(case when date_part('hour',tf0.last_update_time) >=0 and date_part('hour',tf0.last_update_time) <1 then 1 else 0 end) as wal_num_00_01,
    sum(case when date_part('hour',tf0.last_update_time) >=1 and date_part('hour',tf0.last_update_time) <2 then 1 else 0 end) as wal_num_01_02,
    sum(case when date_part('hour',tf0.last_update_time) >=2 and date_part('hour',tf0.last_update_time) <3 then 1 else 0 end) as wal_num_02_03,
    sum(case when date_part('hour',tf0.last_update_time) >=3 and date_part('hour',tf0.last_update_time) <4 then 1 else 0 end) as wal_num_03_04,
    sum(case when date_part('hour',tf0.last_update_time) >=4 and date_part('hour',tf0.last_update_time) <5 then 1 else 0 end) as wal_num_04_05,
    sum(case when date_part('hour',tf0.last_update_time) >=5 and date_part('hour',tf0.last_update_time) <6 then 1 else 0 end) as wal_num_05_06,
    sum(case when date_part('hour',tf0.last_update_time) >=6 and date_part('hour',tf0.last_update_time) <7 then 1 else 0 end) as wal_num_06_07,
    sum(case when date_part('hour',tf0.last_update_time) >=7 and date_part('hour',tf0.last_update_time) <8 then 1 else 0 end) as wal_num_07_08,
    sum(case when date_part('hour',tf0.last_update_time) >=8 and date_part('hour',tf0.last_update_time) <9 then 1 else 0 end) as wal_num_08_09,
    sum(case when date_part('hour',tf0.last_update_time) >=9 and date_part('hour',tf0.last_update_time) <10 then 1 else 0 end) as wal_num_09_10,
    sum(case when date_part('hour',tf0.last_update_time) >=10 and date_part('hour',tf0.last_update_time) <11 then 1 else 0 end) as wal_num_10_11,
    sum(case when date_part('hour',tf0.last_update_time) >=11 and date_part('hour',tf0.last_update_time) <12 then 1 else 0 end) as wal_num_11_12,
    sum(case when date_part('hour',tf0.last_update_time) >=12 and date_part('hour',tf0.last_update_time) <13 then 1 else 0 end) as wal_num_12_13,
    sum(case when date_part('hour',tf0.last_update_time) >=13 and date_part('hour',tf0.last_update_time) <14 then 1 else 0 end) as wal_num_13_14,
    sum(case when date_part('hour',tf0.last_update_time) >=14 and date_part('hour',tf0.last_update_time) <15 then 1 else 0 end) as wal_num_14_15,
    sum(case when date_part('hour',tf0.last_update_time) >=15 and date_part('hour',tf0.last_update_time) <16 then 1 else 0 end) as wal_num_15_16,
    sum(case when date_part('hour',tf0.last_update_time) >=16 and date_part('hour',tf0.last_update_time) <17 then 1 else 0 end) as wal_num_16_17,
    sum(case when date_part('hour',tf0.last_update_time) >=17 and date_part('hour',tf0.last_update_time) <18 then 1 else 0 end) as wal_num_17_18,
    sum(case when date_part('hour',tf0.last_update_time) >=18 and date_part('hour',tf0.last_update_time) <19 then 1 else 0 end) as wal_num_18_19,
    sum(case when date_part('hour',tf0.last_update_time) >=19 and date_part('hour',tf0.last_update_time) <20 then 1 else 0 end) as wal_num_19_20,
    sum(case when date_part('hour',tf0.last_update_time) >=20 and date_part('hour',tf0.last_update_time) <21 then 1 else 0 end) as wal_num_20_21,
    sum(case when date_part('hour',tf0.last_update_time) >=21 and date_part('hour',tf0.last_update_time) <22 then 1 else 0 end) as wal_num_21_22,
    sum(case when date_part('hour',tf0.last_update_time) >=22 and date_part('hour',tf0.last_update_time) <23 then 1 else 0 end) as wal_num_22_23,
    sum(case when date_part('hour',tf0.last_update_time) >=23 and date_part('hour',tf0.last_update_time) <24 then 1 else 0 end) as wal_num_23_24
from tmp_file tf0
where 1=1
 and tf0.file_ls not in ('archive_status')
group by to_char(date_trunc('day',tf0.last_update_time),'yyyymmdd')
order by to_char(date_trunc('day',tf0.last_update_time),'yyyymmdd') desc;"

# index create
index_create="SELECT
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
    LEFT JOIN pg_stat_activity a ON a.pid = p.pid;"

# get user object
user_obj_qry="select
    nsp.nspname as SchemaName
    ,cls.relname as ObjectName
    ,rol.rolname as ObjectOwner
    ,case cls.relkind
        when 'r' then 'TABLE'
        when 'm' then 'MATERIALIZED_VIEW'
        when 'i' then 'INDEX'
        when 'S' then 'SEQUNCE'
        when 'v' then 'VIEW'
        when 'c' then 'TYPE'
        else cls.relkind::text
    end as ObjectType
from pg_class cls
join pg_roles rol
    on rol.oid = cls.relowner
join pg_namespace nsp
    on nsp.oid = cls.relnamespace
where nsp.nspname not in ('information_schema', 'pg_catalog')
    and nsp.nspname not like 'pg_toast%'
    and rol.rolname = '$3'
order by nsp.nspname, cls.relname;"

# partition table
partition_qry="WITH RECURSIVE inheritance_tree AS (
     SELECT   c.oid AS table_oid
            , c.relname  AS table_name
            , n.nspname  AS schema_name
            , NULL::name AS table_parent_name
            , c.relispartition AS is_partition
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'p'
     AND   c.relispartition = false
     UNION ALL
     SELECT inh.inhrelid AS table_oid
          , c.relname AS table_name
          , n.nspname AS schema_name
          , cc.relname AS table_parent_name
          , c.relispartition AS is_partition
     FROM inheritance_tree it
     JOIN pg_inherits inh ON inh.inhparent = it.table_oid
     JOIN pg_class c ON inh.inhrelid = c.oid
     JOIN pg_class cc ON it.table_oid = cc.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
)
SELECT
          it.table_name
        , it.schema_name
        , c.reltuples
        , c.relpages
        , CASE p.partstrat
               WHEN 'l' THEN 'BY LIST'
               WHEN 'r' THEN 'BY RANGE'
               ELSE 'not partitioned'
          END AS partitionin_type
        , it.table_parent_name
        , pg_get_expr( c.relpartbound, c.oid, true ) AS partitioning_values
        , pg_get_expr( p.partexprs, c.oid, true )    AS sub_partitioning_values
FROM inheritance_tree it
JOIN pg_class c ON c.oid = it.table_oid
LEFT JOIN pg_partitioned_table p ON p.partrelid = it.table_oid
ORDER BY 1,2;"

# inherit partition table
inherit_partition="SELECT
    nmsp_parent.nspname AS parent_schema,
    parent.relname AS parent,
    nmsp_child.nspname AS child_schema,
    child.relname AS child
FROM
    pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
    JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
WHERE
    child.relkind = 'r'
ORDER BY child.relname;"

# partition table size
partition_size="SELECT
   pi.inhparent::regclass AS parent_table_name,
   pg_size_pretty(sum(pg_total_relation_size(psu.relid))) AS total,
   pg_size_pretty(sum(pg_relation_size(psu.relid))) AS internal,
   pg_size_pretty(sum(pg_table_size(psu.relid) - pg_relation_size(psu.relid))) AS external, -- toast
   pg_size_pretty(sum(pg_indexes_size(psu.relid))) AS indexes
FROM pg_catalog.pg_statio_user_tables psu
   JOIN pg_class pc ON psu.relname = pc.relname
   JOIN pg_database pd ON pc.relowner = pd.datdba
   JOIN pg_inherits pi ON pi.inhrelid = pc.oid
WHERE pd.datname = 'postgres'
GROUP BY pi.inhparent
ORDER BY sum(pg_total_relation_size(psu.relid)) DESC;"

user_member_qry="WITH RECURSIVE x AS
(
  SELECT member::regrole,
         roleid::regrole AS role,
         member::regrole || ' -> ' || roleid::regrole AS path
  FROM pg_auth_members AS m
  WHERE roleid > 16384
  UNION ALL
  SELECT x.member::regrole,
         m.roleid::regrole,
         x.path || ' -> ' || m.roleid::regrole
 FROM pg_auth_members AS m
    JOIN x ON m.member = x.role
  )
  SELECT member, role, path
  FROM x
  ORDER BY member::text, role::text;"

# main
case "$1" in
    relation)
        if [ $# -lt 3 ];then
            echo "please provide an extra schema name"
            echo -e "Usage: ./pgcheck $1 database schema\n"
            echo -e "\U1F601 tune the parameters a bit more ~ \U1F601"
            exit 1
        fi
        psql -q -d $dbname -c "$relation_info"
        ;;
    relconstraint)
        if [ $# -lt 3 ];then
            echo "please provide an extra table name"
            echo -e "Usage: ./pgcheck $1 database relname\n"
            echo -e "\U1F601 tune the parameters a bit more ~ \U1F601"
            exit 1
        fi
        psql -q -d $dbname -c "$relation_constraint"
        psql -q -d $dbname -c "$relation_multi_idx"
        echo "- Description: "
        echo "1. in default, deleting a column will automatically delete the composite index and constraint corresponding to the column,so you need to pay attention potential problem."
        ;;
    alltoast)
        if [ $# -lt 3 ];then
            echo "please provide an extra schema name"
            echo -e "Usage: ./pgcheck $1 database schema\n"
            echo -e "\U1F601 tune the parameters a bit more ~ \U1F601"
            exit 1
        fi
        psql -q -d $dbname -c "$toast_info"
        ;;
    reltoast)
        if [ $# -lt 3 ];then
             echo "please provide an extra table name"
             echo -e "Usage: ./pgcheck $1 database relname\n"
             echo -e "\U1F601 tune the parameters a bit more ~ \U1F601"
             exit 1
        fi
        psql -q -d $dbname -c "$spe_toast_info"
        psql -q -d $dbname -c "$spe_toast_info_fur"
        ;;
    dbstatus)
        psql -q -x -c "$database_status"
        ;;
    lock|LOCK)
        psql -q -d $dbname -c "$lock_wait_state_further"
        psql -q -x -d $dbname -c "$lock_wait_state"
        echo "wait queue:"
        psql -q -d $dbname -c "$lock_wait_queue"
        echo "- Description: "
        echo "1. in default, max lock numbers in buffer equal max_connections * max_locks_per_transaction.pay attention to 'out of shared memory'"
        ;;
    checkpoint)
        psql -q -x -c "$bgworker_checkpoint_state"
        echo "- Description: "
        echo "1. background worker and checkpointer should be should be the main force to flush dirty pages."
        echo "2. so if you find backend_total_writen is much larger than other values, it means that you need to adjust the relevant parameters of bgworker"
        ;;
    freeze)
        psql -q -x -c "$age_consume"
        echo "- Description: "
        echo "1. datname contains the name of the database."
        echo "2. frozen_xid_age represents the age of the database-level frozen transaction ID. A higher value (for example, greater than autovacuum_freeze_max_age) means that the database needs attention. "
        echo "3. consumed_txid_pct represents the percentage of the transaction ID against the maximum transaction ID limit (2 billion transaction IDs) for the database."
        echo -e "4. remaining_aggressive_vacuum represents the available transaction ID space before it reaches the aggressive VACUUM mode—how close the database is to the autovacuum_freeze_max_age value. A negative value means that there are some tables in the database that trigger an aggressive VACUUM operation due to the age of pg_class.relfrozentxid.\n"
        echo "The following query reports the database system level usage of transaction ID space:"
        psql -q -d $dbname -c "$age_consume_dblvl"

        echo "The following example identifies the top 50 tables that need the VACUUM operation performed on them:"
        psql -q -d $dbname -c "$age_consume_rel_lvl"
        ;;
    connections)
        psql -q -d $dbname -c "$current_connection"
        psql -q -d $dbname -c "$current_query"
        ;;
    index_bloat)
        echo "This query will take a while, wait for a moment."
        psql -q -d $dbname -c "$index_bloat"
        echo "- Description: "
        echo "1. idx_scans: How many times the index is scanned, a lower value may indicate that the index is low efficiency, but pay attention to its functionality, such as a unique index"
        echo "2. bloat_pct: bloat percentage"
        echo "3. table_mb: relation size, exclude index"
        echo "4. actual_mb: actual index size"
        echo "5. bloat_mb: bloat index size"
        echo "6. total_mb: total index size, include bloat size "
        ;;
    index_duplicate)
        psql -q -d $dbname -c "$index_duplicate"
        ;;
    index_low)
        psql -q -d $dbname -c "$index_low_efficiency"
        echo "- Description: "
        echo "1. idx_scans: How many times the index is scanned, a lower value may indicate that the index is low efficiency, but pay attention to its functionality, such as a unique index"
        ;;
    index_state)
        psql -q -d $dbname -c "$index_state"
        psql -q -d $dbname -c "$index_state_further"
        ;;
    long_transaction)
        psql -q -d $dbname -c "$long_transaction"
        echo "- Description: "
        echo "1. long transaction is dbkiller,pay attention!"
        echo "2. it will prevent vacuum cleaning and cause table bloat"
        echo "3. block the use of the index (the index cannot be used after creation), will make the problem phenomenon very confusing"
        echo "4. if there are sub-transactions, combined with long transactions, it is easy to cause a sharp drop in performance"
        echo "5. age can only be reduced to the earliest long transaction that exists in the system, that is, min (pg_stat_activity.(backend_xid, backend_xmin))"
        echo "6. under logical decoding, long transactions will block the creation of replication slots. In fact, it is to push to a consistent point to start parsing, so it will block logical replication, CDC, etc."
        echo "7. under logical decoding, large transactions will cause WAL logs to accumulate"
        echo "8. long transactions will cause partial accumulation of WAL logs in the standby database of stream replication"
        ;;
    relation_bloat)
        echo "This query will take a while, wait for a moment.(in order to be more accurate, it is best to execute the analyze first)"
        psql -q -d $dbname -c "$relation_bloat"
        echo "- Description: "
        echo "1. pct_bloat: bloat percentage"
        echo "2. mb_bloat: bloat table size"
        echo "3. table_mb: total table size, include bloat size"
        echo -e "\nOldest xmin will block vacuum process,pay attention:"
        psql -q -x -d $dbname -c "$oldest_xmin"

        echo -e "\nThe following query retrieves the oldest value for each of the vacuum operation blockers:"
        psql -q -x -d $dbname -c "$oldest_xact"
        ;;
    vacuum_state)
        if [ $current_version -lt 11  ] ;then
            echo "error! current version does not support this view: pg_stat_progress_vacuum"
            exit 1
        fi
        is_running=`psql -q -d $dbname -c "\timing off" -c "$vacuum_state_qry" | wc -l`
        if [ $is_running -eq 4 ];then
            echo "no running vacuum process!"
            exit 0
        else
            for i in {1..5};
            do
                psql -q -x -d $dbname -c "$vacuum_state_qry"
                sleep 1
            done
        fi
        echo "- Description: "
        echo "1. phase – internally, vacuum has several stages and this field shows the exact phase it is in at the moment."
        echo "2. heap_blks_total, heap_blks_scanned, heap_blks_vacuumed – are about relation’s size (in blocks) and how many blocks have already been processed. These are the main values that help to estimate progress of the running a vacuum."
        echo "3. max_dead_tuples, num_dead_tuples – these values are about dead tuples storage which is based on the autovacuum_work_mem (or maintenance_work_mem if previous isn’t set). When there is no free space in the dead tuples storage, vacuum starts processing indexes and it might happen multiple times. Each time when this occurs, the index_vacuum_count is incremented."
        ;;
    vacuum_need)
        psql -q -d $dbname -c "$tbl_need_vacuum"
        ;;
    wait_event)
        psql -q -d $dbname -c "$wait_event"
        ;;
    wal_archive)
        psql -q -x -c "$wal_archive"
        ;;
    wal_generate)
        if [ $# -lt 2 ];then
             echo "please provide detailed wal directory absolute path! such as '/home/postgres/pgdata/pg_wal'"
             echo -e "Usage: ./pgcheck $1 postgres /home/postgres/pgdata/pg_wal\n"
             echo -e "\U1F601 tune the parameters a bit more ~ \U1F601"
             exit 1
         fi
        psql -q -x -c "$wal_generate_speed"
        if [ $current_version -ge 15  ] ;then
            psql -q -x -c "select * from pg_stat_wal"
        fi
        ;;
    replication)
        psql -q -c "$replication_qry"
        ;;
    index_create)
        if [ $current_version -lt 12  ] ;then
            echo "error! current version does not support this view: pg_stat_progress_create_index"
            exit 1
        fi
        is_running=`psql -q -d $dbname -c "\timing off" -c "$index_create" | wc -l`
        if [ $is_running -eq 4 ];then
            echo "no running create index process!"
            exit 0
        else
            for i in {1..5};
            do
                psql -q -x -d $dbname -c "$index_create"
                sleep 1
            done
        fi
        echo "- Description: "
        echo "1. initializing: CREATE INDEX or REINDEX is preparing to create the index. This phase is expected to be very brief."
        echo "2. waiting for writers before build: CREATE INDEX CONCURRENTLY or REINDEX CONCURRENTLY is waiting for transactions with write locks that can potentially see the table to finish. This phase is skipped when not in concurrent mode. Columns lockers_total, lockers_done and current_locker_pid contain the progress information for this phase."
        echo "3. building index: The index is being built by the access method-specific code. In this phase, access methods that support progress reporting fill in their own progress data, and the subphase is indicated in this column. Typically, blocks_total and blocks_done will contain progress data, as well as potentially tuples_total and tuples_done."
        echo "4. waiting for writers before validation: CREATE INDEX CONCURRENTLY or REINDEX CONCURRENTLY is waiting for transactions with write locks that can potentially write into the table to finish. This phase is skipped when not in concurrent mode. Columns lockers_total, lockers_done and current_locker_pid contain the progress information for this phase."
        echo "5. index validation: scanning index: CREATE INDEX CONCURRENTLY is scanning the index searching for tuples that need to be validated. This phase is skipped when not in concurrent mode. Columns blocks_total (set to the total size of the index) and blocks_done contain the progress information for this phase."
        echo "6. index validation: sorting tuples  CREATE INDEX CONCURRENTLY is sorting the output of the index scanning phase."
        echo "7. index validation: scanning table  CREATE INDEX CONCURRENTLY is scanning the table to validate the index tuples collected in the previous two phases. This phase is skipped when not in concurrent mode. Columns blocks_total (set to the total size of the table) and blocks_done contain the progress information for this phase."
        echo "8. waiting for old snapshots CREATE INDEX CONCURRENTLY or REINDEX CONCURRENTLY is waiting for transactions that can potentially see the table to release their snapshots. This phase is skipped when not in concurrent mode. Columns lockers_total, lockers_done and current_locker_pid contain the progress information for this phase."
        echo "9. waiting for readers before marking dead: REINDEX CONCURRENTLY is waiting for transactions with read locks on the table to finish, before marking the old index dead. This phase is skipped when not in concurrent mode. Columns lockers_total, lockers_done and current_locker_pid contain the progress information for this phase."
        echo "10. waiting for readers before dropping: REINDEX CONCURRENTLY is waiting for transactions with read locks on the table to finish, before dropping the old index. This phase is skipped when not in concurrent mode. Columns lockers_total, lockers_done and current_locker_pid contain the progress information for this phase."
        ;;
    object)
        if [ $# -lt 3 ];then
            echo "please provide an extra user name."
            echo -e "Usage: ./pgcheck $1 postgres username\n"
            echo -e "\U1F601 tune the parameters a bit more ~ \U1F601"
            exit 1
        fi
        psql -q -d $dbname -c "$user_obj_qry"
        echo "user member relationship:"
        psql -q -d $dbname -c "$user_member_qry"
        ;;
    partition)
        echo -e "native partition (in order to be more accurate, it is best to execute the analyze first):"
        psql -q -d $dbname -c "$partition_qry"
        echo -e "inherit partition and native partition (in order to be more accurate, it is best to execute the analyze first):"
        psql -q -d $dbname -c "$inherit_partition"
        echo -e "partition size (all):"
        psql -q -d $dbname -c "$partition_size"
        ;;
    *)
        echo "error, please check your input is valid"
        echo -e "you can use command: ./pgcheck --help for help\n"
        echo -e "\U1F632 take it slow, try try see ~ \U1F632"
esac
