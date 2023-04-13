with a as (  
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
select * from a order by xact_start limit 1;
