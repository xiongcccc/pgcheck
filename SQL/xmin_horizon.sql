with bits as (
  select
    (
      select backend_xmin
      from pg_stat_activity
      order by age(backend_xmin) desc nulls last
      limit 1
    ) as xmin_pg_stat_activity,
    (
      select xmin
      from pg_replication_slots
      order by age(xmin) desc nulls last
      limit 1
    ) as xmin_pg_replication_slots,
    (
      select backend_xmin
      from pg_stat_replication
      order by age(backend_xmin) desc nulls last
      limit 1
    ) as xmin_pg_stat_replication,
    (
      select transaction
      from pg_prepared_xacts
      order by age(transaction) desc nulls last
      limit 1
    ) as xmin_pg_prepared_xacts
)
select
  *,
  age(xmin_pg_stat_activity) as xmin_pgsa_age,
  age(xmin_pg_replication_slots) as xmin_pgrs_age,
  age(xmin_pg_stat_replication) as xmin_pgsr_age,
  age(xmin_pg_prepared_xacts) as xmin_pgpx_age,
  greatest(
    age(xmin_pg_stat_activity),
    age(xmin_pg_replication_slots),
    age(xmin_pg_stat_replication),
    age(xmin_pg_prepared_xacts)
  ) as xmin_horizon_age
from bits;

