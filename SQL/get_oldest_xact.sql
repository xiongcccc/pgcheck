SELECT
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
             pg_stat_replication) AS oldest_replica_xact;
