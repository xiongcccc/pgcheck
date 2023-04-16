WITH max_age AS (
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
