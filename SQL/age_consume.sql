SELECT
    datname,
    age(datfrozenxid) AS frozen_xid_age,
    ROUND(100 * (age(datfrozenxid) / 2000000000::float)) consumed_txid_pct,
    2 * 1024 ^ 3 - 1 - age(datfrozenxid) AS remaining_txid,
    current_setting('autovacuum_freeze_max_age')::int - age(datfrozenxid) AS remaining_aggressive_vacuum
FROM
    pg_database;
