SELECT *,
    current_setting('archive_mode')::BOOLEAN
        AND (last_failed_wal IS NULL
            OR last_failed_wal <= last_archived_wal)
        AS is_archiving,
    CAST (archived_count AS NUMERIC)
        / EXTRACT (EPOCH FROM age(now(), stats_reset))
        AS current_archived_wals_per_second
FROM pg_stat_archiver;
