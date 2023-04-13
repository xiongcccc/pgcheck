WITH btree_index_atts AS (
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
  ORDER BY format_bloat.bloat_mb DESC;
