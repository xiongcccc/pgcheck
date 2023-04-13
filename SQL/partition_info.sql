WITH RECURSIVE inheritance_tree AS (
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
 ORDER BY 1,2;
