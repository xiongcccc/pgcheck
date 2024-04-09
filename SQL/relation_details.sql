WITH cteTableInfo AS 
(
    SELECT 
        COUNT(1) AS ct
        ,SUM(length(t::text)) AS TextLength  
        ,'public.t1'::regclass AS TableName  
    FROM public.t1 AS t  
)
,cteRowSize AS 
(
   SELECT ARRAY [pg_relation_size(TableName)
               , pg_relation_size(TableName, 'vm')
               , pg_relation_size(TableName, 'fsm')
               , pg_table_size(TableName)
               , pg_indexes_size(TableName)
               , pg_total_relation_size(TableName)
               , TextLength
             ] AS val
        , ARRAY ['Relation Size'
               , 'Visibility Map'
               , 'Free Space Map'
               , 'Table Included Toast Size'
               , 'Indexes Size'
               , 'Total Relation Size'
               , 'Live Row Byte Size'
             ] AS Name
   FROM cteTableInfo
)
SELECT 
    unnest(name) AS Description
    ,unnest(val) AS Bytes
    ,pg_size_pretty(unnest(val)) AS BytesPretty
    ,unnest(val) / ct AS bytes_per_row
FROM cteTableInfo, cteRowSize
UNION ALL SELECT '------------------------------', NULL, NULL, NULL
UNION ALL SELECT 'TotalRows', ct, NULL, NULL FROM cteTableInfo
UNION ALL SELECT 'LiveTuples', pg_stat_get_live_tuples(TableName), NULL, NULL FROM cteTableInfo
UNION ALL SELECT 'DeadTuples', pg_stat_get_dead_tuples(TableName), NULL, NULL FROM cteTableInfo;
