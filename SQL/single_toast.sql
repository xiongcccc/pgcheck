SELECT attr.attname,
               t.typname,
               CASE
                 WHEN attstorage = 'p' THEN 'plain'
                 WHEN attstorage = 'x' THEN 'extended'
                 WHEN attstorage = 'e' THEN 'external'
                 WHEN attstorage = 'm' THEN 'main'
               END AS attstorage
 FROM pg_attribute attr INNER JOIN
             pg_type t ON t.OID = attr.atttypid
 WHERE attrelid = 'test'::regclass
 ORDER BY attr.attnum;
