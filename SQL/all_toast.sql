SELECT
  n.nspname as schema,
      s.oid::regclass as relname,
      s.reltoastrelid::regclass as toast_name,
      pg_relation_size(s.reltoastrelid) AS toast_size
  FROM
      pg_class s join pg_namespace n
      on s.relnamespace = n.oid
  WHERE
      relkind = 'r'
      AND reltoastrelid <> 0
      AND n.nspname = 'public'
  ORDER BY
      3 DESC;
