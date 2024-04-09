SELECT r.rolname AS user_name,
       c.oid::regclass AS table_name,
       p.perm AS privilege_type
  FROM pg_class c CROSS JOIN
       pg_roles r CROSS JOIN
       unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p(perm)
 WHERE relkind = 'r' AND
       relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname in ('pg_catalog','information_schema')) AND
       has_table_privilege(rolname, c.oid, p.perm);
