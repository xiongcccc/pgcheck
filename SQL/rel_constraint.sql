select pgc.conname as constraint_name,
       ccu.table_schema as table_schema,
       ccu.table_name,
       ccu.column_name,
       pg_get_constraintdef(pgc.oid) as cons_def
from pg_constraint pgc
join pg_namespace nsp on nsp.oid = pgc.connamespace
join pg_class  cls on pgc.conrelid = cls.oid
left join information_schema.constraint_column_usage ccu
          on pgc.conname = ccu.constraint_name
          and nsp.nspname = ccu.constraint_schema
where
        nsp.nspname not in ('pg_catalog','information_schema','pg_toast')
        and ccu.table_name = 'test'
order by pgc.conname;
