createdb -U postgres demo
psql -d demo -c "create schema if not exists raw_1"
psql -U postgres -d dvdrental -c "grant select on all tables in schema public to user0"

# create source tables in raw_1
psql -t -d dvdrental -c "
select 'CREATE TABLE  raw_1.' || table_name ||'(' ||
    STRING_AGG ( column_name ||' '||
      case when data_type='USER-DEFINED' then 'text' else udt_name end, ','
      ORDER BY table_name, ordinal_position )
  ||');'
  from information_schema.columns
  join information_schema.tables using (table_schema,table_name)
 where table_schema = 'public' and table_type = 'BASE TABLE'
 group by table_name
" | psql -d demo
