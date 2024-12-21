# full load of source tables in raw_1
for tbl in `psql -t -d dvdrental -c "
  select table_name
    from information_schema.tables
   where table_schema='public'
     and table_type='BASE TABLE'
"`
do
  echo -: $tbl
  psql -t -d dvdrental -c "copy public.$tbl to stdout" |\
    psql -t -d demo -c "copy raw_1.$tbl from stdin"
done
