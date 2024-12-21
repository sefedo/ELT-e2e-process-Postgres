#
# Delta process, extract old source, load full stg_1, mock cycle data
#

schr=raw_1
schs=stg_1

# extract source, create and load full stg_1

psql -d demo -c "create schema if not exists $schs"

# create source tables in stg_1
psql -t -d dvdrental -c "
select 'CREATE TABLE $schs.' || table_name ||'(' ||
    STRING_AGG ( column_name ||' '||
      case when data_type='USER-DEFINED' then 'text' else udt_name end, ','
      ORDER BY table_name, ordinal_position )
  ||');'
  from information_schema.columns
  join information_schema.tables using (table_schema,table_name)
 where table_schema = 'public' and table_type = 'BASE TABLE'
 group by table_name
" | psql -d demo

for tbl in `psql -t -d dvdrental -c "
  select table_name
    from information_schema.tables
   where table_schema='public'
     and table_type='BASE TABLE'
"`
do
  echo -: $tbl
  psql -t -d dvdrental -c "copy public.$tbl to stdout" |\
    psql -t -d demo -c "copy $schs.$tbl from stdin"
done


# cycle: mock data in raw_1, update source, then run delta sycle

# update source
psql -U postgres -d dvdrental -c "
insert into category values(10017,'Western');
insert into film values(11001,'Tres Amigos','Test 1 film',2022,2,5,1.99,55,22.2,'R');
insert into film_category values(11001,10017);
insert into film_category values(11001,9);
insert into actor values(10201,'Amigo','Uno');
insert into actor values(10202,'Amigo','Dos');
insert into film_actor values(10201,11001);
insert into film_actor values(10202,11001);
update inventory set film_id=11001 where inventory_id=1;
update actor set last_name='Davids' where actor_id=110"

# run cycle
cycle_from=2024-01-01
cycle_upto=2025-01-01

# load delta in raw
(
echo cycle_from=$cycle_from, cycle_upto=$cycle_upto
while read tbl ts
do
  echo -: $tbl $ts
  psql -t -d dvdrental -c "copy (select * from public.$tbl \
        where $ts>='$cycle_from' and $ts<'$cycle_upto') to stdout" |\
    psql -t -d demo -c "truncate $schr.$tbl; copy $schr.$tbl from stdin"
done
)<<EOF
actor last_update
film last_update
film_category last_update
inventory last_update
language last_update
rental last_update
staff last_update
payment payment_date
film_actor last_update
store last_update
address last_update
customer last_update
category last_update
city last_update
country last_update
EOF

# update delta in stg
(
echo cycle_from=$cycle_from, cycle_upto=$cycle_upto
while read tbl key
do
  echo -: $tbl $key
    psql -t -d demo -c "
      delete from $schs.$tbl where ($key) in (select $key from $schr.$tbl);
      insert into $schs.$tbl select * from $schr.$tbl"
done
)<<EOF
actor actor_id
film film_id
film_category film_id,category_id
inventory inventory_id
language language_id
rental rental_id
staff staff_id
payment payment_id
film_actor actor_id,film_id
store store_id
address address_id
customer customer_id
category category_id
city city_id
country country_id
EOF
