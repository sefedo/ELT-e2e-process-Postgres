#
# Delta process, build delta DW in stg_1 and merge into dw
#

schr=raw_1
schs=stg_1

# show status in raw
psql -t -d demo -c "
select 'select '''|| rpad(table_name,15) ||''', count(*) from $schr.'|| table_name ||';'
  from information_schema.tables
 where table_schema = '$schr' and table_type = 'BASE TABLE'
" | psql -t -d demo | grep -v '^$' | sort


cycle_from=2024-01-01
cycle_upto=2025-01-01
ts=last_update

# fast create-load DW tables in stg

psql -d demo -c "drop table if exists $schs.category_dim"
psql -d demo -c "create table $schs.category_dim as
 select *
  from $schs.category t where t.$ts>='$cycle_from' and t.$ts<'$cycle_upto';
alter table $schs.category_dim add primary key (category_id);
alter table $schs.category_dim add unique (name)"

psql -d demo -c "drop table if exists $schs.actor_dim"
psql -d demo -c "create table $schs.actor_dim as
with _v as (
select *
 ,row_number() over (partition by first_name,last_name order by actor_id) rnum
 ,count(*) over (partition by first_name,last_name) rcnt
  from $schs.actor t where t.$ts>='$cycle_from' and t.$ts<'$cycle_upto'
)
select actor_id, first_name, last_name
  , case when rcnt>1 then rnum end as qualifier
  , last_update
  from _v
;
alter table $schs.actor_dim add primary key (actor_id);
alter table $schs.actor_dim add unique (first_name,last_name,qualifier)"

psql -d demo -c "drop table if exists $schs.film_dim"
psql -d demo -c "create table $schs.film_dim as
select F.film_id, F.title, F.description, F.release_year
    , L.name as language
    , rental_duration, rental_rate, length, replacement_cost, rating
    , greatest(F.last_update, L.last_update) as last_update
  from $schs.film F
  join $schs.language L using (language_id)
 where (  F.$ts>='$cycle_from' and F.$ts<'$cycle_upto'
       or L.$ts>='$cycle_from' and L.$ts<'$cycle_upto' )
;
alter table $schs.film_dim add primary key (film_id);
alter table $schs.film_dim add unique (title)"

psql -d demo -c "drop table if exists $schs.store_dim"
psql -t -d demo -c "create table $schs.store_dim as
with _a as (
select ci.city_id, ci.city, co.country_id, co.country, ad.address_id
    , ad.address, ad.address2, ad.district, ad.postal_code, ad.phone
    , greatest(ci.last_update, co.last_update, ad.last_update) as last_update
  from $schs.city ci
  join $schs.country co using (country_id)
  join $schs.address ad using (city_id)
 where (  ci.$ts>='$cycle_from' and ci.$ts<'$cycle_upto'
       or co.$ts>='$cycle_from' and co.$ts<'$cycle_upto'
       or ad.$ts>='$cycle_from' and ad.$ts<'$cycle_upto' )
)
select st.store_id, st.manager_staff_id, st.address_id
  , ad.address, ad.address2, ad.district, ad.city, ad.postal_code, ad.phone
  , ad.country
  , greatest(st.last_update, ad.last_update) as last_update
  from $schs.store st
  join _a ad using (address_id)
 where st.$ts>='$cycle_from' and st.$ts<'$cycle_upto'
;
alter table $schs.store_dim add primary key (store_id)"

psql -d demo -c "drop table if exists $schs.film_fact"
psql -t -d demo -c "create table $schs.film_fact as
select film_id, actor_id, category_id
  , greatest(fa.last_update,f.last_update,fc.last_update,c.last_update) as last_update
  from $schs.film_actor fa
  join $schs.film f using(film_id)
  join $schs.film_category fc using(film_id)
  join $schs.category c using(category_id)
  where (  fa.$ts>='$cycle_from' and fa.$ts<'$cycle_upto'
        or f.$ts>='$cycle_from' and f.$ts<'$cycle_upto'
        or fc.$ts>='$cycle_from' and fc.$ts<'$cycle_upto'
        or c.$ts>='$cycle_from' and c.$ts<'$cycle_upto' )
;
alter table $schs.film_fact add primary key (film_id,actor_id,category_id)"

psql -d demo -c "drop table if exists $schs.inventory_fact"
psql -t -d demo -c "create table $schs.inventory_fact as
 select *
  from $schs.inventory t where t.$ts>='$cycle_from' and t.$ts<'$cycle_upto'
;
alter table $schs.inventory_fact add primary key (inventory_id)"

echo

#psql -d demo -c "\dt stg_1.*"

# apply delta from stg
scho=dw
(
echo cycle_from=$cycle_from, cycle_upto=$cycle_upto
while read tbl key
do
  echo -: $tbl $key
    psql -t -d demo -c "
      delete from $scho.$tbl where ($key) in (select $key from $schs.$tbl);
      insert into $scho.$tbl select * from $schs.$tbl"
done
)<<EOF
actor_dim actor_id
category_dim category_id
film_dim film_id
film_fact film_id,actor_id,category_id
inventory_fact inventory_id
store_dim store_id
EOF
