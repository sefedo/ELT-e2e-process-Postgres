#
# Delta process, run a cycle. T part.
#

umask 00
schr=raw_1
schs=stg_1
dirt=/tmp/rundeck/DataProcess

cycle=`cat $dirt/cycle.running`

cycle_from=`echo $cycle | cut -f1 -d~`
cycle_upto=`echo $cycle | cut -f2 -d~`

echo $0 - cycle_from=$cycle_from, cycle_upto=$cycle_upto
echo
echo DW: insert into schema $schs from schema $schr

ts=last_update

echo -: category_dim
psql -d demo -c "truncate table $schs.category_dim;
insert into $schs.category_dim
select *
  from $schs.category t where t.$ts>='$cycle_from' and t.$ts<'$cycle_upto'
"

echo -: actor_dim
psql -d demo -c "truncate table $schs.actor_dim;
with _v as (
select *
 ,row_number() over (partition by first_name,last_name order by actor_id) rnum
 ,count(*) over (partition by first_name,last_name) rcnt
  from $schs.actor t where t.$ts>='$cycle_from' and t.$ts<'$cycle_upto'
)
insert into $schs.actor_dim
select actor_id, first_name, last_name
  , case when rcnt>1 then rnum end as qualifier
  , last_update
  from _v
"

echo -: film_dim
psql -d demo -c "truncate table $schs.film_dim;
insert into $schs.film_dim
select F.film_id, F.title, F.description, F.release_year
    , L.name as language
    , rental_duration, rental_rate, length, replacement_cost, rating
    , greatest(F.last_update, L.last_update) as last_update
  from $schs.film F
  join $schs.language L using (language_id)
 where (  F.$ts>='$cycle_from' and F.$ts<'$cycle_upto'
       or L.$ts>='$cycle_from' and L.$ts<'$cycle_upto' )
"

echo -: store_dim
psql -t -d demo -c "truncate table $schs.store_dim;
with _a as (
select ci.city_id, ci.city, co.country_id, co.country, ad.address_id
    , ad.address, ad.address2, ad.district, ad.postal_code, ad.phone
    , greatest(ci.last_update, co.last_update, ad.last_update) as last_update
  from $schs.city ci
  join $schs.country co using (country_id)
  join $schs.address ad using (city_id)
)
insert into $schs.store_dim
select st.store_id, st.manager_staff_id, st.address_id
  , ad.address, ad.address2, ad.district, ad.city, ad.postal_code, ad.phone
  , ad.country
  , greatest(st.last_update, ad.last_update) as last_update
  from $schs.store st
  join _a ad using (address_id)
 where ( st.$ts>='$cycle_from' and st.$ts<'$cycle_upto'
       or ad.$ts>='$cycle_from' and ad.$ts<'$cycle_upto' )
"

echo -: film_fact
psql -t -d demo -c "truncate table $schs.film_fact;
insert into $schs.film_fact
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
"

echo -: inventory_fact
psql -t -d demo -c "truncate table $schs.inventory_fact;
insert into $schs.inventory_fact
 select *
  from $schs.inventory t where t.$ts>='$cycle_from' and t.$ts<'$cycle_upto'
"


scho=dw
echo
echo update schema $scho from schema $schs

(
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
