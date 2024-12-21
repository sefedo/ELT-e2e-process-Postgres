# create and load DW

psql -d demo -c "create schema if not exists dw"

# these SQL statements contain DW logic, in real world should be one (or more) DML script per target table
# and DDL scripts maintained separately

psql -d demo -c "create table dw.category_dim as
 select *
  from raw_1.category;
alter table dw.category_dim add primary key (category_id);
alter table dw.category_dim add unique (name)"

psql -d demo -c "create table dw.actor_dim as
with _v as (
select *
 ,row_number() over (partition by first_name,last_name order by actor_id) rnum
 ,count(*) over (partition by first_name,last_name) rcnt
  from raw_1.actor
)
select actor_id, first_name, last_name
  , case when rcnt>1 then rnum end as qualifier
  , last_update
  from _v
;
alter table dw.actor_dim add primary key (actor_id);
alter table dw.actor_dim add unique (first_name,last_name,qualifier)"

psql -d demo -c "create table dw.film_dim as
select F.film_id, F.title, F.description, F.release_year
    , L.name as language
    , rental_duration, rental_rate, length, replacement_cost, rating
    , greatest(F.last_update, L.last_update) as last_update
  from raw_1.film F
  join raw_1.language L using (language_id)
;
alter table dw.film_dim add primary key (film_id);
alter table dw.film_dim add unique (title)"

psql -t -d demo -c "create table dw.store_dim as
with _a as (
select ci.city_id, ci.city, co.country_id, co.country, ad.address_id
    , ad.address, ad.address2, ad.district, ad.postal_code, ad.phone
    , greatest(ci.last_update, co.last_update, ad.last_update) as last_update
  from raw_1.city ci
  join raw_1.country co using (country_id)
  join raw_1.address ad using (city_id)
)
select st.store_id, st.manager_staff_id, st.address_id
  , ad.address, ad.address2, ad.district, ad.city, ad.postal_code, ad.phone
  , ad.country
  , greatest(st.last_update, ad.last_update) as last_update
  from raw_1.store st
  join _a ad using (address_id)
;
alter table dw.store_dim add primary key (store_id)"

psql -t -d demo -c "create table dw.film_fact as
select film_id, actor_id, category_id
  , greatest(fa.last_update,f.last_update,fc.last_update,c.last_update) as last_update
  from raw_1.film_actor fa
  join raw_1.film f using(film_id)
  join raw_1.film_category fc using(film_id)
  join raw_1.category c using(category_id)
;
alter table dw.film_fact add primary key (film_id,actor_id,category_id)"

psql -t -d demo -c "create table dw.inventory_fact as
 select *
  from raw_1.inventory
;
alter table dw.inventory_fact add primary key (inventory_id)"

psql -d demo -c "\dt dw.*"
