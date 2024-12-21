#
# Delta process, run a cycle. E and L part.
#

umask 00
schr=raw_1
schs=stg_1
dirt=/tmp/rundeck/DataProcess
mkdir -p $dirt
chmod -R 777 $dirt

# auto-set cycle interval if not provided

if [ $# -eq 0 ] ; then
  intvl="10 minutes"
  cycle=`psql -t -d demo -c "with _vt as (
    select (substr(to_char( now()::timestamp,'yyyy-mm-dd hh24:mi'),1,15)||'0:00')::timestamp as ts
    ) select ts - interval '$intvl' ||'~'|| ts from _vt
  "`
  cycle_from=`echo $cycle | cut -f1 -d~`
  cycle_upto=`echo $cycle | cut -f2 -d~`
else
  cycle_from=$1
  cycle_upto=$2
fi
echo $0 - $cycle_from~$cycle_upto >$dirt/cycle.running

echo cycle_from=$cycle_from, cycle_upto=$cycle_upto
echo
echo extract from source db into schema $schr

# run extract, load raw

rm $dirt/cycle.E.results 2>/dev/null
touch $dirt/cycle.E.results
(
while read tbl ts key
do
  echo -: $tbl $ts
  psql -t -d dvdrental -c "copy (select * from public.$tbl \
        where $ts>='$cycle_from' and $ts<'$cycle_upto') to stdout" |\
    psql -t -d demo -c "truncate $schr.$tbl; copy $schr.$tbl from stdin" | grep -v 'TRUNCATE' >$dirt/cycle.E.$tbl
    grep -v 'COPY 0' <$dirt/cycle.E.$tbl >/dev/null && echo $tbl $key >>$dirt/cycle.E.results
done
)<<EOF
actor last_update actor_id
film last_update film_id
film_category last_update film_id,category_id
inventory last_update inventory_id
language last_update language_id
rental last_update rental_id
staff last_update staff_id
payment payment_date payment_id
film_actor last_update actor_id,film_id
store last_update store_id
address last_update address_id
customer last_update customer_id
category last_update category_id
city last_update city_id
country last_update country_id
EOF

echo
echo list of updates in schema $schr
cat $dirt/cycle.E.results

# update stg
echo
echo source copy: update schema $schs from schema $schr

(
while read tbl key
do
  echo -: $tbl $key
    psql -t -d demo -c "
      delete from $schs.$tbl where ($key) in (select $key from $schr.$tbl);
      insert into $schs.$tbl select * from $schr.$tbl"
done
) <$dirt/cycle.E.results
