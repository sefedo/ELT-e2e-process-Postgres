cd /home/user0/DataProcess

dropdb -U postgres dvdrental
createdb -U postgres dvdrental
pg_restore -U postgres -d dvdrental dvdrental.tar
psql -U postgres -d dvdrental -c "create role user0 with login"

psql -U postgres -c "\l"
psql -U postgres -c "\du"
psql -d dvdrental -c '\dt public.*'

cd -
