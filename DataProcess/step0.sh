psql -U user0 -d demo -c "
  drop schema if exists stg_1 cascade;
  drop schema if exists raw_1 cascade;
  drop schema if exists dw cascade"
psql -U postgres -d dvdrental -c "
  revoke select on all tables in schema public from user0"

exit
