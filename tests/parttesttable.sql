begin ; 
drop schema if exists test cascade ;

drop role if exists test ; 
create role test login ; 

create schema test ; 
grant usage on schema test to test ; 

create table test.test1an ( id int, ev_date timestamptz default now() ) ; 
create table test.test1mois ( id int, ev_date timestamptz default now() ) ; 
create table test.test1jour ( id int, ev_date timestamptz default now() ) ; 

create sequence test.test1an_id_seq ;
create sequence test.test1mois_id_seq ;
create sequence test.test1jour_id_seq ;

alter table test.test1mois owner to test ; 

grant select, update on test.test1an_id_seq to test ; 
grant select, update on test.test1mois_id_seq to test ; 
grant select, update on test.test1jour_id_seq to test ; 

alter table test.test1an alter column id set default nextval('test.test1an_id_seq');
alter table test.test1mois alter column id set default nextval('test.test1mois_id_seq');
alter table test.test1jour alter column id set default nextval('test.test1jour_id_seq');

grant select on test.test1an to test ; 
grant select, update on test.test1mois to test ; 
grant select, delete, insert, update on test.test1jour to test ; 

create or replace function test.test_trigger () 
returns trigger 
language plpgsql
as $BODY$
begin 
  if TG_OP = 'INSERT' then
    raise notice 'Fct triggered on %', TG_OP ; 
    return new ; 
  elsif TG_OP = 'UPDATE' then
    raise notice 'Fct triggered on %', TG_OP ;
    return new ;
  elsif TG_OP = 'DELETE' then
    raise notice 'Fct triggered on %', TG_OP ;
    return old ;  
  end if;
  return null; 
end; 
$BODY$ ; 

create trigger _delev after delete 
  on test.test1mois
  for each row 
  execute procedure test.test_trigger () ; 

create trigger _insev before insert  
  on test.test1mois
  for each row
  execute procedure test.test_trigger () ; 

create trigger _insupdev before insert or update 
  on test.test1jour 
  for each row 
  execute procedure test.test_trigger () ; 

insert into partition.table (schemaname, tablename, keycolumn, pattern, cleanable, retention_period)  values
          ('test','test1an','ev_date','Y','t','3 year'),
          ('test','test1mois','ev_date','M','f', null),
          ('test','test1jour','ev_date','D','t','2 month') ;

select partition.create_part_trigger('test','test1an') ;
select partition.create_part_trigger('test','test1mois') ;
select partition.create_part_trigger('test','test1jour') ;

select * from partition.create ( current_date , (current_date + interval '3 day')::date ) ;

commit ; 

\c part test 

begin ;
insert into test.test1an ( ev_date ) values ( now() ) ;
insert into test.test1an ( ev_date ) values ( now() ) ;
update test.test1an set ev_date=now() where id=1 ; 
update test.test1an set ev_date=now() where id=2 ; 
commit ;

begin ;
insert into test.test1mois ( ev_date ) values ( now() ) ;
insert into test.test1mois ( ev_date ) values ( now() ) ;
update test.test1mois set ev_date=now() where id=1 ; 
update test.test1mois set ev_date=now() where id=2 ; 
commit ;

begin ;
insert into test.test1jour ( ev_date ) values ( now() ) ;
insert into test.test1jour ( ev_date ) values ( now() ) ;
update test.test1jour  set ev_date=now() where id=1 ; 
update test.test1jour  set ev_date=now() where id=2 ; 
commit ;

\c part postgres 
