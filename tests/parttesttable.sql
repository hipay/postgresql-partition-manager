begin ; 
drop schema if exists test cascade ;

create schema test ; 

create table test.test1an ( id serial, ev_date timestamptz default now() ) ; 
create table test.test1mois ( id serial, ev_date timestamptz default now() ) ; 
create table test.test1jour ( id serial, ev_date timestamptz default now() ) ; 

insert into partition.table (schemaname, tablename, keycolumn, pattern, cleanable, retention_period)  values
          ('test','test1an','ev_date','Y','t','3 year'),
          ('test','test1mois','ev_date','M','f', null),
          ('test','test1jour','ev_date','D','t','2 month') ;

select partition.create_part_trigger('test','test1an') ;
select partition.create_part_trigger('test','test1mois') ;
select partition.create_part_trigger('test','test1jour') ;

select * from partition.create ('2011-01-01') ;

commit ; 

