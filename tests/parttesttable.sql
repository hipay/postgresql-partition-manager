begin ; 
drop schema if exists test cascade ;

create schema test ; 

create table test.test1an ( id serial, ev_date timestamptz default now() ) ; 
create table test.test1mois ( id serial, ev_date timestamptz default now() ) ; 
create table test.test1jour ( id serial, ev_date timestamptz default now() ) ; 

create trigger partitionne before insert on test.test1an for each row execute procedure partition.partitionne_dyn() ; 
create trigger partitionne before insert on test.test1mois for each row execute procedure partition.partitionne_dyn() ; 
create trigger partitionne before insert on test.test1jour for each row execute procedure partition.partitionne_dyn() ; 

create table test.test2an ( id serial, ev_date timestamptz default now() ) ; 
create table test.test2mois ( id serial, ev_date timestamptz default now() ) ; 
create table test.test2jour ( id serial, ev_date timestamptz default now() ) ; 

create trigger partitionne before insert on test.test2an for each row execute procedure partition.partitionne('ev_date','YYYY') ; 
create trigger partitionne before insert on test.test2mois for each row execute procedure partition.partitionne('ev_date','YYYYMM') ; 
create trigger partitionne before insert on test.test2jour for each row execute procedure partition.partitionne('ev_date','YYYYMMDD' ) ; 

create table test.test3an ( id serial, ev_date timestamptz default now() ) ; 
create table test.test3mois ( id serial, ev_date timestamptz default now() ) ; 
create table test.test3jour ( id serial, ev_date timestamptz default now() ) ; 

create trigger partitionne before insert on test.test3an for each row execute procedure partition.partitionne_yearly() ; 
create trigger partitionne before insert on test.test3mois for each row execute procedure partition.partitionne_monthly() ; 
create trigger partitionne before insert on test.test3jour for each row execute procedure partition.partitionne_daily() ; 

create table test.test4an ( id serial, ev_date timestamptz default now() ) ; 
create table test.test4mois ( id serial, ev_date timestamptz default now() ) ; 
create table test.test4jour ( id serial, ev_date timestamptz default now() ) ; 


commit ; 

