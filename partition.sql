
begin ; 

drop schema if exists partition cascade ; 

create schema partition ; 

create table partition.pattern (
   id              char(1) not null, 
   part_type       text not null,
   to_char_pattern text not null,

   primary key ( id )
) ; 

create table partition.table (
   schemaname       text not null, 
   tablename        text not null,
   keycolumn        text not null,
   pattern          char(1) not null references partition.pattern ( id ) , 
   actif            bool not null default 't'::bool, 
   cleanable        bool not null default 'f'::bool,
   retention_period interval,

   primary key ( schemaname, tablename )
);

insert into partition.pattern values 
    ('Y','year','YYYY'),
    ('M','month','YYYYMM'),
    ('D','day', 'YYYYMMDD') 
;

\i part_api.sql 
\i part_triggers.sql 


commit ; 