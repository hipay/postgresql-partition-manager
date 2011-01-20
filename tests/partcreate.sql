begin ; 

select * from partition.table ; 

insert into partition.table (schemaname, tablename, keycolumn, pattern, cleanable, retention_period)  values
          ('test','test1an','ev_date','Y','t','3 year'),
          ('test','test1mois','ev_date','M','f', null),
          ('test','test1jour','ev_date','D','t','2 month'),
          ('test','test2an','ev_date','Y','t','1 year'),
          ('test','test2mois','ev_date','M','t','6 month'),
          ('test','test2jour','ev_date','D','f',null),
          ('test','test3an','ev_date','Y','f', null),
          ('test','test3mois','ev_date','M','t','1 year'),
          ('test','test3jour','ev_date','D','t','40 day') ;

select * from partition.create ('2010-08-01', '2011-06-30') ;


commit ; 
