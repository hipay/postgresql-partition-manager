begin ;

UPDATE partition.pattern
   set to_char_pattern = 'IYYYIW' 
   where id = 'W' ;

create or replace function partition.drop
(
	i_schema text,
	i_table text,
	i_column text,
        i_period text, 
        i_pattern text,
	i_retention_date date,
	OUT tables integer
)
returns integer
set client_min_messages = warning
LANGUAGE plpgsql
as $BODY$
  declare 
    loval  timestamp;
    hival  timestamp;
    counter int := 0 ; 
    pmonth date ;
    spart text ; 
    col text ; 
    qname text = i_schema || '.' || i_table ; 
    begin_date date ;
  begin

    tables = 0 ;
 
    -- raise notice 'i_schema %, i_table %, i_column %, i_period %, i_pattern %, retention_date %',i_schema, i_table, i_column, i_period, i_pattern, i_retention_date  ; 
 
    perform schemaname, tablename from partition.table where schemaname=i_schema and tablename=i_table and cleanable ; 
    if found then

      -- look up for older partition to drop 
      select min( to_date(substr(tablename, length(tablename) - length( i_pattern ) +1 , length(tablename)), i_pattern ) ) into begin_date 
          from pg_tables where schemaname=i_schema and tablename ~ ('^'||i_table||'_[0-9]{'||length( i_pattern )||'}') ;

      FOR pmonth IN SELECT (begin_date + x * ('1 '||i_period)::interval )::date
                      FROM generate_series(0, partition.between(i_period, begin_date, i_retention_date ) ) x
      LOOP
          loval := date_trunc( i_period , pmonth)::date;
          hival := (loval + ('1 '||i_period)::interval  )::date;

          spart = i_table || '_' || to_char ( pmonth , i_pattern ); 

          begin 
            execute ' drop table ' || i_schema || '.' || spart || ' cascade ;' ;  

            tables := tables + 1 ; 
  
          exception when others then
            raise notice 'Drop Part : % ', SQLERRM ; 
          end ; 

          counter = counter + 1 ;

      END LOOP;

    end if ;

    return ; 

  end ;

$BODY$
;

commit ; 


