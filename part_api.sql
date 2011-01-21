
create or replace function partition.between(
  i_period text, 
  begin_date date, 
  end_date date
) 
returns integer
language plpgsql 
as $$
declare 
  v_ret integer ;  
  v_age interval ;
begin
  if i_period = 'year'::text then
    select extract(year from age($3,$2))::integer +1  into v_ret ; 
  elsif i_period = 'month'::text then
    select age($3,$2) into v_age ; 
    select 12 * extract(year from v_age)::integer + extract(month from v_age)::integer into v_ret ; 
  elsif i_period = 'day'::text then
    select end_date::date - begin_date::date into v_ret ; 
  else
    raise exception 'period % not yet implemented', i_period ; 
  end if ; 
  return v_ret ;
end ;
$$ ;


create or replace function partition.create
(
	i_schema text,
	i_table text,
	i_column text,
        i_period text, 
        i_pattern text,
	begin_date date,
	end_date date,
	OUT tables integer,
	OUT indexes integer
)
returns record 
LANGUAGE plpgsql
set client_min_messages = warning
as $BODY$

  declare 
    loval  date;
    hival  date;
    counter int := 0 ; 
    pmonth date ;
    spart text ; 
    col text ; 
    qname text = i_schema || '.' || i_table ; 
  begin

    tables = 0 ;
    indexes = 0 ; 
 
    FOR pmonth IN SELECT (begin_date + x * ('1 '||i_period)::interval )::date
                    FROM generate_series(0, partition.between(i_period, begin_date, end_date ) ) x
    LOOP
        loval := date_trunc( i_period , pmonth)::date;
        hival := (loval + ('1 '||i_period)::interval )::date;

        spart = i_table || '_' || to_char ( pmonth , i_pattern ); 

        begin 

          execute ' create table ' || i_schema || '.' || spart || ' ( '
            || ' like ' || qname  
            || ' INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES, ' 
            || ' check ( '|| i_column  ||' >= ' || quote_literal( loval ) || ' and '|| i_column  ||' < ' || quote_literal( hival ) || ' )) '  
            || ' inherits ( ' || qname || ') ;' ;  

          begin
            execute 'alter table ' || a_tables[counter] || ' add primary key (id ) ; ' ; 
          exception when others then
            raise notice ' Create PK : % ', SQLERRM ; 
          end ; 

          tables := tables + 1 ; 
  
          FOR col IN SELECT * FROM (VALUES ( i_column  )) t(c)
          LOOP
            EXECUTE 'CREATE INDEX idx_' || spart ||'_'||col|| ' ON ' || i_schema || '.' || spart || '('||col||')';
            indexes := indexes + 1;
          END LOOP;
  
        exception when duplicate_table then
          raise notice 'Create Part : % ', SQLERRM ; 
        end ; 

        counter = counter + 1 ;

    END LOOP;

    return ; 

  end ;

$BODY$

;

create or replace function partition.create
(
	begin_date    date,
	end_date       date,
	OUT o_tables  integer,
	OUT o_indexes integer
)
 returns record 
LANGUAGE plpgsql
set client_min_messages = warning
as $BODY$
declare

  p_table record ;

  tables int = 0 ; 
  indexes int = 0 ; 

begin

  o_tables = 0 ;
  o_indexes = 0 ; 

  for p_table in select t.schemaname, t.tablename, t.keycolumn, p.part_type, p.to_char_pattern 
                   from partition.table t , partition.pattern p 
                  where t.pattern=p.id and t.actif 
                  order by schemaname, tablename 
    loop 

    select * from partition.create( p_table.schemaname, p_table.tablename, p_table.keycolumn, p_table.part_type, p_table.to_char_pattern, begin_date, end_date ) 
      into tables, indexes ; 

    o_tables = o_tables + tables ;
    o_indexes = o_indexes + indexes ; 

  end loop ; 

  return ;

end ;
$BODY$
;

create or replace function partition.create
(
	begin_date date,
	OUT tables integer,
	OUT indexes integer
)
returns record 
as $BODY$
  select * from partition.create( $1 , $1 ) ;
$BODY$
LANGUAGE sql ;

create or replace function partition.create
(
	OUT tables integer,
	OUT indexes integer
)
returns record 
as $BODY$
  select * from partition.create( current_date ) ;
$BODY$
 LANGUAGE sql ;

create or replace function partition.create_next
(
	OUT o_tables  integer,
	OUT o_indexes integer
)
 returns record 
LANGUAGE plpgsql
set client_min_messages = warning
as $BODY$
declare

  p_table record ;

  tables int = 0 ; 
  indexes int = 0 ; 

begin

  o_tables = 0 ;
  o_indexes = 0 ; 

  for p_table in select t.schemaname, t.tablename, t.keycolumn, p.part_type, p.to_char_pattern, (now() + p.next_part)::date as pdate 
                   from partition.table t 
                     inner join partition.pattern p
		           on ( t.pattern=p.id )                   
	             full join pg_tables pgt
			   on ( pgt.schemaname=t.schemaname 
                                and pgt.tablename=t.tablename ||'_'|| to_char ( now() + p.next_part , p.to_char_pattern  ) )
		    where t.actif and pgt.schemaname is null and  pgt.tablename is null 
		    order by  t.schemaname ||'.'|| t.tablename 
		      ||'_'|| to_char ( now() + p.next_part , p.to_char_pattern  ) 
    loop 

    select * from partition.create( p_table.schemaname, p_table.tablename, p_table.keycolumn, p_table.part_type, p_table.to_char_pattern, p_table.pdate, p_table.pdate) 
      into tables, indexes ; 

    o_tables = o_tables + tables ;
    o_indexes = o_indexes + indexes ; 

  end loop ; 

  return ;

end ;
$BODY$
;


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
-- set client_min_messages = warning
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
 
    raise notice 'i_schema %, i_table %, i_column %, i_period %, i_pattern %, retention_date %',i_schema, i_table, i_column, i_period, i_pattern, i_retention_date  ; 
 
    perform schemaname, tablename from partition.table where schemaname=i_schema and tablename=i_table and cleanable ; 
    if found then

      -- look up for older partition to drop 
      select min( to_date(substr(tablename, length(tablename) - length( i_pattern ) +1 , length(tablename)), i_pattern ) ) into begin_date 
          from pg_tables where schemaname=i_schema and tablename ~ ('^'||i_table||'_') ;

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

create or replace function partition.drop
(
	OUT o_tables  integer
)
 returns integer
LANGUAGE plpgsql
-- set client_min_messages = warning
as $BODY$
declare
  p_table record ;
  tables int = 0 ;
begin

  o_tables = 0 ; 

  for p_table in select t.schemaname, t.tablename, t.keycolumn, p.part_type, p.to_char_pattern, 
                        current_date - t.retention_period as retention_date  
                   from partition.table t , partition.pattern p 
                  where t.pattern=p.id and t.actif and t.cleanable 
                  order by schemaname, tablename 
    loop 

    select * from partition.drop( p_table.schemaname, p_table.tablename, p_table.keycolumn, p_table.part_type, p_table.to_char_pattern, p_table.retention_date::date  ) 
      into tables ; 

    o_tables = o_tables + tables ;

  end loop ; 

  return ;

end ;
$BODY$
;

create or replace function partition.check_next_part
(
  OUT nagios_return_code int, 
  OUT message text 
)
returns record
language sql
as $BODY$

select case when count(missing_tables) > 0 then 2::int else 0::int end, 'Missing : ' || string_agg( missing_tables,', ')
-- string_agg( missing_tables, ', ') 
from ( select
  t.schemaname ||'.'|| t.tablename 
    ||'_'|| to_char ( now() + p.next_part , p.to_char_pattern  ) 
    as missing_tables  
  from partition.table t 
    inner join partition.pattern p
      on ( t.pattern=p.id )                   
    full join pg_tables pgt
      on ( pgt.schemaname=t.schemaname 
           and pgt.tablename=t.tablename ||'_'|| to_char ( now() + p.next_part , p.to_char_pattern  ) )
  where t.actif and pgt.schemaname is null and  pgt.tablename is null 
  order by  t.schemaname ||'.'|| t.tablename 
    ||'_'|| to_char ( now() + p.next_part , p.to_char_pattern  ) 
) x 
; 

$BODY$ ; 
