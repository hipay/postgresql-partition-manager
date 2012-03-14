
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
  elsif i_period = 'week'::text then
    select (end_date::date - begin_date::date)::integer / 7 into v_ret ; 
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
	OUT indexes integer,
        OUT triggers integer,
        OUT grants integer
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
    v_triggerdef text ; 
    v_owner text ;
    v_current_role text ;
    t_grants int = 0 ; 
    v_constraint text ;  
  begin
    tables = 0 ;
    indexes = 0 ; 
    triggers = 0 ;
    grants = 0 ; 

 
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

          tables := tables + 1 ; 
  
          FOR col IN SELECT * FROM (VALUES ( i_column )) t(c)
          LOOP

            perform 1 from pg_catalog.pg_attribute a 
              where a.attrelid in ( SELECT i.indexrelid
                          FROM pg_catalog.pg_class c, 
                               pg_catalog.pg_class c2, 
                               pg_catalog.pg_namespace n,
                               pg_catalog.pg_index i
                          WHERE c.relname = spart
                            AND n.nspname = i_schema  
                            AND c.relnamespace = n.oid 
                            AND c.oid = i.indrelid 
                            AND i.indexrelid = c2.oid
                      ) 
              group by a.attrelid 
              having count(*) = 1
                 and ARRAY[ col ] @> array_agg( a.attname::text ) ; 
            if not found then
              EXECUTE 'CREATE INDEX idx_' || spart ||'_'||col|| ' ON ' || i_schema || '.' || spart || '('||col||')';
              indexes := indexes + 1;
            end if ; 
          END LOOP;

          -- create fk 

          FOR v_constraint IN select ' add '||pg_get_constraintdef( con.oid , true ) 
                                from pg_constraint con 
                                    join pg_class c 
                                      on con.conrelid=c.oid 
                                    join pg_namespace n 
                                      on c.relnamespace=n.oid 
                                    where con.contype='f' 
                                      and n.nspname = i_schema
                                      and c.relname = i_table
          loop 
            execute ' ALTER TABLE ' || i_schema || '.' || spart ||  v_constraint ; 
          end loop ; 

          -- create trigger           
          for v_triggerdef in select replace( triggerdef,  qname ,  i_schema || '.' || spart  ) 
             from partition.trigger 
             where schemaname= i_schema and tablename = i_table
          loop
            execute v_triggerdef ; 
            triggers := triggers + 1;
          end loop ; 

          select a.rolname, user into v_owner, v_current_role  
            from pg_class c 
              join pg_namespace n on c.relnamespace=n.oid 
              join pg_authid a on c.relowner=a.oid 
            where n.nspname= i_schema and c.relname= i_table ;

          if v_owner <> v_current_role then
            execute 'alter table '|| i_schema || '.' || spart ||' owner to ' || v_owner ; 
          end if ; 
 
          -- grant role 
          select partition.setgrant( i_schema, i_table, '_' || to_char ( pmonth , i_pattern ) ) into t_grants ; 
          grants = grants + t_grants ; 

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
	OUT o_indexes integer,
        OUT o_triggers integer,
        OUT o_grants integer
)
returns record 
LANGUAGE plpgsql
set client_min_messages = warning
as $BODY$
declare

  p_table record ;

  tables int = 0 ; 
  indexes int = 0 ;
  triggers int = 0 ; 
  grants int = 0 ;
 

begin

  o_tables = 0 ;
  o_indexes = 0 ; 
  o_triggers = 0 ;
  o_grants = 0 ; 

  for p_table in select t.schemaname, t.tablename, t.keycolumn, p.part_type, p.to_char_pattern 
                   from partition.table t , partition.pattern p 
                  where t.pattern=p.id and t.actif 
                  order by schemaname, tablename 
    loop 

    select * from partition.create( p_table.schemaname, p_table.tablename, p_table.keycolumn, p_table.part_type, p_table.to_char_pattern, begin_date, end_date ) 
      into tables, indexes, triggers, grants ; 

    o_tables = o_tables + tables ;
    o_indexes = o_indexes + indexes ; 
    o_triggers = o_triggers + triggers ;
    o_grants = o_grants + grants ; 

  end loop ; 

  return ;

end ;
$BODY$
;

create or replace function partition.create
(
	begin_date date,
	OUT tables integer,
	OUT indexes integer,
	OUT triggers integer,
	OUT grants integer
)
returns record 
as $BODY$
  select * from partition.create( $1 , $1 ) ;
$BODY$
LANGUAGE sql ;

create or replace function partition.create
(
	OUT tables integer,
	OUT indexes integer,
	OUT triggers integer,
	OUT grants integer
)
returns record 
as $BODY$
  select * from partition.create( current_date ) ;
$BODY$
 LANGUAGE sql ;

create or replace function partition.create_next
(
	OUT o_tables  integer,
	OUT o_indexes integer, 
	OUT o_triggers  integer,
	OUT o_grants integer
)
 returns record 
LANGUAGE plpgsql
set client_min_messages = warning
as $BODY$
declare

  p_table record ;

  tables int = 0 ; 
  indexes int = 0 ; 
  triggers int = 0 ; 
  grants int = 0 ; 

begin

  o_tables = 0 ;
  o_indexes = 0 ; 
  o_triggers = 0 ;
  o_grants = 0 ; 

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
      into tables, indexes, triggers, grants ; 

    o_tables = o_tables + tables ;
    o_indexes = o_indexes + indexes ; 
    o_triggers = o_triggers + triggers ;
    o_grants = o_grants + grants ; 

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
      -- raise notice 'select min( to_date(substr( tablename, length(tablename) - length( % ) +1  ,length(tablename)) , % ) ) into begin_date from pg_tables where schemaname=% and tablename ~ ( % ) ;',  i_pattern , i_pattern, i_schema, '^'||i_table||'_[0-9]{'||length( i_pattern )||'}'  ;

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

create or replace function partition.drop
(
	OUT o_tables  integer
)
 returns integer
LANGUAGE plpgsql
set client_min_messages = warning
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

    select * from partition.drop( p_table.schemaname, p_table.tablename, p_table.keycolumn, p_table.part_type, p_table.to_char_pattern, p_table.retention_date::date ) 
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


create or replace function partition.grant_replace( p_acl text, p_grant text, p_ext_grant text )
returns text 
language plpgsql 
as $BODY$
declare
  v_nb_grant int ; 
  v_pos int ; 
begin
  select length( p_acl ) into v_nb_grant ; 
  select position( p_grant in p_acl  ) into v_pos ;
  if v_pos <> 0 and v_pos <> v_nb_grant then
    return p_ext_grant || ', ' ;
  elsif v_pos <> 0 and v_pos = v_nb_grant then
    return p_ext_grant ;
  else
    return ''::text ;
  end if ;
end; 
$BODY$ ; 

create or replace function partition.grant( acl text, tablename text )
returns text 
language plpgsql 
as $BODY$
declare
  v_grantee text ;
  v_acl text ;
  v_grant text ;  
begin

  select  split_part( split_part( acl,'/', 1), '=', 1 ) into v_grantee ;
  select  split_part( split_part( acl,'/', 1), '=', 2 ) into v_acl ;

  v_grant = 'GRANT ' ; 

  v_grant =  v_grant || partition.grant_replace( v_acl, 'r','SELECT' ); 
  v_grant =  v_grant || partition.grant_replace( v_acl, 'w','UPDATE' ); 
  v_grant =  v_grant || partition.grant_replace( v_acl, 'a','INSERT' ); 
  v_grant =  v_grant || partition.grant_replace( v_acl, 'd','DELETE' );
  v_grant =  v_grant || partition.grant_replace( v_acl, 'D','TRUNCATE' );
  v_grant =  v_grant || partition.grant_replace( v_acl, 'x','REFERENCES' ); 
  v_grant =  v_grant || partition.grant_replace( v_acl, 't','TRIGGER' ); 
 
  v_grant =  v_grant || ' ON TABLE ' || tablename || ' TO ' || v_grantee ;  

  return v_grant ; 
end; 
$BODY$ ; 

create or replace function partition.setgrant( p_schemaname text, p_tablename text, p_part text )
returns int 
language plpgsql 
as $BODY$
declare
 v_acl text[] ; 
 v_grant text ;  
 i int = 0 ; 
 v_nb_grant int = 0 ; 
begin
  select c.relacl into v_acl 
    from pg_class c 
      join pg_namespace n 
        on c.relnamespace=n.oid 
     where c.relkind='r' 
       and n.nspname= p_schemaname 
       and c.relname= p_tablename  ; 
  if found then 
    if v_acl is not null 
    then
      for i in array_lower( v_acl, 1)..array_upper( v_acl, 1 ) 
      loop
        select partition.grant(  v_acl[i], p_schemaname||'.'||p_tablename||p_part ) into v_grant ; 
        execute v_grant ; 
        -- raise notice 'ACL : % % ', i, v_acl[i] ; 
        -- raise notice 'GRANT : % % ', i, v_grant ;
        v_nb_grant =  v_nb_grant + 1  ; 
      end loop ;
    end if ; 
  end if ;

  return v_nb_grant ; 
end; 
$BODY$ ; 

