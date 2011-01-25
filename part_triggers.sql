
create or replace function partition.create_part_trigger
  (
    p_schemaname text,
    p_tablename  text
  )
returns void
language plpgsql 
as $BODY$
declare
  part text ;
  v_insert text ; 
  v_column text ;
  v_pattern text ; 
begin 
 
  select t.keycolumn, p.to_char_pattern into v_column, v_pattern 
    from partition.table t join partition.pattern p on t.pattern=p.id 
    where t.schemaname = p_schemaname and t.tablename = p_tablename ;  
  if FOUND then

    execute 'create or replace function partition.partitionne_'||p_schemaname||'_'||p_tablename||'()
returns trigger
language plpgsql 
as $PART$
 declare
   part text ;
   v_column text ;
   v_pattern text ; 
 begin 
   IF TG_OP = ''INSERT''
   THEN

     SELECT to_char( NEW.'|| v_column ||', ' || quote_literal( v_pattern  ) ||  ') INTO part ;
 
     execute ''INSERT INTO '' || TG_TABLE_SCHEMA || ''.'' || TG_TABLE_NAME || ''_'' || part 
       || '' SELECT ('' || quote_literal(textin(record_out(NEW)))
       || ''::'' || TG_TABLE_SCHEMA || ''.'' || TG_TABLE_NAME || '').*;'' ;
 
     RETURN null ;
   else
     return new ; 
   END IF;
 end ; 
$PART$ ; ' ; 

  execute 'create trigger _partitionne before insert on '
      ||p_schemaname||'.'||p_tablename||' for each row '
      ||'execute procedure partition.partitionne_' 
      ||p_schemaname||'_'||p_tablename||'() ; ' ; 

  end if ; 


end ;
$BODY$
; 


create or replace function partition.set_trigger_def()
returns trigger
language plpgsql
as $BODY$
declare
  r_trigg record ; 
begin
  if TG_OP = 'INSERT' then 
    for r_trigg in select n.nspname, c.relname, 
                          t.tgname, pg_get_triggerdef( t.oid ) as triggerdef
      from pg_class c 
        join pg_namespace n on c.relnamespace=n.oid
        join pg_trigger t on c.oid=t.tgrelid 
        where c.relkind='r' 
        and ( t.tgconstraint is null or t.tgconstraint = 0 )
        and n.nspname = new.schemaname and c.relname = new.tablename 
    loop

      insert into partition.trigger values (  r_trigg.nspname, r_trigg.relname, r_trigg.tgname, r_trigg.triggerdef ) ; 
 
      execute 'drop trigger ' || r_trigg.tgname || ' on ' || r_trigg.nspname  || '.' ||  r_trigg.relname  ; 

    end loop ; 

  end if ; 
  return new ; 
end ;
$BODY$ ; 

create trigger _settrigg after insert on partition.table 
for each row 
execute procedure partition.set_trigger_def() ; 
